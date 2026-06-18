import Foundation

/// Outcome of running one pipeline.
public enum RunResult: Sendable {
    case completed
    case aborted(any Error & Sendable)

    public var isCompleted: Bool { if case .completed = self { return true }; return false }
}

/// Stateless-across-sessions DAG scheduler. All mutable run state lives in the
/// passed `SessionScope`.
public actor OperationEngine {
    public init() {}

    public func run(_ pipeline: Pipeline, scope: SessionScope) async -> RunResult {
        await scope.beginRun()

        return await withTaskGroup(of: NodeResult.self) { group in
            for node in pipeline.operations {
                group.addTask {
                    await Self.runNode(node, scope: scope)
                }
            }

            var abortError: (any Error & Sendable)? = nil
            for await r in group {
                if r.outcome.kind == .failed,
                   r.failurePolicy == .close,
                   abortError == nil {
                    // Fallback guarantees abortError is non-nil so the .close failure is never misreported as .completed.
                    abortError = r.outcome.failureError ?? SendableErrorBox(UnattributedFailure())
                    await scope.abort()
                    group.cancelAll()
                }
            }

            if let e = abortError { return .aborted(e) }
            await scope.publish()
            return .completed
        }
    }

    /// Run one node: gate on dependencies, evaluate the guard, then run.
    private static func runNode(_ node: AnyOperation, scope: SessionScope) async -> NodeResult {
        // The awaited box is a SIGNAL ONLY (it tells us the upstream completed);
        // we discard it and re-read everything from `scope.snapshot()` below.
        // This is why the abort sentinel's concrete type is irrelevant and never
        // risks a type-mismatch trap — nobody `unwrap`s the value returned here.
        for dep in node.dependsOn { _ = await scope.awaitOutcome(of: dep) }

        // NOTE: hoist the await — `await` is not allowed inside the `||`
        // right-hand autoclosure under Swift 6 strict concurrency.
        let aborted = await scope.isAborted
        if Task.isCancelled || aborted {
            let box = node.makeSkipped(reason: "cancelled")
            await scope.complete(node.id, box)
            return NodeResult(id: node.id, failurePolicy: node.failurePolicy, outcome: box)
        }

        // A node already past its gate runs to completion even if an abort fires
        // afterward: abort cancels gated/future work, not in-flight work. The
        // per-op timeout (Task 10) is the backstop for a hung in-flight op.
        let ctx = PipelineContext(outcomes: await scope.snapshot())
        let outcome: AnyOutcomeBox
        if await node.shouldRun(ctx) {
            outcome = await runWithOptionalTimeout(node, ctx)
        } else {
            outcome = node.makeSkipped(reason: "guard returned false")
        }
        await scope.complete(node.id, outcome)
        return NodeResult(id: node.id, failurePolicy: node.failurePolicy, outcome: outcome)
    }

    /// Race the op against its deadline. On expiry, produce a typed `.failed`
    /// outcome (subject to the op's failurePolicy upstream).
    ///
    /// IMPORTANT — why this is NOT a `withTaskGroup`: a task group always awaits
    /// ALL its children at scope exit. If the op ignores cancellation, exiting the
    /// group would block until the op returns — defeating the timeout entirely and
    /// violating the spec's login-liveness guarantee. Instead the op runs in an
    /// UNSTRUCTURED `Task` whose completion we never await: we read the first racer
    /// to publish on an `AsyncStream` and return immediately. On timeout we request
    /// cancellation of the op and ABANDON it (Swift has no forcible termination);
    /// when it eventually finishes it writes into its own session scope, which is
    /// safe by the write-boundary invariant.
    private static func runWithOptionalTimeout(
        _ node: AnyOperation, _ ctx: RunContext
    ) async -> AnyOutcomeBox {
        guard let timeout = node.timeout else { return await node.run(ctx) }

        let (stream, continuation) = AsyncStream<AnyOutcomeBox>.makeStream()

        // The op runs unstructured; we will NOT await its completion.
        let work = Task {
            let box = await node.run(ctx)
            continuation.yield(box)
            continuation.finish()
        }
        // The timer races it.
        let timer = Task {
            if (try? await Task.sleep(for: timeout)) != nil {
                continuation.yield(node.makeFailed(TimeoutError(operation: node.typeName)))
            }
            continuation.finish()
        }

        var iterator = stream.makeAsyncIterator()
        // First racer to yield wins; if cancelled mid-wait, next() returns nil.
        let first = await iterator.next() ?? node.makeFailed(TimeoutError(operation: node.typeName))
        work.cancel()    // request cancellation of the op (cooperative; abandoned otherwise)
        timer.cancel()
        return first
    }
}

private struct UnattributedFailure: Error, Sendable {}

/// Internal per-node result surfaced to the engine's collection loop.
struct NodeResult: Sendable {
    let id: ObjectIdentifier
    let failurePolicy: FailurePolicy
    let outcome: AnyOutcomeBox
}
