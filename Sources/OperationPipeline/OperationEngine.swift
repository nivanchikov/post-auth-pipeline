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
                    abortError = r.outcome.failureError
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

        let ctx = PipelineContext(outcomes: await scope.snapshot())
        let outcome: AnyOutcomeBox
        if await node.shouldRun(ctx) {
            outcome = await node.run(ctx)
        } else {
            outcome = node.makeSkipped(reason: "guard returned false")
        }
        await scope.complete(node.id, outcome)
        return NodeResult(id: node.id, failurePolicy: node.failurePolicy, outcome: outcome)
    }
}

/// Internal per-node result surfaced to the engine's collection loop.
struct NodeResult: Sendable {
    let id: ObjectIdentifier
    let failurePolicy: FailurePolicy
    let outcome: AnyOutcomeBox
}
