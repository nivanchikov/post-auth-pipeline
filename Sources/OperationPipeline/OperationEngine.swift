import Foundation

/// Outcome of running one pipeline.
public enum RunResult: Sendable {
    case completed
    case aborted(PipelineFailure)

    public var isCompleted: Bool {
        if case .completed = self {
            return true
        }
        return false
    }
}

/// Stateless DAG scheduler. All mutable run state lives in the per-run `RunState`.
public struct OperationEngine: Sendable {
    public init() {}

    public func run(_ pipeline: Pipeline) async -> RunResult {
        await run(pipeline, state: RunState())
    }

    func run(_ pipeline: Pipeline, state: RunState) async -> RunResult {
        await withTaskGroup(of: NodeResult.self) { group in
            for node in pipeline.operations {
                group.addTask {
                    await Self.runNode(node, state: state)
                }
            }

            var abortFailure: PipelineFailure?
            for await result in group {
                if result.outcome.kind == .failed,
                   result.failurePolicy == .close,
                   abortFailure == nil {
                    // Fallback guarantees abortFailure is non-nil so the .close failure is never misreported as
                    // .completed.
                    abortFailure = PipelineFailure(
                        operation: result.typeName,
                        underlying: result.outcome.failureError ?? UnattributedFailure()
                    )
                    await state.abort()
                    group.cancelAll()
                }
            }

            if let failure = abortFailure {
                return .aborted(failure)
            }
            return .completed
        }
    }

    /// Run one node: gate on dependencies, evaluate the guard, then run.
    private static func runNode(_ node: AnyOperation, state: RunState) async -> NodeResult {
        // The awaited box is a SIGNAL ONLY (it tells us the upstream completed);
        // we discard it and re-read everything from `scope.snapshot()` below.
        // This is why the abort sentinel's concrete type is irrelevant and never
        // risks a type-mismatch trap — nobody `unwrap`s the value returned here.
        for dep in node.dependsOn {
            _ = await state.awaitOutcome(of: dep)
        }

        // NOTE: hoist the await — `await` is not allowed inside the `||`
        // right-hand autoclosure under Swift 6 strict concurrency.
        let aborted = await state.isAborted
        if Task.isCancelled || aborted {
            let box = node.makeSkipped(reason: "cancelled")
            await state.complete(node.id, box)
            return NodeResult(id: node.id, typeName: node.typeName, failurePolicy: node.failurePolicy, outcome: box)
        }

        // A node already past its gate runs until its operation returns or its
        // cooperative timeout wins the race.
        let ctx = await PipelineContext(outcomes: state.snapshot())
        let outcome: AnyOutcomeBox = if await node.shouldRun(ctx) {
            await runWithOptionalTimeout(node, ctx)
        } else {
            node.makeSkipped(reason: "guard returned false")
        }
        await state.complete(node.id, outcome)
        return NodeResult(id: node.id, typeName: node.typeName, failurePolicy: node.failurePolicy, outcome: outcome)
    }

    /// Race the op against its deadline with structured child tasks. This keeps
    /// timeout handling simple, with the usual Swift caveat: timeout cancellation
    /// is cooperative, so an operation that ignores cancellation can still delay
    /// the task-group scope from returning.
    private static func runWithOptionalTimeout(
        _ node: AnyOperation, _ ctx: RunContext
    ) async -> AnyOutcomeBox {
        guard let timeout = node.timeout else {
            return await node.run(ctx)
        }

        return await withTaskGroup(of: AnyOutcomeBox.self, returning: AnyOutcomeBox.self) { group in
            group.addTask { await node.run(ctx) }
            group.addTask {
                if await (try? Task.sleep(for: timeout)) != nil {
                    return node.makeFailed(TimeoutError(operation: node.typeName))
                }
                return node.makeSkipped(reason: "cancelled")
            }

            let first = await group.next() ?? node.makeSkipped(reason: "cancelled")
            group.cancelAll()
            return first
        }
    }
}

private struct UnattributedFailure: Error, Sendable {}

/// Internal per-node result surfaced to the engine's collection loop.
struct NodeResult: Sendable {
    let id: ObjectIdentifier
    let typeName: String
    let failurePolicy: FailurePolicy
    let outcome: AnyOutcomeBox
}
