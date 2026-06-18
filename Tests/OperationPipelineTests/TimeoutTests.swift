import Testing
@testable import OperationPipeline

private struct SlowOp<M>: Operation {
    typealias Value = String
    let policy: FailurePolicy
    var failurePolicy: FailurePolicy { policy }
    var timeout: Duration? { .milliseconds(50) }
    func run(_ ctx: RunContext) async throws -> String {
        try? await Task.sleep(for: .seconds(10))   // far longer than the timeout
        return "done"
    }
}
private enum SM1 {}; private enum SM2 {}

@Test func failOpenTimeoutIsPrunedAndRunCompletes() async throws {
    let op = SlowOp<SM1>(policy: .open)
    let pipeline = try buildPipeline([AnyOperation(op)])
    let scope = SessionScope()

    let result = await OperationEngine().run(pipeline, scope: scope)

    #expect(result.isCompleted)                    // fail-open timeout does not abort
    let live = await scope.liveSnapshot()
    #expect(live[ObjectIdentifier(SlowOp<SM1>.self)]?.kind == .failed)
}

@Test func failCloseTimeoutAbortsRun() async throws {
    let op = SlowOp<SM2>(policy: .close)
    let pipeline = try buildPipeline([AnyOperation(op)])

    let result = await OperationEngine().run(pipeline, scope: SessionScope())

    if case .aborted = result { #expect(Bool(true)) } else { Issue.record("expected abort") }
}
