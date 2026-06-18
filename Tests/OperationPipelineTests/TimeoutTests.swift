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
private enum SM1 {}; private enum SM2 {}; private enum SM3 {}

private struct QuickOp<M>: Operation {
    typealias Value = String
    var failurePolicy: FailurePolicy { .open }
    var timeout: Duration? { .seconds(5) }   // ample headroom; op returns immediately
    func run(_ ctx: RunContext) async throws -> String { "quick" }
}

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

@Test func opWinsBeforeTimeoutKeepsSuccessValue() async throws {
    // Op has a timeout but returns promptly, so the op should win the race and
    // its real success value must be preserved (NOT replaced by a TimeoutError).
    let op = QuickOp<SM3>()
    let pipeline = try buildPipeline([AnyOperation(op)])
    let scope = SessionScope()

    let result = await OperationEngine().run(pipeline, scope: scope)

    #expect(result.isCompleted)
    let live = await scope.liveSnapshot()
    let box = live[ObjectIdentifier(QuickOp<SM3>.self)]
    #expect(box?.kind == .success)
    if case .success(let v)? = box?.unwrap(as: String.self) {
        #expect(v == "quick")
    } else {
        Issue.record("expected success with the op's real value")
    }
}
