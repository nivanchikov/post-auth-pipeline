import Testing
import OperationPipeline
@testable import Auth

private enum GM1 {}; private enum GM2 {}; private enum GM3 {}
private struct GateOp<M>: Operation {
    typealias Value = String
    let policy: FailurePolicy
    let shouldThrow: Bool
    var failurePolicy: FailurePolicy { policy }
    func run(_ ctx: RunContext) async throws -> String {
        if shouldThrow { throw GateError() }
        return "ok"
    }
}
private struct GateError: Error, Sendable {}

private actor Gate {
    private var continuations: [CheckedContinuation<Void, Never>] = []
    private var open = false
    func wait() async {
        if open { return }
        await withCheckedContinuation { continuations.append($0) }
    }
    func release() { open = true; continuations.forEach { $0.resume() }; continuations = [] }
}

private struct SlowGatingOp<M>: Operation {
    typealias Value = String
    let gate: Gate
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String {
        await gate.wait()
        return "ok"
    }
}

@Test func gatingRunnerReturnsSuccessWhenPipelineCompletes() async throws {
    let op = GateOp<GM1>(policy: .open, shouldThrow: false)
    let pipeline = try buildPipeline([AnyOperation(op)])
    let broker = SessionScopeBroker()
    let runner = EngineGatingRunner(engine: OperationEngine(), pipeline: pipeline, broker: broker)

    let result = await runner.run()

    if case .success = result { #expect(Bool(true)) } else { Issue.record("expected success") }
    #expect(await broker.currentSession() != nil)   // session kept on success
}

@Test func gatingRunnerReturnsAbortedAndEndsSessionOnFailClose() async throws {
    let op = GateOp<GM2>(policy: .close, shouldThrow: true)
    let pipeline = try buildPipeline([AnyOperation(op)])
    let broker = SessionScopeBroker()
    let runner = EngineGatingRunner(engine: OperationEngine(), pipeline: pipeline, broker: broker)

    let result = await runner.run()

    if case .aborted = result { #expect(Bool(true)) } else { Issue.record("expected aborted") }
    #expect(await broker.currentSession() == nil)    // session dropped on abort
}

@Test func gatingRunnerAbortsWhenTaskIsCancelled() async throws {
    let gate = Gate()
    let pipeline = try buildPipeline([AnyOperation(SlowGatingOp<GM3>(gate: gate))])
    let broker = SessionScopeBroker()
    let runner = EngineGatingRunner(engine: OperationEngine(), pipeline: pipeline, broker: broker)

    let task = Task { await runner.run() }
    task.cancel()
    await gate.release()
    let result = await task.value

    if case .aborted = result { #expect(Bool(true)) } else { Issue.record("expected aborted on cancellation") }
    #expect(await broker.currentSession() == nil)
}
