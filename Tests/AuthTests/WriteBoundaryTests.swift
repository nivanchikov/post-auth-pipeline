import Testing
import OperationPipeline
@testable import Auth

private enum LM {}
/// A post-login op that blocks until released, then completes — simulating an
/// op still in-flight when logout fires.
private struct SlowPostOp<M>: Operation {
    typealias Value = String
    let gate: Gate
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String {
        await gate.wait()
        return "late"
    }
}
private actor Gate {
    private var continuations: [CheckedContinuation<Void, Never>] = []
    private var open = false
    func wait() async {
        if open { return }
        await withCheckedContinuation { continuations.append($0) }
    }
    func release() { open = true; continuations.forEach { $0.resume() }; continuations = [] }
}

@Test func lateCompletingOpDoesNotLeakIntoNewSession() async throws {
    let gate = Gate()
    let pipeline = try buildPipeline([AnyOperation(SlowPostOp<LM>(gate: gate))])
    let broker = SessionScopeBroker()
    let engine = OperationEngine()

    // Session 1 begins; start the slow post-login run on scope 1.
    let scope1 = await broker.beginSession()
    let run1 = Task { _ = await engine.run(pipeline, scope: scope1) }

    // Logout: drop the session, then a NEW session begins (re-login).
    await broker.endSession(scope1)
    let scope2 = await broker.beginSession()
    #expect(scope2 !== scope1)

    // Now let the stale op finish — it writes into scope1 (discarded), never scope2.
    await gate.release()
    _ = await run1.value

    let live2 = await scope2.liveSnapshot()
    #expect(live2[ObjectIdentifier(SlowPostOp<LM>.self)] == nil)   // no leak into new session
}
