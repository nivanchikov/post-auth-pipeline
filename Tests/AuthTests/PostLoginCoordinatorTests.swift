import Testing
import OperationPipeline
@testable import Auth

private enum PM {}
private struct PostOp<M>: Operation {
    typealias Value = String
    let ran: RanFlag
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String { await ran.set(); return "post" }
}

@Test func runsPostLoginPipelineOnAuthenticated() async throws {
    let ran = RanFlag()
    let pipeline = try buildPipeline([AnyOperation(PostOp<PM>(ran: ran))])
    let broker = SessionScopeBroker()
    _ = await broker.beginSession()   // simulate gating already created the session

    let (stream, cont) = AsyncStream.makeStream(of: AuthState.self)
    let coordinator = PostLoginCoordinator(engine: OperationEngine(), pipeline: pipeline, broker: broker)
    let observation = coordinator.observe(stream)

    cont.yield(.authenticated)
    let didRun = await poll { await ran.value }   // deterministic, no sleep
    observation.cancel()

    #expect(didRun)
}

@Test func endsSessionOnUnauthenticated() async throws {
    let ran = RanFlag()
    let pipeline = try buildPipeline([AnyOperation(PostOp<PM>(ran: ran))])
    let broker = SessionScopeBroker()
    _ = await broker.beginSession()

    let (stream, cont) = AsyncStream.makeStream(of: AuthState.self)
    let coordinator = PostLoginCoordinator(engine: OperationEngine(), pipeline: pipeline, broker: broker)
    let observation = coordinator.observe(stream)

    cont.yield(.unauthenticated(nil))
    let cleared = await poll { await broker.currentSession() == nil }
    observation.cancel()

    #expect(cleared)   // session cleared on logout
}
