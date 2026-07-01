@testable import Auth

// swiftlint:disable type_name
import OperationPipeline
import Testing

private enum PM {}
private struct PostOp<M>: Operation {
    typealias Value = String
    let ran: RanFlag
    var failurePolicy: FailurePolicy {
        .open
    }

    func run(_: RunContext) async throws -> String {
        await ran.set()
        return "post"
    }
}

private struct CancellablePostOp<M>: Operation {
    typealias Value = String
    let started: RanFlag
    let cancelled: RanFlag
    let gate: Gate
    var failurePolicy: FailurePolicy {
        .open
    }

    func run(_: RunContext) async throws -> String {
        await started.set()
        await gate.wait()
        if Task.isCancelled {
            await cancelled.set()
        }
        return "post"
    }
}

private actor Gate {
    private var continuations: [CheckedContinuation<Void, Never>] = []
    private var open = false

    func wait() async {
        if open {
            return
        }
        await withCheckedContinuation { continuations.append($0) }
    }

    func release() {
        open = true
        continuations.forEach { $0.resume() }
        continuations = []
    }
}

@Test func runsPostLoginPipelineOnAuthenticated() async throws {
    let ran = RanFlag()
    let pipeline = try buildPipeline([AnyOperation(PostOp<PM>(ran: ran))])

    let (stream, cont) = AsyncStream.makeStream(of: AuthState.self)
    let coordinator = PostLoginCoordinator(engine: OperationEngine(), pipeline: pipeline)
    let observation = coordinator.observe(stream)

    cont.yield(.authenticated)
    let didRun = await poll { await ran.value } // deterministic, no sleep
    observation.cancel()

    #expect(didRun)
}

@Test func cancelsPostLoginOnUnauthenticated() async throws {
    let started = RanFlag()
    let cancelled = RanFlag()
    let gate = Gate()
    let pipeline = try buildPipeline([
        AnyOperation(CancellablePostOp<PM>(started: started, cancelled: cancelled, gate: gate))
    ])

    let (stream, cont) = AsyncStream.makeStream(of: AuthState.self)
    let coordinator = PostLoginCoordinator(engine: OperationEngine(), pipeline: pipeline)
    let observation = coordinator.observe(stream)

    // The coordinator only acts on .unauthenticated after an active session,
    // so simulate the full login → logout transition.
    cont.yield(.authenticated)
    let didStart = await poll { await started.value }
    cont.yield(.unauthenticated(nil))
    await gate.release()
    let didCancel = await poll { await cancelled.value }
    observation.cancel()

    #expect(didStart)
    #expect(didCancel)
}

// swiftlint:enable type_name
