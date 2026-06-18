import Testing
import OperationPipeline
@testable import Auth

// Worked example mirroring the spec: EntitlementsOp (gating, fail-close) →
// PaywallOp (gating, fail-open, outcome-based guard). PushOp (post-login).
private struct Entitlements: Sendable { let showPaywall: Bool }

private struct EntitlementsOp: Operation {
    typealias Value = Entitlements
    var failurePolicy: FailurePolicy { .close }
    func run(_ ctx: RunContext) async throws -> Entitlements { Entitlements(showPaywall: true) }
}
private struct PaywallOp: Operation {
    typealias Value = String
    var dependsOn: [any Operation.Type] { [EntitlementsOp.self] }
    var failurePolicy: FailurePolicy { .open }
    func shouldRun(_ ctx: RunContext) async -> Bool {
        guard let ent = ctx.value(of: EntitlementsOp.self) else { return false }
        return ent.showPaywall
    }
    func run(_ ctx: RunContext) async throws -> String { "paywall-configured" }
}
private struct PushOp: Operation {
    typealias Value = String
    let ran: RanFlag
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String {
        await ran.set()
        // Prove the post-login op ran on the SHARED session by reading the
        // gating-produced entitlements value.
        let saw = ctx.value(of: EntitlementsOp.self)
        return "registered:\(saw?.showPaywall ?? false)"
    }
}

@Test func fullLoginRunsGatingThenPostLogin() async throws {
    let broker = SessionScopeBroker()
    let engine = OperationEngine()

    let gatingPipeline = try buildPipeline([AnyOperation(EntitlementsOp()), AnyOperation(PaywallOp())])
    let pushRan = RanFlag()
    let postPipeline = try buildPipeline([AnyOperation(PushOp(ran: pushRan))])

    let gatingRunner = EngineGatingRunner(engine: engine, pipeline: gatingPipeline, broker: broker)
    let module = AuthModule(backend: AlwaysOKBackend(), gating: gatingRunner)
    let coordinator = PostLoginCoordinator(engine: engine, pipeline: postPipeline, broker: broker)
    let observation = coordinator.observe(module.stateStream)

    await module.login(Credentials(username: "u", password: "p"))
    #expect(await module.state == .authenticated)

    // Wait for the post-login run to publish; `pushRan` flips before publish,
    // so poll the live map directly for the final outcome.
    let pushPublished = await poll {
        let scope = await broker.currentSession()
        let live = await scope?.liveSnapshot() ?? [:]
        return live[ObjectIdentifier(PushOp.self)] != nil
    }
    observation.cancel()
    #expect(pushPublished)

    // The shared session context carries the gating entitlements value and the
    // post-login op saw it.
    let scope = await broker.currentSession()
    let live = await scope?.liveSnapshot() ?? [:]
    #expect(live[ObjectIdentifier(EntitlementsOp.self)]?.kind == .success)
    if case .success(let v)? = live[ObjectIdentifier(PushOp.self)]?.unwrap(as: String.self) {
        #expect(v == "registered:true")
    } else {
        Issue.record("expected PushOp success with shared-context value")
    }
}

private struct AlwaysOKBackend: AuthBackend {
    func authenticate(_ credentials: Credentials) async throws {}
}
