@testable import Auth
import OperationPipeline
import Testing

/// Worked example mirroring the spec: EntitlementsOp (gating, fail-close) →
/// PaywallOp (gating, fail-open, outcome-based guard). PushOp (post-login).
private struct Entitlements: Sendable { let showPaywall: Bool }

private struct EntitlementsOp: Operation {
    typealias Value = Entitlements
    var failurePolicy: FailurePolicy {
        .close
    }

    func run(_: RunContext) async throws -> Entitlements {
        Entitlements(showPaywall: true)
    }
}

private struct PaywallOp: Operation {
    typealias Value = String
    var dependsOn: [any Operation.Type] {
        [EntitlementsOp.self]
    }

    var failurePolicy: FailurePolicy {
        .open
    }

    func shouldRun(_ ctx: RunContext) async -> Bool {
        guard let ent = ctx.value(of: EntitlementsOp.self) else {
            return false
        }
        return ent.showPaywall
    }

    func run(_: RunContext) async throws -> String {
        "paywall-configured"
    }
}

private struct PushOp: Operation {
    typealias Value = String
    let ran: RanFlag
    var failurePolicy: FailurePolicy {
        .open
    }

    func run(_: RunContext) async throws -> String {
        await ran.set()
        return "registered"
    }
}

@Test func fullLoginRunsGatingThenPostLogin() async throws {
    let engine = OperationEngine()

    let gatingPipeline = try buildPipeline([AnyOperation(EntitlementsOp()), AnyOperation(PaywallOp())])
    let pushRan = RanFlag()
    let postPipeline = try buildPipeline([AnyOperation(PushOp(ran: pushRan))])

    let gatingRunner = EngineGatingRunner(engine: engine, pipeline: gatingPipeline)
    let module = AuthModule(backend: AlwaysOKBackend(), gating: gatingRunner)
    let coordinator = PostLoginCoordinator(engine: engine, pipeline: postPipeline)
    let observation = coordinator.observe(module.stateStream)

    await module.login(Credentials(username: "u", password: "p"))
    #expect(await module.state == .authenticated)

    let pushPublished = await poll { await pushRan.value }
    observation.cancel()
    #expect(pushPublished)
}

private struct AlwaysOKBackend: AuthBackend {
    func authenticate(_: Credentials) async throws {}
}
