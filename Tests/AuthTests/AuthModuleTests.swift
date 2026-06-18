import Testing
import OperationPipeline
@testable import Auth

/// Stub gating runner returning a fixed result.
private struct StubGating: GatingRunner {
    let result: GatingResult
    func run() async -> GatingResult { result }
}

/// Stub backend that succeeds or throws.
private struct StubBackend: AuthBackend {
    let fails: Bool
    func authenticate(_ credentials: Credentials) async throws {
        if fails { throw BackendError() }
    }
}
private struct BackendError: Error, Sendable {}

private actor Gate {
    private var continuations: [CheckedContinuation<Void, Never>] = []
    private var open = false
    func wait() async {
        if open { return }
        await withCheckedContinuation { continuations.append($0) }
    }
    func release() { open = true; continuations.forEach { $0.resume() }; continuations = [] }
}

private struct GatingGate: GatingRunner {
    let gate: Gate
    let result: GatingResult
    func run() async -> GatingResult {
        await gate.wait()
        return result
    }
}

@Test func successfulLoginReachesAuthenticated() async throws {
    let module = AuthModule(backend: StubBackend(fails: false), gating: StubGating(result: .success))

    await module.login(Credentials(username: "u", password: "p"))
    #expect(await module.state == .authenticated)

    // Drain the buffered stream deterministically. AsyncStream's default buffer
    // is `.unbounded`, so every yielded state is retained; iterate until we see
    // `.authenticated`, then break (the stream never finishes on its own).
    var seen: [AuthState] = []
    for await s in module.stateStream {
        seen.append(s)
        if s == .authenticated { break }
    }
    #expect(seen.contains(.authenticating))
    #expect(seen.contains(.authenticated))
}

@Test func gatingAbortKeepsUnauthenticatedWithWrappedError() async throws {
    let module = AuthModule(
        backend: StubBackend(fails: false),
        gating: StubGating(result: .aborted(BackendError()))
    )
    await module.login(Credentials(username: "u", password: "p"))
    if case .unauthenticated(let err) = await module.state {
        #expect(err != nil)
    } else {
        Issue.record("expected unauthenticated with error")
    }
}

@Test func backendFailureNeverRunsGating() async throws {
    let module = AuthModule(backend: StubBackend(fails: true), gating: StubGating(result: .success))
    await module.login(Credentials(username: "u", password: "p"))
    if case .unauthenticated(let err) = await module.state { #expect(err != nil) }
    else { Issue.record("expected unauthenticated") }
}

@Test func logoutReturnsToCleanUnauthenticated() async throws {
    let module = AuthModule(backend: StubBackend(fails: false), gating: StubGating(result: .success))
    await module.login(Credentials(username: "u", password: "p"))
    await module.logout()
    #expect(await module.state == .unauthenticated(nil))
}

@Test func logoutWhileGatingPreventsStaleAuthentication() async throws {
    let gate = Gate()
    let module = AuthModule(
        backend: StubBackend(fails: false),
        gating: GatingGate(gate: gate, result: .success)
    )

    let loginTask = Task { await module.login(Credentials(username: "u", password: "p")) }

    // Wait until login has actually started on the actor before logging out.
    var started = false
    for _ in 0..<10_000 {
        if case .authenticating = await module.state { started = true; break }
        await Task.yield()
    }
    #expect(started)

    await module.logout()
    await gate.release()
    _ = await loginTask.value

    #expect(await module.state == .unauthenticated(nil))
}
