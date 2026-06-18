import Testing
import OperationPipeline
@testable import Auth

@Test func authStateEqualityIgnoresUnderlyingErrorIdentity() {
    let e1 = AuthError(failingOperation: "Ent", underlying: PlainAuthError())
    let e2 = AuthError(failingOperation: "Ent", underlying: PlainAuthError())
    #expect(AuthState.unauthenticated(e1) == AuthState.unauthenticated(e2))
    #expect(AuthState.authenticated == AuthState.authenticated)
    #expect(AuthState.unauthenticated(nil) != AuthState.authenticated)
}

@Test func brokerBeginCreatesFreshScopeAndEndDrops() async {
    let broker = SessionScopeBroker()
    let s1 = await broker.beginSession()
    #expect(await broker.currentSession() === s1)
    let s2 = await broker.beginSession()           // re-login mints a new scope
    #expect(s2 !== s1)
    await broker.endSession()
    #expect(await broker.currentSession() == nil)
}

struct PlainAuthError: Error, Sendable {}
