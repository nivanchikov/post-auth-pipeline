import OperationPipeline

/// Error the auth module surfaces when gating aborts login. Wraps the engine's
/// fail-close error and identifies the blocking operation, if known.
public struct AuthError: Error, Sendable {
    public let failingOperation: String?
    public let underlying: any Error & Sendable
    public init(failingOperation: String?, underlying: any Error & Sendable) {
        self.failingOperation = failingOperation
        self.underlying = underlying
    }
}

/// Auth state published by the auth module. The host drives login UI off this.
public enum AuthState: Sendable {
    case unauthenticated(AuthError?)   // nil = clean / initial
    case authenticating
    case authenticated
}

extension AuthState: Equatable {
    public static func == (lhs: AuthState, rhs: AuthState) -> Bool {
        switch (lhs, rhs) {
        case (.authenticating, .authenticating),
             (.authenticated, .authenticated):
            return true
        case let (.unauthenticated(a), .unauthenticated(b)):
            // Compare by presence + failing-operation label, not error identity.
            return a?.failingOperation == b?.failingOperation && (a == nil) == (b == nil)
        default:
            return false
        }
    }
}
