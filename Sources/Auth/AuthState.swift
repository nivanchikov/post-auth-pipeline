import OperationPipeline

/// Error the auth module surfaces when gating aborts login. Wraps the engine's
/// fail-close error and identifies the blocking operation, if known.
public struct AuthError: Error, Sendable {
    public let failingOperation: String?
    public let message: String

    public init(failingOperation: String?, message: String) {
        self.failingOperation = failingOperation
        self.message = message
    }

    public init(failure: PipelineFailure) {
        self.init(failingOperation: failure.operation, message: failure.message)
    }
}

/// Auth state published by the auth module. The host drives login UI off this.
public enum AuthState: Sendable {
    case unauthenticated(AuthError?) // nil = clean / initial
    case authenticating
    case authenticated
}

extension AuthState: Equatable {
    public static func == (lhs: AuthState, rhs: AuthState) -> Bool {
        switch (lhs, rhs) {
        case (.authenticating, .authenticating),
             (.authenticated, .authenticated):
            true
        case let (.unauthenticated(lhsError), .unauthenticated(rhsError)):
            // Compare by presence + failing-operation label, not error identity.
            lhsError?.failingOperation == rhsError?.failingOperation
                && (lhsError == nil) == (rhsError == nil)
        default:
            false
        }
    }
}
