import OperationPipeline

/// Shares ONE `SessionScope` between the gating runner and the post-login
/// coordinator for a single auth session. `beginSession` mints a fresh scope
/// (dropping any prior one — re-login/forced-reauth are treated like logout);
/// `endSession` drops it (the write-boundary clear).
public actor SessionScopeBroker {
    private var current: SessionScope?
    public init() {}

    public func beginSession() -> SessionScope {
        let scope = SessionScope()
        current = scope
        return scope
    }

    public func currentSession() -> SessionScope? { current }

    public func endSession() { current = nil }
}
