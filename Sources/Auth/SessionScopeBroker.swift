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

    /// Drops the session **only** if `scope` is still the current one. This
    /// prevents a stale, overlapping gating run from erasing a newer session
    /// when it finishes later (forced re-auth / double login race).
    public func endSession(_ scope: SessionScope) {
        if current === scope { current = nil }
    }
}
