/// Type-erased `Outcome`. Stores a concrete `Outcome<V>` so heterogeneous
/// outcomes share one map. The single `as?` cast the API hides lives here.
struct AnyOutcomeBox: Sendable {
    enum Kind: Sendable, Equatable { case success, skipped, failed }

    let kind: Kind
    /// The failure error, present only when `kind == .failed`.
    let failureError: (any Error)?
    private let storage: any Sendable // holds the original Outcome<V>

    init(_ outcome: Outcome<some Sendable>) {
        switch outcome {
        case .success: kind = .success
            failureError = nil
        case .skipped: kind = .skipped
            failureError = nil
        case let .failed(error): kind = .failed
            failureError = error
        }
        storage = outcome
    }

    /// Recover the concrete outcome. Returns `nil` only when the key was absent
    /// (callers use optional chaining on the map). If the box is present but
    /// holds a different value type, that is a wiring bug and we trap.
    func unwrap<Value: Sendable>(as _: Value.Type) -> Outcome<Value>? {
        guard let typed = storage as? Outcome<Value> else {
            preconditionFailure(
                "AnyOutcomeBox type mismatch: stored \(Swift.type(of: storage)), asked for Outcome<\(Value.self)>"
            )
        }
        return typed
    }
}
