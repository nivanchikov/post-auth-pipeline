/// Read-only view of completed upstream outcomes, passed into each operation.
/// Immutable and `Sendable`: a snapshot taken when a node starts running.
public protocol RunContext: Sendable {
    /// The dependency's success value, or `nil` if absent / not a success.
    func value<O: Operation>(of type: O.Type) -> O.Value?
    /// The dependency's full outcome, or `nil` if absent.
    func outcome<O: Operation>(of type: O.Type) -> Outcome<O.Value>?
    /// Type-erased success check (used by the default cascade guard).
    func succeeded(_ type: any Operation.Type) -> Bool
}

/// Concrete `RunContext` backed by an immutable outcome map.
struct PipelineContext: RunContext {
    private let outcomes: [ObjectIdentifier: AnyOutcomeBox]

    init(outcomes: [ObjectIdentifier: AnyOutcomeBox]) {
        self.outcomes = outcomes
    }

    func value<O: Operation>(of type: O.Type) -> O.Value? {
        guard case let .success(value)? = outcomes[ObjectIdentifier(type)]?.unwrap(as: O.Value.self)
        else {
            return nil
        }
        return value
    }

    func outcome<O: Operation>(of type: O.Type) -> Outcome<O.Value>? {
        outcomes[ObjectIdentifier(type)]?.unwrap(as: O.Value.self)
    }

    func succeeded(_ type: any Operation.Type) -> Bool {
        outcomes[ObjectIdentifier(type)]?.kind == .success
    }
}
