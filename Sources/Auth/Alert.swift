/// A transient alert an operation asks the host to present before login
/// completes. Carried in an operation's outcome, never persisted.
///
/// `Choice` is a caller-owned type (typically an enum) naming the possible
/// answers. Keeping it a type parameter means the operation switches on the
/// result exhaustively — a missed case is a compile error, not a silent
/// stringly-typed fallthrough.
public struct Alert<Choice: Sendable & Hashable>: Sendable, Identifiable, Equatable {
    public let id: String
    public let title: String
    public let message: String
    /// Ordered buttons. One per possible `Choice`.
    public let actions: [AlertAction<Choice>]

    public init(id: String, title: String, message: String, actions: [AlertAction<Choice>]) {
        self.id = id
        self.title = title
        self.message = message
        self.actions = actions
    }
}

/// One button in an alert. Its `choice` is the value delivered back to the
/// operation when tapped.
public struct AlertAction<Choice: Sendable & Hashable>: Sendable, Identifiable, Equatable {
    public enum Role: Sendable, Equatable {
        case normal
        case cancel
        case destructive
    }

    public let choice: Choice
    public let title: String
    public let role: Role
    public var id: Choice { choice }

    public init(_ choice: Choice, title: String, role: Role = .normal) {
        self.choice = choice
        self.title = title
        self.role = role
    }
}

/// The host-owned collaborator an operation awaits to present an alert. The
/// engine and auth module know nothing about it; it is injected into the
/// operation like any other domain dependency. The `Choice` primary associated
/// type lets an operation depend on `any AlertPresenter<MyChoice>`.
public protocol AlertPresenter<Choice>: Sendable {
    associatedtype Choice: Sendable & Hashable
    /// Present `alert` and suspend until the user answers. The returned `Choice`
    /// is the tapped action's `choice`.
    func present(_ alert: Alert<Choice>) async -> Choice
}
