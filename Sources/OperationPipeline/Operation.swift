import Foundation

/// A unit of post-auth work. Its own type is its identity and its value key.
public protocol Operation: Sendable {
    associatedtype Value: Sendable

    /// Prerequisite operation types. e.g. `[EntitlementsOp.self]`.
    var dependsOn: [any Operation.Type] { get }
    /// Whether this op's failure aborts the pipeline.
    var failurePolicy: FailurePolicy { get }
    /// Optional per-op deadline. `nil` = no timeout.
    var timeout: Duration? { get }
    /// True if this op suspends awaiting a signal the engine cannot itself
    /// produce (e.g. a human tap) via a continuation that does NOT observe
    /// cancellation. Such an op cannot free itself if abort-cancelled, so
    /// `buildPipeline` guarantees it is never abort-cancelled: every fail-close
    /// op must be a transitive ancestor or descendant of it, never a concurrent
    /// sibling that could throw and `cancelAll()` mid-suspension. Default `false`.
    var awaitsExternalInput: Bool { get }

    /// Guard + outcome-based blocking. See default below.
    func shouldRun(_ ctx: RunContext) async -> Bool
    /// The work. Throwing → `.failed`; returning → `.success`.
    func run(_ ctx: RunContext) async throws -> Value
}

public extension Operation {
    var dependsOn: [any Operation.Type] {
        []
    }

    var timeout: Duration? {
        nil
    }

    var awaitsExternalInput: Bool {
        false
    }

    /// Default cascade: run only if EVERY declared dependency succeeded.
    /// Override to take full control (the documented escape hatch for
    /// outcome-based blocking or running despite a failed-open dependency).
    func shouldRun(_ ctx: RunContext) async -> Bool {
        dependsOn.allSatisfy { ctx.succeeded($0) }
    }
}
