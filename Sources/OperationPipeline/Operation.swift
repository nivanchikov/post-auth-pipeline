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

    /// Default cascade: run only if EVERY declared dependency succeeded.
    /// Override to take full control (the documented escape hatch for
    /// outcome-based blocking or running despite a failed-open dependency).
    func shouldRun(_ ctx: RunContext) async -> Bool {
        dependsOn.allSatisfy { ctx.succeeded($0) }
    }
}
