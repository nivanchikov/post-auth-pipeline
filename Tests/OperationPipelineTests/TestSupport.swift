import Foundation
import Testing            // needed once helpers (Task 8's OverrideD) use #expect
@testable import OperationPipeline

/// Records the order in which operations ran (thread-safe).
actor RunLog {
    private(set) var order: [String] = []
    func record(_ name: String) { order.append(name) }
}

/// A synthetic op that records when it runs and returns its name length.
/// Generic over a phantom marker so each becomes a distinct type/identity.
struct Probe<Marker>: OperationPipeline.Operation {
    typealias Value = String
    let name: String
    let log: RunLog
    let deps: [any OperationPipeline.Operation.Type]
    let policy: FailurePolicy
    let body: @Sendable (RunContext) async throws -> String

    var dependsOn: [any OperationPipeline.Operation.Type] { deps }
    var failurePolicy: FailurePolicy { policy }

    func run(_ ctx: RunContext) async throws -> String {
        await log.record(name)
        return try await body(ctx)
    }
}

// Distinct phantom markers → distinct Operation types → distinct identities.
enum MA {}; enum MB {}; enum MC {}; enum MD {}
typealias OpA = Probe<MA>
typealias OpB = Probe<MB>
typealias OpC = Probe<MC>
typealias OpD = Probe<MD>
