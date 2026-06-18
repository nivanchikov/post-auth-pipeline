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

/// Incrementing counter, safe for concurrent access.
actor Counter {
    private var n = 0
    func next() -> Int { n += 1; return n }
}

extension Outcome {
    var successValue: Value? { if case .success(let v) = self { return v }; return nil }
}

/// Post-login-style op (no dependency edge) that reads OpA's value from the
/// shared session context (live map), proving cross-pipeline reads work.
struct ReadsA: OperationPipeline.Operation {
    typealias Value = String
    let log: RunLog
    var dependsOn: [any OperationPipeline.Operation.Type] { [] }   // NO edge — reads via shared context
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String {
        await log.record("ReadsA")
        return "saw:\(ctx.value(of: OpA.self) ?? "nil")"
    }
}

/// D depends on B and C; overrides the default cascade to run when C succeeded,
/// even though B failed-open. Reads C's value, tolerates B being nil.
struct OverrideD: OperationPipeline.Operation {
    typealias Value = String
    let name: String
    let log: RunLog
    var dependsOn: [any OperationPipeline.Operation.Type] { [OpB.self, OpC.self] }
    var failurePolicy: FailurePolicy { .open }

    func shouldRun(_ ctx: RunContext) async -> Bool {
        ctx.succeeded(OpC.self)                  // run on C alone; ignore B
    }
    func run(_ ctx: RunContext) async throws -> String {
        await log.record(name)
        #expect(ctx.value(of: OpB.self) == nil)  // B failed → nil, handled
        return ctx.value(of: OpC.self) ?? "?"
    }
}
