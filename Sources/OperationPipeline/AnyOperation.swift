import Foundation

/// Type-erased, executable wrapper around a concrete `Operation`. Captures the
/// op's `Value` type in closures so engine-created skipped/failed outcomes are
/// boxed with the SAME concrete type the op would produce (so later
/// `unwrap(as: O.Value.self)` reads never trap).
public struct AnyOperation: Sendable {
    let id: ObjectIdentifier
    let dependsOn: [ObjectIdentifier]
    let dependsOnTypes: [any Operation.Type] // retained for diagnostics
    let failurePolicy: FailurePolicy
    let timeout: Duration?
    let typeName: String

    private let _shouldRun: @Sendable (RunContext) async -> Bool
    private let _run: @Sendable (RunContext) async -> AnyOutcomeBox
    private let _skipped: @Sendable (String) -> AnyOutcomeBox
    private let _failed: @Sendable (any Error) -> AnyOutcomeBox

    public init<O: Operation>(_ op: O) {
        id = ObjectIdentifier(O.self)
        dependsOn = op.dependsOn.map { ObjectIdentifier($0) }
        dependsOnTypes = op.dependsOn
        failurePolicy = op.failurePolicy
        timeout = op.timeout
        typeName = String(describing: O.self)

        _shouldRun = { await op.shouldRun($0) }
        _run = { ctx in
            do {
                let value = try await op.run(ctx)
                return AnyOutcomeBox(Outcome<O.Value>.success(value))
            } catch {
                return AnyOutcomeBox(Outcome<O.Value>.failed(error))
            }
        }
        _skipped = { AnyOutcomeBox(Outcome<O.Value>.skipped(reason: $0)) }
        _failed = { AnyOutcomeBox(Outcome<O.Value>.failed($0)) }
    }

    func shouldRun(_ ctx: RunContext) async -> Bool {
        await _shouldRun(ctx)
    }

    func run(_ ctx: RunContext) async -> AnyOutcomeBox {
        await _run(ctx)
    }

    func makeSkipped(reason: String) -> AnyOutcomeBox {
        _skipped(reason)
    }

    func makeFailed(_ error: any Error) -> AnyOutcomeBox {
        _failed(error)
    }
}
