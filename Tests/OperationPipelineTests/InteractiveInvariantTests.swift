// swiftlint:disable identifier_name
@testable import OperationPipeline
import Testing

/// The invariant: an operation that suspends on external input (an alert ack)
/// does not observe cancellation while suspended, so the pipeline must guarantee
/// it is never abort-cancelled. That holds iff every fail-close op is COMPARABLE
/// to it in the DAG (a transitive ancestor or descendant) — never a concurrent
/// sibling that could throw and trigger `cancelAll()` mid-suspension.
@Suite struct InteractiveInvariantTests {
    private func op<M>(
        _: M.Type,
        deps: [any OperationPipeline.Operation.Type] = [],
        policy: FailurePolicy = .close,
        interactive: Bool = false
    ) -> Probe<M> {
        Probe<M>(name: "\(M.self)", log: RunLog(), deps: deps, policy: policy,
                 interactive: interactive, body: { _ in "x" })
    }

    @Test func rejectsFailCloseSiblingRacingInteractive() {
        // S(interactive) depends on A; B is fail-close but incomparable to S.
        let s = op(MA.self, deps: [OpB.self], interactive: true)
        let a = op(MB.self)
        let b = op(MC.self) // fail-close, unrelated to S → can abort mid-alert
        #expect(throws: PipelineValidationError.self) {
            _ = try buildPipeline([AnyOperation(s), AnyOperation(a), AnyOperation(b)])
        }
    }

    @Test func acceptsInteractiveDependingOnAllFailClose() throws {
        let s = op(MA.self, deps: [OpB.self, OpC.self], interactive: true)
        let a = op(MB.self)
        let b = op(MC.self)
        let pipeline = try buildPipeline([AnyOperation(s), AnyOperation(a), AnyOperation(b)])
        #expect(pipeline.operations.count == 3)
    }

    @Test func acceptsTransitiveAncestorNotRedeclared() throws {
        // A ← B ← S(interactive). S depends only on B, but A is a transitive
        // ancestor, so A already completed before S suspends — comparable, safe.
        let a = op(MA.self) // fail-close, reached only transitively
        let b = op(MB.self, deps: [OpA.self])
        let s = op(MC.self, deps: [OpB.self], interactive: true)
        let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b), AnyOperation(s)])
        #expect(pipeline.operations.count == 3)
    }

    @Test func acceptsFailOpenSiblingRacingInteractive() throws {
        // A fail-OPEN sibling can't abort, so racing the alert is fine.
        let s = op(MA.self, deps: [OpB.self], interactive: true)
        let a = op(MB.self)
        let b = op(MC.self, policy: .open) // incomparable but fail-open → no abort
        let pipeline = try buildPipeline([AnyOperation(s), AnyOperation(a), AnyOperation(b)])
        #expect(pipeline.operations.count == 3)
    }

    @Test func acceptsFailCloseDescendantOfInteractive() throws {
        // A fail-close op downstream of S is gated behind the ack, so it can't
        // fail while S suspends — comparable (descendant), safe.
        let s = op(MA.self, interactive: true)
        let d = op(MB.self, deps: [OpA.self]) // fail-close, depends on S
        let pipeline = try buildPipeline([AnyOperation(s), AnyOperation(d)])
        #expect(pipeline.operations.count == 2)
    }
}

// swiftlint:enable identifier_name
