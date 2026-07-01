// swiftlint:disable identifier_name
@testable import OperationPipeline
import Testing

@Test func failCloseAbortsRunWithError() async throws {
    let log = RunLog()
    let a = OpA(
        name: "A",
        log: log,
        deps: [],
        policy: .close,
        body: { _ in throw PlainError(code: 42) }
    )
    let pipeline = try buildPipeline([AnyOperation(a)])

    let result = await OperationEngine().run(pipeline)

    if case let .aborted(failure) = result {
        #expect(failure.operation == "Probe<MA>")
        #expect(failure.message.contains("42"))
    } else {
        Issue.record("expected .aborted")
    }
}

@Test func failOpenDoesNotAbortAndPrunesDependents() async throws {
    let log = RunLog()
    let a = OpA(
        name: "A",
        log: log,
        deps: [],
        policy: .open,
        body: { _ in throw PlainError(code: 1) }
    )
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    let c = OpC(name: "C", log: log, deps: [], policy: .open, body: { _ in "c" }) // independent
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b), AnyOperation(c)])

    let result = await OperationEngine().run(pipeline)

    #expect(result.isCompleted)
    let order = await log.order
    #expect(order.contains("C")) // independent op still ran
    #expect(!order.contains("B")) // dependent of failed-open A was pruned (skipped)
}

@Test func firstFailCloseWins() async throws {
    // Two close-failures; result carries the first observed error, run aborts.
    let log = RunLog()
    let a = OpA(
        name: "A",
        log: log,
        deps: [],
        policy: .close,
        body: { _ in throw PlainError(code: 1) }
    )
    let b = OpB(
        name: "B",
        log: log,
        deps: [],
        policy: .close,
        body: { _ in throw PlainError(code: 2) }
    )
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b)])

    let result = await OperationEngine().run(pipeline)
    if case let .aborted(failure) = result {
        #expect(["Probe<MA>", "Probe<MB>"].contains(failure.operation))
        #expect(failure.message.contains("1") || failure.message.contains("2"))
    } else {
        Issue.record("expected abort")
    }
}

// swiftlint:enable identifier_name
