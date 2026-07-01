// swiftlint:disable identifier_name
@testable import OperationPipeline
import Testing

@Test func runsAllIndependentOperations() async throws {
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [], policy: .open, body: { _ in "b" })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b)])

    let engine = OperationEngine()
    let scope = RunState()
    let result = await engine.run(pipeline, state: scope)

    #expect(result.isCompleted)
    let order = await log.order
    #expect(Set(order) == ["A", "B"])
}

@Test func respectsDependencyOrdering() async throws {
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    // B depends on A.
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { ctx in
        #expect(ctx.value(of: OpA.self) == "a") // A's value is available
        return "b"
    })
    let pipeline = try buildPipeline([AnyOperation(b), AnyOperation(a)]) // order shuffled

    let engine = OperationEngine()
    let result = await engine.run(pipeline, state: RunState())

    #expect(result.isCompleted)
    let order = await log.order
    #expect(order == ["A", "B"]) // A always before B regardless of registration order
}

@Test func independentBranchesRunConcurrently() async throws {
    // A -> B, A -> C ; B and C are independent of each other.
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    let c = OpC(name: "C", log: log, deps: [OpA.self], policy: .open, body: { _ in "c" })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b), AnyOperation(c)])

    let result = await OperationEngine().run(pipeline, state: RunState())
    #expect(result.isCompleted)
    let order = await log.order
    #expect(order.first == "A") // A precedes both
    #expect(Set(order) == ["A", "B", "C"])
}

// swiftlint:enable identifier_name
