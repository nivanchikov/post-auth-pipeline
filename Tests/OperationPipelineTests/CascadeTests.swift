import Testing
@testable import OperationPipeline

@Test func dependentDefaultsToSkippedWhenDependencyFailsOpen() async throws {
    let log = RunLog()
    // A fails-open; B depends on A with the DEFAULT guard → B must be skipped.
    let a = OpA(name: "A", log: log, deps: [], policy: .open,
                body: { _ in throw PlainError(code: 1) })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b)])

    let scope = SessionScope()
    let result = await OperationEngine().run(pipeline, scope: scope)

    #expect(result.isCompleted)                 // fail-open does not abort
    let order = await log.order
    #expect(order == ["A"])                      // B never ran
    let live = await scope.liveSnapshot()
    #expect(live[ObjectIdentifier(OpB.self)]?.kind == .skipped)
}

@Test func diamondWithGuardOverrideRunsDespiteFailedDependency() async throws {
    // A -> {B(fails-open), C(succeeds)} -> D. D overrides to run on C alone.
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open,
                body: { _ in throw PlainError(code: 2) })
    let c = OpC(name: "C", log: log, deps: [OpA.self], policy: .open, body: { _ in "c" })
    let d = OverrideD(name: "D", log: log)
    let pipeline = try buildPipeline([
        AnyOperation(a), AnyOperation(b), AnyOperation(c), AnyOperation(d),
    ])

    let scope = SessionScope()
    let result = await OperationEngine().run(pipeline, scope: scope)

    #expect(result.isCompleted)
    let order = await log.order
    #expect(order.contains("D"))                 // override made D run
    let live = await scope.liveSnapshot()
    #expect(live[ObjectIdentifier(OverrideD.self)]?.kind == .success)
}
