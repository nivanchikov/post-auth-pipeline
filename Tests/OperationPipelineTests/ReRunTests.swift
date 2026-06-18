import Testing
@testable import OperationPipeline

@Test func reRunOverwritesPublishedValuesWithLatest() async throws {
    let log = RunLog()
    // The op returns a different value on each run via an external counter.
    let counter = Counter()
    let a = OpA(name: "A", log: log, deps: [], policy: .open,
                body: { _ in "v\(await counter.next())" })
    let pipeline = try buildPipeline([AnyOperation(a)])
    let engine = OperationEngine()
    let scope = SessionScope()

    _ = await engine.run(pipeline, scope: scope)
    let first = await scope.liveSnapshot()[ObjectIdentifier(OpA.self)]
    #expect(first?.unwrap(as: String.self)?.successValue == "v1")

    _ = await engine.run(pipeline, scope: scope)         // re-run, same scope
    let second = await scope.liveSnapshot()[ObjectIdentifier(OpA.self)]
    #expect(second?.unwrap(as: String.self)?.successValue == "v2")  // latest wins
}

@Test func crossPipelineReadsSeeLastPublishedValueDuringNoRun() async throws {
    // After a gating run publishes, a separate (post-login) op reading via a
    // fresh run sees the published value through the shared live map.
    let log = RunLog()
    let gating = OpA(name: "G", log: log, deps: [], policy: .open, body: { _ in "ent" })
    let gatingPipeline = try buildPipeline([AnyOperation(gating)])

    // Post-login op reads OpA's published value from the shared live map.
    let post = ReadsA(log: log)
    let postPipeline = try buildPipeline([AnyOperation(post)])

    let engine = OperationEngine()
    let scope = SessionScope()
    _ = await engine.run(gatingPipeline, scope: scope)
    _ = await engine.run(postPipeline, scope: scope)

    let live = await scope.liveSnapshot()
    #expect(live[ObjectIdentifier(ReadsA.self)]?.unwrap(as: String.self)?.successValue == "saw:ent")
}
