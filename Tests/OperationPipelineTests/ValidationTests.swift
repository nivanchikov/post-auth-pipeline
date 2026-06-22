// swiftlint:disable identifier_name
@testable import OperationPipeline
import Testing

@Test func rejectsDuplicateOperationType() {
    let log = RunLog()
    let a1 = OpA(name: "A1", log: log, deps: [], policy: .open, body: { _ in "a" })
    let a2 = OpA(name: "A2", log: log, deps: [], policy: .open, body: { _ in "a" })
    #expect(throws: PipelineValidationError.self) {
        _ = try buildPipeline([AnyOperation(a1), AnyOperation(a2)])
    }
}

@Test func rejectsUnregisteredDependency() {
    let log = RunLog()
    // B depends on A, but A is not registered (also covers cross-pipeline edges).
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    #expect(throws: PipelineValidationError.self) {
        _ = try buildPipeline([AnyOperation(b)])
    }
}

@Test func rejectsCycle() {
    let log = RunLog()
    // A depends on B, B depends on A → cycle.
    let a = OpA(name: "A", log: log, deps: [OpB.self], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    #expect(throws: PipelineValidationError.self) {
        _ = try buildPipeline([AnyOperation(a), AnyOperation(b)])
    }
}

@Test func acceptsValidAcyclicGraph() throws {
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b)])
    #expect(pipeline.operations.count == 2)
}

// swiftlint:enable identifier_name
