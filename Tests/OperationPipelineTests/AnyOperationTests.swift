// swiftlint:disable identifier_name
@testable import OperationPipeline
import Testing

private struct EchoOp: Operation {
    typealias Value = Int
    let out: Int
    var failurePolicy: FailurePolicy {
        .open
    }

    func run(_: RunContext) async throws -> Int {
        out
    }
}

private struct ThrowingOp: Operation {
    typealias Value = Int
    var failurePolicy: FailurePolicy {
        .close
    }

    func run(_: RunContext) async throws -> Int {
        throw PlainError(code: 5)
    }
}

@Test func anyOperationCarriesIdentityAndPolicy() {
    let any = AnyOperation(EchoOp(out: 3))
    #expect(any.id == ObjectIdentifier(EchoOp.self))
    #expect(any.failurePolicy == .open)
    #expect(any.dependsOn.isEmpty)
}

@Test func anyOperationRunBoxesSuccess() async {
    let any = AnyOperation(EchoOp(out: 7))
    let box = await any.run(PipelineContext(outcomes: [:]))
    #expect(box.kind == .success)
    if case let .success(v)? = box.unwrap(as: Int.self) {
        #expect(v == 7)
    } else {
        Issue.record("expected success")
    }
}

@Test func anyOperationRunBoxesThrowAsFailed() async {
    let any = AnyOperation(ThrowingOp())
    let box = await any.run(PipelineContext(outcomes: [:]))
    #expect(box.kind == .failed)
    #expect((box.failureError as? PlainError) == PlainError(code: 5))
}

@Test func anyOperationFactoriesUseConcreteValueType() {
    let any = AnyOperation(EchoOp(out: 1))
    // skipped/failed boxes must unwrap as Int (the op's Value), not crash.
    #expect(any.makeSkipped(reason: "g").unwrap(as: Int.self) != nil)
    #expect(any.makeFailed(PlainError(code: 9)).unwrap(as: Int.self) != nil)
}

// swiftlint:enable identifier_name
