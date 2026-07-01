// swiftlint:disable identifier_name
@testable import OperationPipeline
import Testing

private struct AOp: Operation {
    typealias Value = Int
    var dependsOn: [any Operation.Type] {
        []
    }

    var failurePolicy: FailurePolicy {
        .open
    }

    func run(_: RunContext) async throws -> Int {
        1
    }
}

private struct BOp: Operation {
    typealias Value = String
    var dependsOn: [any Operation.Type] {
        [AOp.self]
    }

    var failurePolicy: FailurePolicy {
        .open
    }

    func run(_: RunContext) async throws -> String {
        "b"
    }
}

@Test func contextReturnsTypedValueAndOutcome() {
    let ctx = PipelineContext(outcomes: [
        ObjectIdentifier(AOp.self): AnyOutcomeBox(Outcome<Int>.success(99))
    ])
    #expect(ctx.value(of: AOp.self) == 99)
    #expect(ctx.succeeded(AOp.self) == true)
    if case let .success(v)? = ctx.outcome(of: AOp.self) {
        #expect(v == 99)
    } else {
        Issue.record("expected success outcome")
    }
}

@Test func contextReturnsNilForAbsentOrNonSuccess() {
    let ctx = PipelineContext(outcomes: [
        ObjectIdentifier(AOp.self): AnyOutcomeBox(Outcome<Int>.skipped(reason: "x"))
    ])
    #expect(ctx.value(of: AOp.self) == nil) // skipped → no value
    #expect(ctx.succeeded(AOp.self) == false)
    #expect(ctx.value(of: BOp.self) == nil) // absent key
    #expect(ctx.succeeded(BOp.self) == false)
}

@Test func defaultShouldRunRequiresAllDependenciesSucceeded() async {
    let b = BOp()
    let allGood = PipelineContext(outcomes: [
        ObjectIdentifier(AOp.self): AnyOutcomeBox(Outcome<Int>.success(1))
    ])
    let depFailed = PipelineContext(outcomes: [
        ObjectIdentifier(AOp.self): AnyOutcomeBox(Outcome<Int>.failed(PlainError(code: 1)))
    ])
    #expect(await b.shouldRun(allGood) == true)
    #expect(await b.shouldRun(depFailed) == false) // default-skip cascade
}

// swiftlint:enable identifier_name
