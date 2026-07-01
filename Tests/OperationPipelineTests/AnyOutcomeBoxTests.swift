// swiftlint:disable identifier_name
@testable import OperationPipeline
import Testing

@Test func boxRecoversSuccessValue() {
    let box = AnyOutcomeBox(Outcome<Int>.success(42))
    #expect(box.kind == .success)
    if case let .success(v)? = box.unwrap(as: Int.self) {
        #expect(v == 42)
    } else {
        Issue.record("expected success")
    }
}

@Test func boxReportsSkippedAndFailedKinds() {
    #expect(AnyOutcomeBox(Outcome<Int>.skipped(reason: "x")).kind == .skipped)
    let failed = AnyOutcomeBox(Outcome<Int>.failed(PlainError(code: 1)))
    #expect(failed.kind == .failed)
    #expect((failed.failureError as? PlainError) == PlainError(code: 1))
}

@Test func unwrapToCorrectTypeSucceeds() {
    let box = AnyOutcomeBox(Outcome<String>.success("hi"))
    #expect(box.unwrap(as: String.self) != nil)
}

// swiftlint:enable identifier_name
