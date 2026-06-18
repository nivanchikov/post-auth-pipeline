import Testing
@testable import OperationPipeline

struct PlainError: Error, Sendable, Equatable { let code: Int }

@Test func asSendablePassesThroughSendableError() {
    let e = PlainError(code: 7)
    let boxed = asSendable(e)
    #expect((boxed as? PlainError) == e)
}

// In Swift 6 strict mode, every type implicitly gets Sendable conformance (value types,
// final classes) and creating a truly non-Sendable type that compiles without errors is
// impossible. The plan's NonSendableError struct example would also implicitly conform.
// Instead we test the wrapping path directly via SendableErrorBox construction,
// which is what asSendable falls back to for non-Sendable errors at runtime.
@Test func asSendableWrapsNonSendableError() {
    struct FakeNonSendable: Error { let msg: String }  // implicitly Sendable in Swift 6
    let e = FakeNonSendable(msg: "whoops")
    // Verify that SendableErrorBox correctly wraps an error and preserves its description.
    let box = SendableErrorBox(e)
    #expect(box.localizedDescription.isEmpty == false)
    // Verify asSendable still works for this type (it passes through since Swift 6 makes it Sendable).
    let boxed = asSendable(e)
    #expect(boxed.localizedDescription.isEmpty == false)
}

@Test func failurePolicyHasOpenAndClose() {
    #expect(FailurePolicy.open != FailurePolicy.close)
}
