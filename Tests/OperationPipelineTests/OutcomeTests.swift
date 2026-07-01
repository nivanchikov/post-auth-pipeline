@testable import OperationPipeline
import Testing

struct PlainError: Error, Sendable, Equatable { let code: Int }

@Test func pipelineFailurePreservesOperationAndMessage() {
    let error = PlainError(code: 7)
    let failure = PipelineFailure(operation: "FetchEntitlements", underlying: error)
    #expect(failure.operation == "FetchEntitlements")
    #expect(failure.message == String(describing: error))
}

@Test func failurePolicyHasOpenAndClose() {
    #expect(FailurePolicy.open != FailurePolicy.close)
}
