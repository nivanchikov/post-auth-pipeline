import Foundation

/// The terminal result of running one operation.
public enum Outcome<Value: Sendable>: Sendable {
    case success(Value)
    case skipped(reason: String)
    case failed(any Error)
}

/// Whether an operation's failure aborts login or is tolerated.
public enum FailurePolicy: Sendable, Equatable {
    case open // failure tolerated; the pipeline proceeds without this op
    case close // failure aborts the pipeline (login)
}

/// Concrete failure surfaced by a pipeline abort.
public struct PipelineFailure: Error, Sendable, Equatable {
    public let operation: String
    public let message: String

    public init(operation: String, message: String) {
        self.operation = operation
        self.message = message
    }

    public init(operation: String, underlying: any Error) {
        self.init(operation: operation, message: String(describing: underlying))
    }
}

/// Produced when an operation exceeds its declared `timeout`.
public struct TimeoutError: Error, Sendable, Equatable {
    public let operation: String
    public init(operation: String) {
        self.operation = operation
    }
}
