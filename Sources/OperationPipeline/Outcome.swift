import Foundation

/// The terminal result of running one operation.
public enum Outcome<Value: Sendable>: Sendable {
    case success(Value)
    case skipped(reason: String)
    case failed(any Error & Sendable)   // bare `any Error` is NOT Sendable
}

/// Whether an operation's failure aborts login or is tolerated.
public enum FailurePolicy: Sendable, Equatable {
    case open   // failure tolerated; the pipeline proceeds without this op
    case close  // failure aborts the pipeline (login)
}

/// Wraps a non-Sendable error so it can cross concurrency boundaries.
public struct SendableErrorBox: Error, Sendable {
    public let message: String
    public init(_ error: any Error) { self.message = String(describing: error) }
    public var localizedDescription: String { message }
}

/// Coerce any thrown error into `any Error & Sendable`, wrapping if necessary.
public func asSendable(_ error: any Error) -> any Error & Sendable {
    (error as? any Error & Sendable) ?? SendableErrorBox(error)
}
