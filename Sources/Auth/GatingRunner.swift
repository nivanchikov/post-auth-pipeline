import OperationPipeline

/// The single opaque dependency the auth module awaits during login. The auth
/// module knows nothing about concrete operations behind this protocol.
public protocol GatingRunner: Sendable {
    func run() async -> GatingResult
}

public enum GatingResult: Sendable {
    case success
    case aborted(PipelineFailure)
}
