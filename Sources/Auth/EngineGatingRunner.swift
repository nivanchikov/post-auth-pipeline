import OperationPipeline

/// Implements `GatingRunner` by running the gating pipeline on a fresh session
/// scope.
public struct EngineGatingRunner: GatingRunner {
    private let engine: OperationEngine
    private let pipeline: Pipeline

    public init(engine: OperationEngine, pipeline: Pipeline) {
        self.engine = engine
        self.pipeline = pipeline
    }

    public func run() async -> GatingResult {
        switch await engine.run(pipeline) {
        case .completed:
            if Task.isCancelled {
                return .aborted(PipelineFailure(operation: "OperationEngine", underlying: CancellationError()))
            }
            return .success
        case let .aborted(error):
            return .aborted(error)
        }
    }
}
