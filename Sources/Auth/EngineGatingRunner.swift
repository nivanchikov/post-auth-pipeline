import OperationPipeline

/// Implements `GatingRunner` by running the gating pipeline on a fresh session
/// scope obtained from the broker. On abort, drops the session (login failed).
public struct EngineGatingRunner: GatingRunner {
    private let engine: OperationEngine
    private let pipeline: Pipeline
    private let broker: SessionScopeBroker

    public init(engine: OperationEngine, pipeline: Pipeline, broker: SessionScopeBroker) {
        self.engine = engine
        self.pipeline = pipeline
        self.broker = broker
    }

    public func run() async -> GatingResult {
        let scope = await broker.beginSession()
        switch await engine.run(pipeline, scope: scope) {
        case .completed:
            // Guard against stale runs: a newer session may have replaced this
            // one, or the task may have been cancelled. Don't authenticate on a
            // stale or cancelled run.
            let stillCurrent = await broker.currentSession() === scope
            if Task.isCancelled || !stillCurrent {
                await broker.endSession(scope)
                return .aborted(SendableErrorBox(CancellationError()))
            }
            return .success
        case .aborted(let error):
            await broker.endSession(scope)   // login failed → drop the session context
            return .aborted(error)
        }
    }
}
