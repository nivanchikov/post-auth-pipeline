import OperationPipeline

/// Observes the auth module's state stream and runs the post-login pipeline on
/// the SHARED session scope when authentication completes; cancels and clears
/// the session on logout. The auth module has no knowledge of this.
public final class PostLoginCoordinator: Sendable {
    private let engine: OperationEngine
    private let pipeline: Pipeline
    private let broker: SessionScopeBroker

    public init(engine: OperationEngine, pipeline: Pipeline, broker: SessionScopeBroker) {
        self.engine = engine
        self.pipeline = pipeline
        self.broker = broker
    }

    /// Start observing. Returns the observation task; cancel it to stop observing.
    ///
    /// Note: cancelling the returned task stops consuming the stream, but does
    /// NOT itself cancel an in-flight post-login run (`runTask` is unstructured).
    /// The post-login run is cancelled by the `.unauthenticated` branch below —
    /// i.e. logout cancels the run, which is the guarantee we actually rely on.
    public func observe(_ stream: AsyncStream<AuthState>) -> Task<Void, Never> {
        let engine = engine, pipeline = pipeline, broker = broker
        return Task {
            var runTask: Task<Void, Never>?
            for await state in stream {
                switch state {
                case .authenticated:
                    runTask?.cancel()
                    if let scope = await broker.currentSession() {
                        runTask = Task { _ = await engine.run(pipeline, scope: scope) }
                    }
                case .unauthenticated:
                    runTask?.cancel()                 // cancel in-flight post-login
                    if let scope = await broker.currentSession() {
                        await broker.endSession(scope) // drop the session context
                    }
                case .authenticating:
                    break
                }
            }
            runTask?.cancel()
        }
    }
}
