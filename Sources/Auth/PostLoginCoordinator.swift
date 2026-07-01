import OperationPipeline

/// Observes the auth module's state stream and runs the post-login pipeline on
/// authentication; cancels it on logout. The auth module has no knowledge of this.
public final class PostLoginCoordinator: Sendable {
    private let engine: OperationEngine
    private let pipeline: Pipeline

    public init(engine: OperationEngine, pipeline: Pipeline) {
        self.engine = engine
        self.pipeline = pipeline
    }

    /// Start observing. Returns the observation task; cancel it to stop observing.
    ///
    /// Note: cancelling the returned task stops consuming the stream, but does
    /// NOT itself cancel an in-flight post-login run (`runTask` is unstructured).
    /// The post-login run is cancelled by the `.unauthenticated` branch below —
    /// i.e. logout cancels the run, which is the guarantee we actually rely on.
    public func observe(_ stream: AsyncStream<AuthState>) -> Task<Void, Never> {
        let engine = engine, pipeline = pipeline
        return Task {
            var runTask: Task<Void, Never>?
            // True if we have seen .authenticating or .authenticated since the
            // last .unauthenticated. Prevents stale buffered unauthenticated
            // events from clearing a brand-new session, and lets .authenticating
            // cancel old post-login work on re-auth.
            var hadActiveSession = false
            for await state in stream {
                switch state {
                case .authenticated:
                    hadActiveSession = true
                    runTask?.cancel()
                    runTask = Task { _ = await engine.run(pipeline) }
                case .authenticating:
                    hadActiveSession = true
                    runTask?.cancel() // stop old post-login on re-auth
                case .unauthenticated:
                    guard hadActiveSession else {
                        continue
                    }
                    hadActiveSession = false
                    runTask?.cancel() // cancel in-flight post-login
                }
            }
            runTask?.cancel()
        }
    }
}
