import OperationPipeline

public struct Credentials: Sendable {
    public let username: String
    public let password: String
    public init(username: String, password: String) {
        self.username = username
        self.password = password
    }
}

/// The backend auth call the module already owns.
public protocol AuthBackend: Sendable {
    func authenticate(_ credentials: Credentials) async throws
}

/// The self-contained auth module. Owns the state machine and publishes
/// `AuthState`. Knows nothing about concrete operations — it only awaits the
/// injected `GatingRunner`. UI presentation is the host's concern, driven by
/// the published state.
public actor AuthModule {
    private let backend: AuthBackend
    private let gating: GatingRunner

    public private(set) var state: AuthState = .unauthenticated(nil) {
        didSet { continuation.yield(state) }
    }

    /// Single-consumer stream of state changes. `AsyncStream` does not broadcast
    /// to multiple iterators; the host should consume this from one place (e.g.
    /// the root coordinator) and fan out if needed.
    public nonisolated let stateStream: AsyncStream<AuthState>
    private let continuation: AsyncStream<AuthState>.Continuation

    public init(backend: AuthBackend, gating: GatingRunner) {
        self.backend = backend
        self.gating = gating
        (stateStream, continuation) = AsyncStream.makeStream(of: AuthState.self)
    }

    /// Called on app start; host presents login UI when state is unauthenticated.
    public func start() {
        continuation.yield(state) // emit current state to fresh observers
    }

    public func login(_ credentials: Credentials) async {
        guard case .unauthenticated = state else {
            return
        }
        state = .authenticating

        do {
            try await backend.authenticate(credentials)
        } catch {
            guard case .authenticating = state else {
                return
            }
            state = .unauthenticated(AuthError(failingOperation: nil, message: String(describing: error)))
            return
        }

        guard case .authenticating = state else {
            return
        }
        switch await gating.run() {
        case .success:
            guard case .authenticating = state else {
                return
            }
            state = .authenticated
        case let .aborted(error):
            guard case .authenticating = state else {
                return
            }
            state = .unauthenticated(AuthError(failure: error))
        }
    }

    public func logout() {
        state = .unauthenticated(nil)
    }
}
