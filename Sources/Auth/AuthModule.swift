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

    public nonisolated let stateStream: AsyncStream<AuthState>
    private let continuation: AsyncStream<AuthState>.Continuation

    public init(backend: AuthBackend, gating: GatingRunner) {
        self.backend = backend
        self.gating = gating
        (stateStream, continuation) = AsyncStream.makeStream(of: AuthState.self)
    }

    /// Called on app start; host presents login UI when state is unauthenticated.
    public func start() {
        continuation.yield(state)   // emit current state to fresh observers
    }

    public func login(_ credentials: Credentials) async {
        state = .authenticating

        do {
            try await backend.authenticate(credentials)
        } catch {
            state = .unauthenticated(AuthError(failingOperation: nil, underlying: asSendable(error)))
            return
        }

        switch await gating.run() {
        case .success:
            state = .authenticated
        case .aborted(let error):
            state = .unauthenticated(AuthError(failingOperation: nil, underlying: error))
        }
    }

    public func logout() {
        state = .unauthenticated(nil)
    }
}
