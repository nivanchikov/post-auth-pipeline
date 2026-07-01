import Auth
import OperationPipeline
import SwiftUI

/// Wires the real auth module + gating pipeline (Entitlements → UserAccounts →
/// PresentAlerts) and mirrors the module's published state onto the main actor
/// for SwiftUI.
@MainActor
final class DemoModel: ObservableObject {
    @Published private(set) var state: AuthState = .unauthenticated(nil)
    let alerts = AlertCenter()
    private let module: AuthModule

    static let welcome = Auth.Alert<WelcomeChoice>(
        id: "welcome",
        title: "Welcome back",
        message: "Your Pro entitlements are active. Choose how to continue.",
        actions: [
            AlertAction(.proceed, title: "Continue"),
            AlertAction(.decline, title: "Decline terms", role: .destructive),
            // Explicit cancel-role action: occupies the alert's cancel slot so
            // iOS does NOT synthesize its own Cancel (which would dismiss the
            // alert WITHOUT delivering a choice, hanging the suspended op).
            AlertAction(.remind, title: "Remind me later", role: .cancel)
        ]
    )

    init() {
        let engine = OperationEngine()
        let presenter = alerts
        let gating = try! buildPipeline([
            AnyOperation(EntitlementsOp(alert: Self.welcome)),
            AnyOperation(UserAccountsOp()),
            AnyOperation(PresentAlertsOp(presenter: presenter))
        ])
        module = AuthModule(
            backend: DemoBackend(),
            gating: EngineGatingRunner(engine: engine, pipeline: gating)
        )
        let stream = module.stateStream
        Task { @MainActor [weak self] in
            for await newState in stream {
                self?.state = newState
            }
        }
    }

    var isAuthenticating: Bool {
        if case .authenticating = state { true } else { false }
    }

    func login() {
        Task { await module.login(Credentials(username: "u", password: "p")) }
    }

    func logout() {
        Task { await module.logout() }
    }
}

@main
struct AuthDemoApp: App {
    var body: some Scene {
        WindowGroup { RootView() }
    }
}

struct RootView: View {
    @StateObject private var model = DemoModel()
    var body: some View {
        ContentView(model: model, alerts: model.alerts)
    }
}

struct ContentView: View {
    @ObservedObject var model: DemoModel
    @ObservedObject var alerts: AlertCenter

    var body: some View {
        Group {
            if case .authenticated = model.state {
                HomeView(model: model)
            } else {
                LoginView(model: model)
            }
        }
        .alert(
            alerts.current?.title ?? "",
            isPresented: Binding(get: { alerts.current != nil }, set: { _ in }),
            presenting: alerts.current
        ) { alert in
            ForEach(alert.actions) { action in
                Button(action.title, role: action.role.buttonRole) {
                    alerts.choose(action.choice)
                }
                .accessibilityIdentifier("alert-\(action.choice)")
            }
        } message: { alert in
            Text(alert.message)
        }
    }
}

private extension AlertAction<WelcomeChoice>.Role {
    var buttonRole: ButtonRole? {
        switch self {
        case .normal: nil
        case .cancel: .cancel
        case .destructive: .destructive
        }
    }
}

struct LoginView: View {
    @ObservedObject var model: DemoModel

    var body: some View {
        VStack(spacing: 24) {
            Text("Auth Demo").font(.largeTitle.bold())
            if model.isAuthenticating {
                ProgressView("Signing in…")
            }
            Button("Log in") { model.login() }
                .buttonStyle(.borderedProminent)
                .disabled(model.isAuthenticating)
                .accessibilityIdentifier("login-button")
            if case let .unauthenticated(error?) = model.state {
                Text(error.message).font(.footnote).foregroundStyle(.red)
            }
        }
        .padding()
    }
}

struct HomeView: View {
    @ObservedObject var model: DemoModel

    var body: some View {
        VStack(spacing: 24) {
            Text("Logged in 🎉")
                .font(.largeTitle.bold())
                .accessibilityIdentifier("home-title")
            Text("Login screen dismissed only after you tapped OK.")
                .multilineTextAlignment(.center)
                .foregroundStyle(.secondary)
            Button("Log out") { model.logout() }
                .accessibilityIdentifier("logout-button")
        }
        .padding()
    }
}
