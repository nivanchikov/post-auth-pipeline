import Auth
import Foundation
import OperationPipeline

/// In-memory entitlements storage — the "service dependency" from the design.
/// Entitlements land here; alerts never do.
actor EntitlementsStore {
    static let shared = EntitlementsStore()
    private(set) var saved: [String] = []
    func save(_ entitlements: [String]) {
        saved = entitlements
    }
}

/// The payload an entitlements request returns: the persisted part plus a
/// transient alert that rides along in the operation's outcome only.
struct EntitlementsPayload: Sendable {
    let entitlements: [String]
    let alert: Auth.Alert<WelcomeChoice>?
}

/// 1. Entitlements — persists entitlements, carries the alert forward. Fail-close.
struct EntitlementsOp: OperationPipeline.Operation {
    let alert: Auth.Alert<WelcomeChoice>?
    var failurePolicy: FailurePolicy { .close }

    func run(_: RunContext) async throws -> EntitlementsPayload {
        try? await Task.sleep(for: .milliseconds(150)) // simulate the network call
        await EntitlementsStore.shared.save(["pro", "beta-features"])
        return EntitlementsPayload(entitlements: ["pro", "beta-features"], alert: alert)
    }
}

/// 2. User accounts — runs after entitlements, before the alert.
struct UserAccountsOp: OperationPipeline.Operation {
    var dependsOn: [any OperationPipeline.Operation.Type] { [EntitlementsOp.self] }
    var failurePolicy: FailurePolicy { .close }

    func run(_: RunContext) async throws -> String {
        try? await Task.sleep(for: .milliseconds(150))
        return "accounts-loaded"
    }
}

/// 3. Present alerts — terminal node. Reads the alert from the entitlements
/// outcome via context and suspends on the ack. Nothing depends on it, so the
/// ack blocks only the dismiss, not the earlier operations.
struct PresentAlertsOp: OperationPipeline.Operation {
    let presenter: any AlertPresenter<WelcomeChoice>
    var dependsOn: [any OperationPipeline.Operation.Type] { [EntitlementsOp.self, UserAccountsOp.self] }
    var failurePolicy: FailurePolicy { .close }
    // Suspends on a human tap via a non-cancellation-observing gate. buildPipeline
    // enforces that no fail-close op races this one (see PipelineValidationError).
    var awaitsExternalInput: Bool { true }

    func run(_ ctx: RunContext) async throws {
        guard let alert = ctx.value(of: EntitlementsOp.self)?.alert else {
            return
        }
        switch await presenter.present(alert) {
        case .proceed, .remind:
            return // acknowledged → proceed to dismiss
        case .decline:
            throw AlertDeclined()
        }
    }
}

struct AlertDeclined: Error {}

/// Trivial backend that always authenticates.
struct DemoBackend: AuthBackend {
    func authenticate(_: Credentials) async throws {
        try? await Task.sleep(for: .milliseconds(200))
    }
}
