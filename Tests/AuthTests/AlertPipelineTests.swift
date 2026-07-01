@testable import Auth
import OperationPipeline
import Testing

/// The caller-owned answer vocabulary for the welcome alert.
private enum Choice: Sendable, Hashable {
    case proceed
    case remind
    case decline
}

/// Records completion order across operations so tests can assert the alert ack
/// is the last thing to happen.
private actor Recorder {
    private(set) var events: [String] = []
    func record(_ event: String) {
        events.append(event)
    }
}

/// In-memory sink standing in for the entitlements service dependency.
private actor EntitlementsStore {
    private(set) var saved: [String] = []
    func save(_ entitlements: [String]) {
        saved = entitlements
    }
}

/// Presenter that records when it is asked to present and returns a canned
/// choice. Recording the call lets us prove entitlements + accounts already
/// finished before the alert is shown.
private struct RecordingPresenter: AlertPresenter {
    let recorder: Recorder
    let response: Choice
    let presented: PresentedBox

    func present(_ alert: Alert<Choice>) async -> Choice {
        await presented.set(alert)
        await recorder.record("present:\(alert.id)")
        return response
    }
}

private actor PresentedBox {
    private(set) var alert: Alert<Choice>?
    func set(_ alert: Alert<Choice>) {
        self.alert = alert
    }
}

// MARK: - Operations under test (host-domain shapes, mirrored by the demo app)

private struct EntitlementsPayload: Sendable {
    let entitlements: [String]
    let alert: Alert<Choice>?
}

private struct EntitlementsOp: Operation {
    let store: EntitlementsStore
    let recorder: Recorder
    let alert: Alert<Choice>?
    var failurePolicy: FailurePolicy { .close }

    func run(_: RunContext) async throws -> EntitlementsPayload {
        await store.save(["pro"]) // entitlements persist; the alert does not
        await recorder.record("entitlements")
        return EntitlementsPayload(entitlements: ["pro"], alert: alert)
    }
}

private struct UserAccountsOp: Operation {
    let recorder: Recorder
    var dependsOn: [any Operation.Type] { [EntitlementsOp.self] }
    var failurePolicy: FailurePolicy { .close }

    func run(_: RunContext) async throws -> String {
        await recorder.record("userAccounts")
        return "accounts-loaded"
    }
}

private struct PresentAlertsOp: Operation {
    let presenter: any AlertPresenter<Choice>
    var dependsOn: [any Operation.Type] { [EntitlementsOp.self, UserAccountsOp.self] }
    var failurePolicy: FailurePolicy { .close }
    var awaitsExternalInput: Bool { true }

    func run(_ ctx: RunContext) async throws {
        guard let alert = ctx.value(of: EntitlementsOp.self)?.alert else {
            return
        }
        switch await presenter.present(alert) {
        case .proceed, .remind:
            return // proceed to dismiss
        case .decline:
            throw AlertDeclined() // decline aborts login
        }
    }
}

private struct AlertDeclined: Error {}

private let sampleAlert = Alert<Choice>(
    id: "welcome",
    title: "Heads up",
    message: "Terms changed",
    actions: [
        AlertAction(.proceed, title: "Continue"),
        AlertAction(.remind, title: "Remind me later"),
        AlertAction(.decline, title: "Decline", role: .destructive)
    ]
)

@Suite struct AlertPipelineTests {
    private func makePipeline(
        recorder: Recorder,
        store: EntitlementsStore,
        presenter: RecordingPresenter,
        alert: Alert<Choice>?
    ) throws -> Pipeline {
        try buildPipeline([
            AnyOperation(EntitlementsOp(store: store, recorder: recorder, alert: alert)),
            AnyOperation(UserAccountsOp(recorder: recorder)),
            AnyOperation(PresentAlertsOp(presenter: presenter))
        ])
    }

    @Test func alertIsPresentedAfterEntitlementsAndAccounts() async throws {
        let recorder = Recorder(), store = EntitlementsStore()
        let presenter = RecordingPresenter(recorder: recorder, response: .proceed, presented: PresentedBox())
        let pipeline = try makePipeline(recorder: recorder, store: store, presenter: presenter, alert: sampleAlert)

        let result = await OperationEngine().run(pipeline)

        #expect(result.isCompleted)
        let events = await recorder.events
        #expect(events == ["entitlements", "userAccounts", "present:welcome"])
    }

    @Test func onlyEntitlementsArePersisted() async throws {
        let recorder = Recorder(), store = EntitlementsStore()
        let presenter = RecordingPresenter(recorder: recorder, response: .proceed, presented: PresentedBox())
        let pipeline = try makePipeline(recorder: recorder, store: store, presenter: presenter, alert: sampleAlert)

        _ = await OperationEngine().run(pipeline)

        #expect(await store.saved == ["pro"]) // the alert never reaches storage
        #expect(await presenter.presented.alert == sampleAlert)
    }

    @Test func declineChoiceAbortsTheRun() async throws {
        let recorder = Recorder(), store = EntitlementsStore()
        let presenter = RecordingPresenter(recorder: recorder, response: .decline, presented: PresentedBox())
        let pipeline = try makePipeline(recorder: recorder, store: store, presenter: presenter, alert: sampleAlert)

        let result = await OperationEngine().run(pipeline)

        guard case let .aborted(failure) = result else {
            Issue.record("expected abort on decline")
            return
        }
        #expect(failure.operation == "PresentAlertsOp")
    }

    @Test func nonDeclineChoiceProceeds() async throws {
        let recorder = Recorder(), store = EntitlementsStore()
        let presenter = RecordingPresenter(recorder: recorder, response: .remind, presented: PresentedBox())
        let pipeline = try makePipeline(recorder: recorder, store: store, presenter: presenter, alert: sampleAlert)

        let result = await OperationEngine().run(pipeline)

        #expect(result.isCompleted) // a non-abort choice dismisses the login screen
    }

    @Test func noAlertSkipsPresentationButStillCompletes() async throws {
        let recorder = Recorder(), store = EntitlementsStore()
        let presenter = RecordingPresenter(recorder: recorder, response: .proceed, presented: PresentedBox())
        let pipeline = try makePipeline(recorder: recorder, store: store, presenter: presenter, alert: nil)

        let result = await OperationEngine().run(pipeline)

        #expect(result.isCompleted)
        #expect(await presenter.presented.alert == nil)
        #expect(await recorder.events == ["entitlements", "userAccounts"])
    }
}
