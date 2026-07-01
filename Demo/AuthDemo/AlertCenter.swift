import Auth
import SwiftUI

/// The answer vocabulary for the demo's welcome alert. Owned by the host, so the
/// operation's switch over it is exhaustive and compiler-checked.
enum WelcomeChoice: Sendable, Hashable {
    case proceed
    case remind
    case decline
}

/// Host-owned presenter. It is BOTH the `AlertPresenter` an operation awaits and
/// the observable state a SwiftUI `.alert` renders. The op-facing side is async
/// (it awaits `present`); the UI-facing side is a button action (`choose`). The
/// `ContinuationGate` is the single bridge between the two.
@MainActor
final class AlertCenter: ObservableObject, AlertPresenter {
    /// The alert currently on screen, or `nil`. Drives the SwiftUI `.alert`.
    @Published private(set) var current: Auth.Alert<WelcomeChoice>?
    private var gate: ContinuationGate<WelcomeChoice>?

    // op side — suspends until the user answers on screen.
    func present(_ alert: Auth.Alert<WelcomeChoice>) async -> WelcomeChoice {
        let gate = ContinuationGate<WelcomeChoice>()
        self.gate = gate
        current = alert
        let choice = await gate.value()
        current = nil
        self.gate = nil
        return choice
    }

    // UI side — the alert button action.
    func choose(_ choice: WelcomeChoice) {
        let gate = gate
        Task { await gate?.resume(with: choice) }
    }
}
