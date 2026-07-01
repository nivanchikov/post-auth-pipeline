@testable import Auth
import Testing

/// The one-shot handoff that lets a suspended operation park on a human tap.
/// Generic over the delivered value; the alert path uses `ContinuationGate<AlertAction>`.
/// No cancellation handling by design — the presenting op is the terminal
/// pipeline node, so its alert `await` is never the target of an engine abort
/// (see `ContinuationGate` docs).
@Suite struct ContinuationGateTests {
    @Test func resumeWhileWaitingDeliversValue() async {
        let gate = ContinuationGate<Int>()
        let waiter = Task { await gate.value() }
        // Let the waiter suspend, then resume it.
        _ = await poll { await gate.isWaiting }
        await gate.resume(with: 42)
        #expect(await waiter.value == 42)
    }

    @Test func secondResumeAfterDeliveryIsNoOp() async {
        let gate = ContinuationGate<Int>()
        let waiter = Task { await gate.value() }
        _ = await poll { await gate.isWaiting }
        await gate.resume(with: 1)
        await gate.resume(with: 2) // continuation already gone → safe no-op
        #expect(await waiter.value == 1)
    }
}
