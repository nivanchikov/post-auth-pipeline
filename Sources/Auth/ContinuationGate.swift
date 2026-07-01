/// One-shot handoff between an async operation and a human tap. The operation
/// awaits `value()`; the UI calls `resume(with:)` from the alert button action.
/// This is the ONLY continuation code in the alert path — every other layer
/// stays declarative.
///
/// No cancellation handling — and this rests on a pipeline invariant, not on
/// wishful thinking. The engine's only cancel source is an abort: a fail-close
/// op throws, the engine calls `cancelAll()`, and every node task is cancelled.
/// But the presenting op is the TERMINAL node — it depends (transitively) on
/// every fail-close op, so its alert `await` is reachable only after all of them
/// have already succeeded. By then nothing is left running to throw, so the
/// abort-cancel and the alert-await are mutually exclusive in time: the alert
/// suspension can never be the thing `cancelAll()` cancels.
///
/// Preserve that invariant. If the presenting op ever stops depending on some
/// fail-close op (a sibling that can throw WHILE the alert is up), or login
/// becomes abortable (Cancel button, login timeout, cancellable login task),
/// restore the `withTaskCancellationHandler` + settled-latch path — otherwise a
/// cancelled operation would leak here, hanging the run on a human tap that no
/// longer matters.
public actor ContinuationGate<Value: Sendable> {
    private var continuation: CheckedContinuation<Value, Never>?

    public init() {}

    /// True while a caller is suspended in `value()`. Test aid: lets tests wait
    /// for suspension deterministically instead of racing a fixed sleep.
    var isWaiting: Bool {
        continuation != nil
    }

    /// Suspend until the gate is resumed.
    public func value() async -> Value {
        await withCheckedContinuation { continuation = $0 }
    }

    /// Deliver `value` to the waiter. The continuation is dropped after the
    /// first resume, so a stray second call (e.g. a double tap) is a safe no-op.
    public func resume(with value: Value) {
        continuation?.resume(returning: value)
        continuation = nil
    }
}
