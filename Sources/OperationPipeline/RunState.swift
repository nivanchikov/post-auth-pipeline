import Foundation

/// Owns accumulated outcomes plus dependency-gate continuations for one engine run.
actor RunState {
    private var outcomes: [ObjectIdentifier: AnyOutcomeBox] = [:]
    private var waiters: [ObjectIdentifier: [CheckedContinuation<AnyOutcomeBox, Never>]] = [:]
    private var aborted = false

    init() {}

    /// Clear all run outcomes and wake suspended dependency gates.
    func reset() {
        outcomes = [:]
        aborted = false
        flushWaiters(with: AnyOutcomeBox(Outcome<Int>.skipped(reason: "reset")))
    }

    /// Record a node's outcome and wake anything gated on it.
    func complete(_ id: ObjectIdentifier, _ outcome: AnyOutcomeBox) {
        outcomes[id] = outcome
        if let continuations = waiters[id] {
            waiters[id] = nil
            for continuation in continuations {
                continuation.resume(returning: outcome)
            }
        }
    }

    /// Suspend until `id` has completed in this session. Dependencies are
    /// expected to point at nodes in the currently running pipeline; shared
    /// completed run outcomes are available to operation code through `snapshot()`.
    ///
    /// IMPORTANT: if the scope is aborted or reset while a
    /// caller is suspended here, the returned box is a SENTINEL whose concrete value
    /// type is unspecified (`Outcome<Int>`). Callers MUST treat the returned box as a
    /// completion *signal* only — check `isAborted` and never call `unwrap(as:)` on it.
    func awaitOutcome(of id: ObjectIdentifier) async -> AnyOutcomeBox {
        if let outcome = outcomes[id] {
            return outcome
        }
        // The continuation closure runs synchronously on the actor BEFORE suspension,
        // so (read outcomes) → (append waiter) is one uninterrupted actor step — no
        // interleaving with complete(), hence no stranded-continuation deadlock.
        return await withCheckedContinuation { continuation in
            waiters[id, default: []].append(continuation)
        }
    }

    /// The context base for a node about to run.
    func snapshot() -> [ObjectIdentifier: AnyOutcomeBox] {
        outcomes
    }

    var isAborted: Bool {
        aborted
    }

    /// Mark the run aborted and free any suspended gate waiters so their tasks
    /// can observe cancellation and exit. The sentinel is never stored or read.
    func abort() {
        aborted = true
        flushWaiters(with: AnyOutcomeBox(Outcome<Int>.skipped(reason: "aborted")))
    }

    /// Resume every suspended waiter with a wake signal. The sentinel's concrete
    /// `Outcome<Int>` type (from `abort()`/`reset()`) is deliberate-and-irrelevant:
    /// it is only ever used to unblock the suspended task, never unwrapped, so the
    /// value type cannot cause an `unwrap(as:)` type-mismatch trap downstream.
    private func flushWaiters(with sentinel: AnyOutcomeBox) {
        let pending = waiters
        waiters = [:]
        for (_, continuations) in pending {
            for continuation in continuations {
                continuation.resume(returning: sentinel)
            }
        }
    }
}
