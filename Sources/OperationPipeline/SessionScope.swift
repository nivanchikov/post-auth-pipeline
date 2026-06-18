import Foundation

/// Owns ALL per-session and per-run mutable state: the published `live` map
/// (cross-pipeline, shared for the session), the current run's `working` map,
/// and the dependency-gate continuations. The engine is stateless across
/// sessions; everything that could leak across a session boundary lives here,
/// so dropping the scope drops the state (see write-boundary invariant).
public actor SessionScope {
    private var live: [ObjectIdentifier: AnyOutcomeBox] = [:]
    private var working: [ObjectIdentifier: AnyOutcomeBox] = [:]
    private var waiters: [ObjectIdentifier: [CheckedContinuation<AnyOutcomeBox, Never>]] = [:]
    private var aborted = false

    public init() {}

    /// Reset per-run state. `live` is preserved (it's session-scoped).
    public func beginRun() {
        working = [:]
        // Resume any stragglers (defensive; a clean run leaves none).
        flushWaiters(with: AnyOutcomeBox(Outcome<Int>.skipped(reason: "run reset")))
        aborted = false
    }

    /// Record a node's outcome and wake anything gated on it.
    public func complete(_ id: ObjectIdentifier, _ outcome: AnyOutcomeBox) {
        working[id] = outcome
        if let cs = waiters[id] {
            waiters[id] = nil
            for c in cs { c.resume(returning: outcome) }
        }
    }

    /// Suspend until `id` completes in THIS run. Dependencies are always within
    /// the same pipeline (cross-pipeline edges are rejected at validation), so
    /// every awaited id eventually completes — unless aborted, which resumes
    /// all waiters so their tasks can unwind.
    ///
    /// IMPORTANT: if the scope is aborted or reset (`abort()`/`beginRun()`) while a
    /// caller is suspended here, the returned box is a SENTINEL whose concrete value
    /// type is unspecified (`Outcome<Int>`). Callers MUST treat the returned box as a
    /// completion *signal* only — check `isAborted` and never call `unwrap(as:)` on it.
    public func awaitOutcome(of id: ObjectIdentifier) async -> AnyOutcomeBox {
        if let o = working[id] { return o }
        // The continuation closure runs synchronously on the actor BEFORE suspension,
        // so (read working) → (append waiter) is one uninterrupted actor step — no
        // interleaving with complete(), hence no stranded-continuation deadlock.
        return await withCheckedContinuation { c in
            waiters[id, default: []].append(c)
        }
    }

    /// The context base for a node about to run: live overlaid with this run's
    /// working entries (working wins). Built once per node at run start.
    public func snapshot() -> [ObjectIdentifier: AnyOutcomeBox] {
        live.merging(working) { _, new in new }
    }

    public func liveSnapshot() -> [ObjectIdentifier: AnyOutcomeBox] { live }

    /// Atomically publish this run's results into the live (cross-pipeline) map.
    public func publish() {
        live.merge(working) { _, new in new }
        working = [:]
    }

    public var isAborted: Bool { aborted }

    /// Mark the run aborted and free any suspended gate waiters so their tasks
    /// can observe cancellation and exit. The sentinel is never stored or read.
    public func abort() {
        aborted = true
        flushWaiters(with: AnyOutcomeBox(Outcome<Int>.skipped(reason: "aborted")))
    }

    /// Resume every suspended waiter with a wake signal. The sentinel's concrete
    /// `Outcome<Int>` type (from `abort()`/`beginRun()`) is deliberate-and-irrelevant:
    /// it is only ever used to unblock the suspended task, never unwrapped, so the
    /// value type cannot cause an `unwrap(as:)` type-mismatch trap downstream.
    private func flushWaiters(with sentinel: AnyOutcomeBox) {
        let pending = waiters
        waiters = [:]
        for (_, cs) in pending { for c in cs { c.resume(returning: sentinel) } }
    }
}
