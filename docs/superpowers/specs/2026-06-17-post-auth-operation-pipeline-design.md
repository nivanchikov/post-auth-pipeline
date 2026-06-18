# Post-Auth Operation Pipeline — Design

**Date:** 2026-06-17
**Status:** Approved design, pending implementation plan

## Problem

The auth module (a self-contained Swift package) presents a login screen on `start()`,
re-presents it on `logout()`, exposes auth state via an `AsyncStream<AuthState>`, and logs
in by calling the backend. We need to extend it so that:

1. **Gating operations (N):** after the backend auth call, a set of async operations (e.g.
   entitlements fetch) must run **before** the module signals "fully authenticated" and
   dismisses the login screen.
2. **Post-login operations (X):** side-effecting actions (register for push, present banners,
   etc.) must run **after** authentication, but must **not** block the auth state change or
   the dismissal of the login screen.
3. Operations have varied behavior:
   - **Fail-open vs fail-close:** some operations failing should abort login; others are
     tolerable and login proceeds without them.
   - **Guards:** an operation may have logic that decides whether it should run at all.
   - **Dependencies & ordering:** an operation may depend on others, must run with a
     priority relative to a group, and an operation's **outcome** may block a dependent from
     running even when the upstream operation did not fail.

**Hard constraint:** none of this is baked into the auth module. The auth module has **zero
knowledge** of the concrete operations. It knows only a single opaque injected dependency it
can call to run the gating work.

## Goals & Principles

- **Auth module stays ignorant.** It depends only on a tiny protocol; it never references a
  concrete operation.
- **One authoring model, two pipelines.** Gating and post-login share the same engine and the
  same way of writing an operation. The only difference is who triggers them and whether they
  block login.
- **Ergonomic and extensible.** Adding a new operation = writing one type, registering an
  instance, and declaring its dependencies/policy. No engine changes, no auth-module changes.
- **Type-checked references, fail-fast wiring.** `dependsOn` references operation *types*
  (`SomeOp.self`), so a reference to a non-existent/non-conforming type is a compile error and
  there are no stringly-typed identifiers. **However, presence of the referenced node in the
  pipeline is not a compile-time guarantee** — see "Registration & graph validation." A
  `dependsOn` pointing at an op type that was never registered is caught at graph-build time
  (fail fast at startup), not by the compiler.
- **Swift 6 / strict concurrency.** Actor-isolated engine, `Sendable` throughout, structured
  concurrency for parallelism and cancellation. Errors are carried as `any Error & Sendable`
  (bare `any Error` is not `Sendable`).

## Architecture

Three players. The auth module is deliberately ignorant of operations.

```
┌─────────────────┐   awaits    ┌──────────────────────────┐
│   Auth Module   │────────────▶│  GatingRunner (protocol) │  ← injected, opaque
│  start()/logout │             └──────────────────────────┘
│  AsyncStream<   │                         │ implemented by
│    AuthState>   │                         ▼
└────────┬────────┘             ┌──────────────────────────┐
         │ publishes state      │   OperationEngine        │  ← shared DAG executor
         │                      │   (actor)                │
         ▼                      └──────────────────────────┘
┌──────────────────────────┐                ▲
│ PostLoginCoordinator     │────────────────┘ runs the post-login pipeline
│ observes AsyncStream,    │
│ runs on .authenticated,  │
│ cancels on logout        │
└──────────────────────────┘
```

### Auth module

- Holds one injected dependency, `GatingRunner`, which it `await`s during login.
- Flips to `.authenticated` only when gating reports success; otherwise stays unauthenticated
  with a wrapped error (see Error Surfacing).
- Publishes `AuthState` via the `AsyncStream` it already exposes. It does **not** know the
  post-login pipeline exists.

### OperationEngine

- The shared DAG executor. Used to build **two pipelines** from two different operation sets:
  the gating pipeline and the post-login pipeline. Same engine, same authoring model.
- Stateless across sessions; per-run state lives in a `SessionScope` (see Context Lifecycle).

### PostLoginCoordinator

- Host-side. Subscribes to the auth module's `AsyncStream<AuthState>`.
- On transition **into** `.authenticated`: runs the post-login pipeline.
- On transition **into** `.unauthenticated` (logout, abort, forced re-auth): cancels the
  in-flight post-login run.
- This is why post-login needs no hook inside the auth module: every path that authenticates
  (interactive login, cold start with a saved session, re-auth) flows through the same stream.

**Why a stream, not a callback or explicit call:** the auth module already publishes state, so
observation requires no new callout and no per-entry-point wiring. A lifecycle delegate would
force the auth module to "call out" and remember to fire on every authenticating path
(including cold start); an explicit call from the composition root would have to be repeated at
every such entry point. Both are recorded as rejected alternatives.

## Operation Contract

Identity is the operation's **own type** — both the node identity and the typed-value key.
There is no minted operation id and no separate key type.

```swift
enum Outcome<Value: Sendable>: Sendable {
    case success(Value)
    case skipped(reason: String)
    case failed(any Error & Sendable)   // bare `any Error` is NOT Sendable
}

enum FailurePolicy: Sendable {
    case open   // failure is tolerable; login proceeds without this op
    case close  // failure aborts login
}

protocol Operation: Sendable {
    associatedtype Value: Sendable

    var dependsOn: [any Operation.Type] { get }   // e.g. [EntitlementsOp.self] — compiler-checked
    var failurePolicy: FailurePolicy { get }

    func shouldRun(_ ctx: RunContext) async -> Bool         // guard + outcome-based blocking
    func run(_ ctx: RunContext) async throws -> Value
}

protocol RunContext: Sendable {
    // Read a dependency's value — typed at the call site, recovered from the op type.
    func value<O: Operation>(of type: O.Type) -> O.Value?
    func outcome<O: Operation>(of type: O.Type) -> Outcome<O.Value>?
}
```

Identity internally is `ObjectIdentifier(type)`; topological sort and cycle detection key on
that.

### Type erasure at the storage boundary (the cast the API hides)

The *authoring surface* is cast-free: a call site `ctx.value(of: EntitlementsOp.self)` binds
`O = EntitlementsOp`, so `O.Value` is concrete and the return type recovers correctly with no
caller cast. But the engine must store outcomes of **different value types in one map**
(`ObjectIdentifier → erased outcome`), and `Outcome<Value>` is invariant — an `Outcome<Int>`
is not an `Outcome<any Sendable>`. So exactly one `as?` cast is unavoidable *inside* the
accessor:

```swift
storage[ObjectIdentifier(type)]?.unwrap(as: O.self)   // centralized, safe by identity
```

The cast is safe (the type identity is the key), but it exists. It is centralized in a small
type-erased box (`AnyOutcomeBox` wrapping a concrete `Outcome<V>` with an
`unwrap<O>() -> Outcome<O.Value>?`). On a key-present-but-type-mismatch (only reachable via a
wiring bug) the box **traps** rather than returning `nil`, so misconfiguration surfaces loudly
instead of silently reading as absent.

### Registration & graph validation

`any Operation.Type` (as used in `dependsOn`) is an existential metatype usable **only for
identity** — you cannot instantiate it or call `run`/`shouldRun`/read `failurePolicy` through
it. The *executable* node (the configured instance with its injected collaborators) is
therefore registered separately by the host:

```swift
let engine = OperationEngine(operations: [
    EntitlementsOp(client: entitlementsClient),
    PaywallOp(),
    // ...
])
```

At graph-build time (once, at startup) the engine validates: every `dependsOn` type resolves
to a **registered** instance, and the graph is acyclic. A `dependsOn` referencing an
unregistered op type, or a cycle, is a **fail-fast startup error** — not a compile error and
not a silent no-op.

### Authoring example

```swift
struct PaywallOp: Operation {
    var dependsOn: [any Operation.Type] { [EntitlementsOp.self] }
    var failurePolicy: FailurePolicy { .open }

    func shouldRun(_ ctx: RunContext) async -> Bool {
        // Outcome-based blocking, fully typed:
        guard let ent = ctx.value(of: EntitlementsOp.self) else { return false }
        return ent.showPaywall
    }

    func run(_ ctx: RunContext) async throws -> PaywallConfig { /* ... */ }
}
```

An operation receives its own collaborators (network clients, services) via its initializer —
the host constructs the operation and supplies dependencies. The engine injects nothing but
the `RunContext`.

### Caveat: one instance per type per pipeline

Type-as-identity means a given operation type can appear **once** per pipeline; you cannot
register two configured instances of the same type as two distinct nodes. This matches the
post-auth domain (each operation is a singleton: "the entitlements op," "the push op"). If a
future need genuinely requires N parametrized copies of one type, that specific case would
fall back to an explicit identifier. Accepted trade-off.

## Execution Semantics

The engine runs each pipeline as a DAG. Independent nodes run **concurrently**; edges are
respected.

### Scheduling: per-node await-gates (not a bare TaskGroup)

A plain `TaskGroup` only models "spawn N children, await all" — it has **no** notion of "B
starts after A." So the engine does **not** rely on a bare group for ordering. Instead, each
node is spawned as a child task that, before running its body, `await`s the completion of its
dependencies' tasks:

```swift
await withTaskGroup(of: Void.self) { group in
    var handles: [ObjectIdentifier: Task<AnyOutcomeBox, Never>] = [:]
    for node in topologicallyOrdered {
        let deps = node.dependsOn.map { handles[ObjectIdentifier($0)]! }
        handles[node.id] = Task {
            for d in deps { _ = await d.value }   // await-gate on upstreams
            return await run(node, ctx)
        }
    }
    // group/structured scope awaits all node tasks; cancellation propagates to every child
}
```

This gives **true maximal parallelism** (a node starts the instant *its* upstreams finish, not
when a whole "level" finishes) and makes cancellation free: cancelling the scope cancels every
child task. (A simpler "wave scheduling" — topo-sort into levels, run each level in a group —
is the recorded alternative; it's easier but serializes across levels and loses cross-level
overlap.)

Per-operation lifecycle:

1. **Wait** until all `dependsOn` operations reach a terminal outcome (success / skipped /
   failed).
2. **Default cascade:** if any dependency is non-success, the operation defaults to
   `.skipped(reason:)` **unless** its guard overrides (see Cascade Rules).
3. **Guard:** evaluate `shouldRun(ctx)`. Returning `false` ⇒ `.skipped`. This is where both
   plain guards ("user opted out") and outcome-based blocking ("A's value says don't run B")
   live, because the guard can read upstream outcomes/values from the context.
4. **Run:** execute `run(ctx)`. Success stores the typed value in the context; a throw ⇒
   `.failed`.
5. **Fail policy:** a `.close` failure aborts the run and reports a structured error; a
   `.open` failure prunes the failed operation's now-unsatisfied subtree (dependents default
   to skipped per the cascade rule) and the run continues.

**Per-operation timeout (gating is on the critical path).** Because gating blocks the login
screen, a hung operation would hang login indefinitely. Each operation may declare a deadline;
on expiry the engine treats it as a failure subject to the op's own `failurePolicy` (a
fail-open op times out and is pruned; a fail-close op times out and aborts login). A pipeline
may also carry an overall deadline. This is the only protection against an op that never
returns and never observes cancellation.

### Cascade Rules (DECIDED: default-skip + guard override)

When operation A is non-success (failed-open or skipped) and B depends on A:

- **Chosen:** B **defaults to `.skipped`**. B's author can override by writing a `shouldRun`
  that inspects A's outcome via the context and returns `true`. Safe default (no operation
  runs on missing prerequisites by accident), with a full escape hatch.

**Diamond worked example (A → {B, C} → D).** Suppose B is fail-open and *fails*, C *succeeds*.
D depends on both. Per the wait rule, D waits for B and C to be terminal; per the cascade rule,
D defaults to `.skipped` because B (a dependency) is non-success — **even though C succeeded.**
If D's author only truly needs C, they override via `shouldRun` returning `true`. **Overriding
is an explicit acceptance of responsibility:** once D runs, `ctx.value(of: B.self)` returns
`nil`, and D must handle the partial-prerequisite case itself. `outcome(of: B.self)` is
available to distinguish *failed* from *skipped*. This is the cost of the global default-skip
rule; the "required vs optional edges" alternative below makes this declarative instead, at the
price of more per-edge authoring.

**Override re-arms fail-close.** Fail policy is evaluated on an operation's *own* run,
independent of why its prerequisites were missing. So if B is `.open` and fails, and D
overrides the skip, runs without B's value, and itself throws as a `.close` op — D's failure
*aborts login*. A tolerable upstream failure can thus be escalated into a login-aborting one
through an override. Intended, but sharp; authors overriding into a fail-close op must mean it.

Open alternatives (recorded, not chosen):

- **Always-run-guard:** engine never auto-skips; the guard always runs and is solely
  responsible, even when a dependency failed. More explicit, more boilerplate per op.
- **Required vs optional edges:** each edge tagged required (non-success of parent auto-skips
  child) or optional (child runs regardless). More expressive edges, more to author.

## Data Flow & Context

Data passing is **typed-value-via-context (DECIDED):** the engine itself carries values
between operations. When an operation finishes, its outcome is written into a shared context
keyed by operation type. Any number of dependents read the same entry — fan-out (A → {B, C})
and diamonds (A → B, A → C, B&C → D) need nothing special for *reads*; "carrying" is a shared
read, not a hand-off. (Note: this addresses read fan-out only — the *cascade* semantics for
diamonds are covered separately under Cascade Rules.)

**Within a single run, the context is write-once per operation** (each node writes its outcome
exactly once, on completion). **Across re-runs of a pipeline it is not append-only** — a
re-run overwrites entries (see Re-runs). To avoid torn reads during a re-run, a re-run's
results are applied as an **atomic end-of-run snapshot swap**: readers always see either the
fully-previous or fully-current set for that pipeline, never a mix of old and new values across
a diamond. The earlier framing of the context as "append-only" applies only *within* one run.

- A failed-open operation leaves a `.failed` entry (not absent); a guarded-out operation
  leaves `.skipped`. So `value(of:)` returns `nil` while `outcome(of:)` still distinguishes
  *failed* from *intentionally skipped* for a dependent that wants to react.
- No external store is required for one operation to consume another's result. A host store is
  needed only if something **outside** the pipeline (a view, a long-lived service) also wants
  that data — the host's choice, not forced by the engine.

Open alternative (recorded, not chosen):

- **Outcome-status-only:** engine tracks only ran/skipped/failed; payloads are shared through
  the host's own services/stores. Smaller engine, but shared state lives outside it.

## Context Lifecycle

**The context is scoped to one auth session, shared across both pipelines.**

- **Born** when login begins; the gating run creates the `SessionScope` that owns the context.
- **Accumulates** as operations complete (both pipelines write into the same context).
- **Shared across pipelines:** the same context gating populated is handed to the post-login
  run, so a post-login operation can read a value a gating operation produced (e.g. a banner
  op reading `Entitlements`) without refetching or a store.
- **Cleared** deterministically at session end. The `SessionScope` is dropped and every cached
  value with it. There is no manual reset.

Session-ending events, all treated identically (DECIDED — forced re-auth treated like logout):

- **Logout** (`logout()` → `.unauthenticated`).
- **Fail-close abort** during gating (login never completes).
- **Forced re-auth / token expiry** — even if the same user re-authenticates silently, the
  context is dropped and a fresh one is minted. Preserving across silent refresh is explicitly
  **not** done.

**Re-login mints a brand-new empty context.** This is a correctness/security boundary: a new
session must never read the prior session's values (no cross-user data leakage).

### Write-boundary invariant (prevents cross-session leakage under cancellation)

The clear-vs-write race is the place the no-leak guarantee can quietly fail: a post-login
operation that is mid-`run()` past a logout could, if it resolves later, write its outcome into
the *new* session's context. The invariant that forbids this:

- An operation's `RunContext` is **bound at task spawn** and threaded explicitly into
  `run(ctx)`/`shouldRun(ctx)`. There is **no global "current context" accessor** an op can
  reach through to find "the live session."
- The engine **never mutates a context after that context's session terminal event.** A late
  in-flight op holds a reference to the *old* `SessionScope` and writes harmlessly into the
  now-discarded context; it can never reach session N+1's context.
- Transient per-run state (the node task handles, the in-flight context) is **owned by the
  `SessionScope`, not stored as actor properties.** So a fast logout→re-login that briefly
  overlaps a cancelled-old run and a fresh-new run cannot cross wires — each run's state lives
  in its own scope. ("Stateless across sessions" means exactly this: the actor holds no
  session-spanning mutable state.)

This invariant is explicitly tested (see Testing Strategy: late-completing op must not touch
the new session's context).

### Re-runs within a session

- Re-triggering the post-login pipeline in the same session (e.g. app returns to foreground)
  re-executes that pipeline's operations and **overwrites** their entries via the atomic
  end-of-run snapshot swap (see Data Flow). Latest fully-completed run wins; no torn reads.
  Cross-pipeline reads then see the freshest value.
- Only the pipeline actually run executes; a post-login re-trigger does not re-run gating
  operations.

## Concurrency, Cancellation, Errors

- **Engine is an `actor`.** Each pipeline run uses a structured task scope with per-node
  await-gates (see Scheduling) so independent DAG nodes execute concurrently while edges are
  respected. Everything is `Sendable` / Swift 6 strict-concurrency clean (errors carried as
  `any Error & Sendable`). Per-run transient state is owned by the `SessionScope`, not stored
  as actor properties, so an overlapping cancelled-old / fresh-new run pair never crosses wires.
- **Cancellation:** logout / abort / forced re-auth cancels the in-flight post-login run;
  in-flight operations observe `Task` cancellation cooperatively, with a per-op timeout as the
  backstop for ops that never observe cancellation. Re-login starts a fresh run. A
  late-completing op writes only into its own now-discarded session context, never the new one
  (see Write-boundary invariant).
- **Error surfacing (DECIDED):** the engine reports a structured failure (the first
  fail-close operation's type + its underlying `any Error & Sendable`). The **auth module catches this and
  wraps it in its own `AuthError` type**, exposed on `AuthState`, so the login screen can show
  a tailored message and offer retry. The engine does not know about UI; the auth module owns
  the mapping.

### Gating return contract

`GatingRunner.run()` returns `.success` or `.aborted(underlying)`. The auth module flips to
`.authenticated` only on `.success`; otherwise it stays unauthenticated and surfaces a wrapped
`AuthError`. Post-login never blocks anything: it is launched by the coordinator and its
failures are fail-open by nature (logged, never affecting auth state).

## Testing Strategy

- **Engine (isolation):** synthetic operations exercise dependency ordering, parallelism,
  guard skip, outcome-based blocking, default-skip cascade (incl. the diamond example with a
  guard override reading a `nil` upstream value), fail-open subtree pruning, fail-close abort,
  per-op timeout (fail-open vs fail-close on expiry), and cancellation. No auth or UI needed.
- **Graph validation:** an unregistered `dependsOn` target and a cycle each fail fast at
  graph-build, with a clear diagnostic; a type-mismatched context read traps.
- **Auth module:** a stub `GatingRunner` (success / abort) verifies state transitions and
  `AuthError` wrapping without any real operations.
- **PostLoginCoordinator:** a fake `AsyncStream<AuthState>` drives login → logout → re-login
  to verify the pipeline runs on authenticate, cancels on logout, and re-runs fresh on
  re-login.
- **Context lifecycle:** assert the context is cleared on each session-ending event and that a
  re-login sees an empty context (no prior-session values).
- **Write-boundary (security):** an operation still in-flight when logout fires, completing
  *after* the session ended, must not write into the new session's context — assert the
  next session's context never observes the late op's value.
- **Re-run atomicity:** during a pipeline re-run, a reader observing a diamond never sees a mix
  of previous-run and current-run values.

## Scope, Scale & Known Limitations

- **Expected scale:** a handful of operations per pipeline (~5–15), shallow depth. The
  full topological-sort + cycle-detection + parallel scheduler is deliberately more machinery
  than 5 nodes strictly need, but it is *simpler to author against* than priority tiers, the
  parallelism is real for independent gating fetches, and cycle detection is cheap correctness
  insurance. The design is **not** intended to scale to 100-node graphs; if that ever becomes a
  need, revisit the scheduler. Right-sizing this expectation is intentional, not an oversight.
- **One instance per type per pipeline** (type-as-identity). N parametrized copies of one op
  type are not expressible without falling back to an explicit id for that case.
- **`shouldRun` returns `Bool`,** which cannot distinguish "skip me, but let my dependents
  still evaluate against my own upstreams" from "skip me and cascade." Today a guarded-out op
  is `.skipped` and cascades per the default rule. This expressivity limit is accepted as
  YAGNI; revisit only if a concrete case needs the distinction.

## Decisions Summary

| Topic | Decision | Recorded alternatives |
|---|---|---|
| Phase model | Shared engine, two pipelines (gating + post-login) | Two explicit phases; three+ named stages |
| Post-login trigger | Coordinator observes `AsyncStream<AuthState>` | Injected lifecycle delegate; explicit call from composition root |
| Ordering model | Dependency graph (DAG) + parallelism | Priority tiers; tiers + edges |
| Operation identity | Type-as-identity (`SomeOp.self`) | Minted `OperationID` |
| Cascade default | Default-skip + guard override | Always-run-guard; required/optional edges |
| Data passing | Typed-value-via-context (engine carries data) | Outcome-status-only (host stores) |
| Context lifetime | Session-scoped, shared across pipelines, cleared at session end | Per-run context with explicit handoff |
| Forced re-auth | Treated like logout (drop context) | Preserve context on same-user silent refresh |
| DAG scheduling | Per-node await-gates over a structured group (max parallelism) | Wave scheduling (level-by-level); custom scheduler |
| Wiring validation | Host registers instances; presence + acyclicity checked fail-fast at graph-build | Compile-time only (not achievable); silent skip |
| Concurrency | Actor engine + structured task scope | Actor engine, sequential within run |
| Hung ops | Per-operation (and pipeline) timeout, applied via `failurePolicy` | No timeout (rejected — hangs login) |
| Error surfacing | Auth module wraps engine error in `AuthError` on state | Generic failure, details logged only |
```
