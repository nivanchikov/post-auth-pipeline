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
- **Swift 6 / strict concurrency.** `Sendable` throughout, structured concurrency for
  parallelism and cancellation. Pipeline aborts surface a concrete `PipelineFailure`.

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
- Stateless across sessions; per-run state lives in internal `RunState` (see Context Lifecycle).

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
    case failed(any Error)
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
let pipeline = try buildPipeline([
    EntitlementsOp(client: entitlementsClient),
    PaywallOp(),
    // ...
])
let engine = OperationEngine()
```

Validation is **per-pipeline** and runs at graph-build time (once, at startup). For each
pipeline the engine checks: every `dependsOn` type resolves to an instance **registered in that
same pipeline**, and the pipeline's graph is acyclic. Three failures are **fail-fast startup
errors** (not compile errors, not silent no-ops):

- a `dependsOn` referencing an **unregistered** op type,
- a **cycle**, and
- a **cross-pipeline edge** — a gating op depending on a post-login op or vice versa. The two
  pipelines have separate graphs; an edge across them is almost certainly a wiring mistake and
  is rejected.

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

### Scheduling: structured children + engine-fulfilled dependency gates

A plain `TaskGroup` only models "spawn N children, await all" — it has **no** notion of "B
starts after A." But the obvious fix (hold a `Task` handle per node and `await` upstream
handles before running) is also wrong: a `Task { … }` created inside the group is
**unstructured**, so cancelling the group does *not* cancel it. Per-node await-gates and
structured-child cancellation pull in opposite directions with the raw API. The resolution
keeps both properties:

- **Every node is a structured child** spawned via `group.addTask` — so cancelling the scope
  (on logout/abort) cancels every node, and the group naturally awaits them all.
- **Dependency gating is done by awaiting an engine-fulfilled signal,** not by holding child
  handles. Before running its body, a node calls `await scope.awaitOutcome(of: upstream)` for
  each dependency. That call suspends on a per-node continuation the engine resolves the moment
  the upstream node completes (the upstream's child, on finishing, both stores its outcome and
  fulfills any waiters). Acyclicity (validated at graph-build) guarantees these awaits cannot
  form a wait-cycle, so the schedule is deadlock-free.

```swift
await withTaskGroup(of: Void.self) { group in
    for node in nodes {                          // order irrelevant; gates enforce edges
        group.addTask {                          // structured child → cancellable by the scope
            for dep in node.dependsOn {
                _ = await scope.awaitOutcome(of: dep)   // suspends until upstream completes
            }
            let outcome = await run(node, scope)        // run, then:
            await scope.complete(node, outcome)         // store + fulfill this node's waiters
        }
    }
}
```

This gives **true maximal parallelism** (a node starts the instant *its own* upstreams finish,
not when a whole "level" finishes) and **structured cancellation** (every node is a real group
child). The per-node continuations live in the `RunState` actor, so completion and
waiter-fulfilment are race-free. (A simpler "wave scheduling" — topo-sort into levels, run each
level in a sub-group — is the recorded alternative; it's easier but serializes across levels and
loses cross-level overlap.)

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
does not carry a separate overall deadline in this version.

Timeouts are implemented with a structured task-group race between the operation and a timer.
That keeps cancellation structured and simple, but it is cooperative: Swift has no forcible
task termination, and a non-cooperative operation that ignores cancellation can still delay the
group from returning. Operation authors should make network calls and waits cancellation-aware.

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

**Session outcome map.** Each node writes its outcome exactly once, on completion, into the
run context. A diamond reader (D reading B and C) still gets consistent values because the
await-gates make D wait for B and C before it runs. Gating and post-login do not share pipeline
context; if they need common domain data, that data should live in injected collaborators or
stores.

Pipeline re-runs are deliberately not modeled. If a host needs foreground refreshes or repeated
pipelines later, that should be added as a separate, explicit requirement instead of carrying
staging/publish machinery in the first version.

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

**The context is scoped to one engine run.**

- A gating pipeline run gets its own internal `RunState`.
- A post-login pipeline run gets a separate internal `RunState`.
- The auth layer does not broker or retain scopes.
- Re-login naturally starts fresh because each `OperationEngine.run(_:)` creates fresh run state.

There is no cross-session pipeline data to clear. Operations that need durable domain data should
write to injected collaborators/stores rather than relying on pipeline context.

### Re-runs within a session

Re-runs are out of scope for this version. The session context is an accumulated outcome map,
not a transactional re-run store. If repeated post-login refresh becomes a concrete need, add it
with explicit semantics then.

## Concurrency, Cancellation, Errors

- **Engine is stateless.** Each pipeline run uses a structured task scope with per-node
  await-gates (see Scheduling) so independent DAG nodes execute concurrently while edges are
  respected. Everything is `Sendable` / Swift 6 strict-concurrency clean. Per-run transient
  state is owned by `RunState`, not stored on `OperationEngine`, so overlapping runs never
  cross wires.
- **Cancellation:** logout / abort / forced re-auth cancels the in-flight post-login run;
  in-flight operations observe `Task` cancellation cooperatively, with a per-op timeout as the
  backstop for ops that never observe cancellation. Re-login starts a fresh run. A
  late-completing op writes only into its own now-discarded session context, never the new one
  (see Write-boundary invariant).
- **Error surfacing (DECIDED):** the engine reports a `PipelineFailure` with the first
  fail-close operation's type name and a diagnostic message. The auth module wraps this in its
  own `AuthError` type, exposed on `AuthState`, so the login screen can show a tailored message
  and offer retry. The engine does not know about UI; the auth module owns the mapping.

### Gating return contract

`GatingRunner.run()` returns `.success` or `.aborted(PipelineFailure)`. The auth module flips to
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
- **No transactional re-runs.** Re-triggering a pipeline in the same session is not a first-class
  requirement. Add explicit rerun semantics only when a concrete workflow needs them.

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
| DAG scheduling | Structured `addTask` children + engine-fulfilled dependency gates (max parallelism, cancellable) | Wave scheduling (level-by-level); unstructured `Task` handles (loses cancellation) |
| Re-run consistency | Not modeled in v1 | Staging map + atomic publish→live swap |
| Wiring validation | Host registers instances; presence + acyclicity checked fail-fast at graph-build | Compile-time only (not achievable); silent skip |
| Concurrency | Stateless engine + structured task scope + internal `RunState` actor | Sequential within run |
| Hung ops | Per-operation cooperative timeout, applied via `failurePolicy` | No timeout; unstructured abandoned tasks |
| Error surfacing | Auth module wraps engine error in `AuthError` on state | Generic failure, details logged only |
```
