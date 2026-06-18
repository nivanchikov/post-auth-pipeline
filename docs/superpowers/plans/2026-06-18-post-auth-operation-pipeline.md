# Post-Auth Operation Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a pluggable, DAG-based post-authentication operation engine in a Swift 6 package, plus the auth-module integration that runs a gating pipeline before signalling "authenticated" and a post-login pipeline driven by observing auth state — with the auth module holding zero knowledge of concrete operations.

**Architecture:** Two SwiftPM library targets. `OperationPipeline` is the auth-agnostic engine: type-as-identity `Operation`s, a `SessionScope` actor that owns per-run state and dependency-gate continuations, and an `OperationEngine` actor that schedules each pipeline as structured `TaskGroup` children gated on upstream completion. `Auth` is the integration: `AuthState`/`AuthModule` (state machine + `AsyncStream`), a `GatingRunner` protocol the module awaits, an `EngineGatingRunner` adapter, a `SessionScopeBroker` that shares one scope between gating and post-login, and a `PostLoginCoordinator` that observes the stream.

**Tech Stack:** Swift 6 (strict concurrency), Swift Package Manager, `swift-testing` (`import Testing`), structured concurrency (`actor`, `withTaskGroup`, `CheckedContinuation`), `ObjectIdentifier` for type identity.

**Implementation order note (from the Codex review):** the scheduler (`SessionScope` gates + `OperationEngine` task-group) is the riskiest piece and is implemented first (Tasks 5–7), before cascade, fail-policy, timeout, validation, re-run, and the auth layer.

---

## File Structure

```
Package.swift
Sources/
  OperationPipeline/
    Outcome.swift            # Outcome<Value>, FailurePolicy, OutcomeKind, asSendable(_:)
    AnyOutcomeBox.swift      # type-erased Outcome (the centralized, trapping cast)
    Operation.swift          # Operation protocol + default shouldRun (encodes cascade) + default timeout
    RunContext.swift         # RunContext protocol + PipelineContext value type
    AnyOperation.swift       # type-erased Operation (run/shouldRun/skipped/failed factories)
    SessionScope.swift       # actor: live + working maps, gate waiters, complete/awaitOutcome/snapshot/publish/abort
    OperationEngine.swift    # actor: run(pipeline:scope:) scheduler, RunResult, timeout, fail-policy
    Pipeline.swift           # validated pipeline value + buildPipeline(...) validation (dup/unregistered/cycle)
  Auth/
    AuthState.swift          # AuthState enum, AuthError
    GatingRunner.swift       # GatingRunner protocol, GatingResult
    SessionScopeBroker.swift # actor sharing one SessionScope between gating + post-login
    EngineGatingRunner.swift # GatingRunner impl over OperationEngine + broker
    AuthModule.swift         # actor: login/logout/start, AuthState AsyncStream
    PostLoginCoordinator.swift # observes AsyncStream<AuthState>, runs/cancels post-login
Tests/
  OperationPipelineTests/
    AnyOutcomeBoxTests.swift
    SchedulerTests.swift
    CascadeTests.swift
    FailPolicyTests.swift
    TimeoutTests.swift
    ValidationTests.swift
    ReRunTests.swift
    TestSupport.swift        # synthetic operations + helpers shared by engine tests
  AuthTests/
    AuthModuleTests.swift
    PostLoginCoordinatorTests.swift
    WriteBoundaryTests.swift
```

---

### Task 1: Package scaffolding

**Files:**
- Create: `Package.swift`
- Create: `Sources/OperationPipeline/Placeholder.swift`
- Create: `Sources/Auth/Placeholder.swift`
- Create: `Tests/OperationPipelineTests/SmokeTests.swift`

- [ ] **Step 1: Write `Package.swift`**

```swift
// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "Auth",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "OperationPipeline", targets: ["OperationPipeline"]),
        .library(name: "Auth", targets: ["Auth"]),
    ],
    targets: [
        .target(
            name: "OperationPipeline",
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .target(
            name: "Auth",
            dependencies: ["OperationPipeline"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "OperationPipelineTests",
            dependencies: ["OperationPipeline"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "AuthTests",
            dependencies: ["Auth", "OperationPipeline"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
```

- [ ] **Step 2: Create placeholder sources so targets compile**

`Sources/OperationPipeline/Placeholder.swift`:
```swift
// Replaced in later tasks.
enum OperationPipelinePlaceholder {}
```

`Sources/Auth/Placeholder.swift`:
```swift
// Replaced in later tasks.
enum AuthPlaceholder {}
```

- [ ] **Step 3: Write a smoke test**

`Tests/OperationPipelineTests/SmokeTests.swift`:
```swift
import Testing
@testable import OperationPipeline

@Test func packageBuilds() {
    #expect(Bool(true))
}
```

- [ ] **Step 4: Run the build and test**

Run: `swift test`
Expected: build succeeds, 1 test passes.

- [ ] **Step 5: Commit**

```bash
git add Package.swift Sources Tests
git commit -m "chore: scaffold OperationPipeline + Auth Swift 6 package"
```

---

### Task 2: Core value types — Outcome, FailurePolicy, error helper

**Files:**
- Create: `Sources/OperationPipeline/Outcome.swift`
- Delete: `Sources/OperationPipeline/Placeholder.swift`
- Test: `Tests/OperationPipelineTests/OutcomeTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/OutcomeTests.swift`:
```swift
import Testing
@testable import OperationPipeline

struct PlainError: Error, Sendable, Equatable { let code: Int }
struct NonSendableError: Error { let closure: () -> Void }  // not Sendable

@Test func asSendablePassesThroughSendableError() {
    let e = PlainError(code: 7)
    let boxed = asSendable(e)
    #expect((boxed as? PlainError) == e)
}

@Test func asSendableWrapsNonSendableError() {
    let e = NonSendableError(closure: {})
    let boxed = asSendable(e)
    #expect(boxed is SendableErrorBox)
    #expect(boxed.localizedDescription.isEmpty == false)
}

@Test func failurePolicyHasOpenAndClose() {
    #expect(FailurePolicy.open != FailurePolicy.close)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter OutcomeTests`
Expected: FAIL — `asSendable`, `SendableErrorBox`, `FailurePolicy`, `Outcome` not defined.

- [ ] **Step 3: Write the implementation**

`Sources/OperationPipeline/Outcome.swift`:
```swift
import Foundation

/// The terminal result of running one operation.
public enum Outcome<Value: Sendable>: Sendable {
    case success(Value)
    case skipped(reason: String)
    case failed(any Error & Sendable)   // bare `any Error` is NOT Sendable
}

/// Whether an operation's failure aborts login or is tolerated.
public enum FailurePolicy: Sendable, Equatable {
    case open   // failure tolerated; the pipeline proceeds without this op
    case close  // failure aborts the pipeline (login)
}

/// Wraps a non-Sendable error so it can cross concurrency boundaries.
public struct SendableErrorBox: Error, Sendable {
    public let message: String
    public init(_ error: any Error) { self.message = String(describing: error) }
    public var localizedDescription: String { message }
}

/// Coerce any thrown error into `any Error & Sendable`, wrapping if necessary.
public func asSendable(_ error: any Error) -> any Error & Sendable {
    (error as? any Error & Sendable) ?? SendableErrorBox(error)
}
```

- [ ] **Step 4: Delete the placeholder**

```bash
git rm Sources/OperationPipeline/Placeholder.swift
```

- [ ] **Step 5: Run test to verify it passes**

Run: `swift test --filter OutcomeTests`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: add Outcome, FailurePolicy, and Sendable error helper"
```

---

### Task 3: AnyOutcomeBox — the centralized type-erased outcome

**Files:**
- Create: `Sources/OperationPipeline/AnyOutcomeBox.swift`
- Test: `Tests/OperationPipelineTests/AnyOutcomeBoxTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/AnyOutcomeBoxTests.swift`:
```swift
import Testing
@testable import OperationPipeline

@Test func boxRecoversSuccessValue() {
    let box = AnyOutcomeBox(Outcome<Int>.success(42))
    #expect(box.kind == .success)
    if case .success(let v)? = box.unwrap(as: Int.self) { #expect(v == 42) }
    else { Issue.record("expected success") }
}

@Test func boxReportsSkippedAndFailedKinds() {
    #expect(AnyOutcomeBox(Outcome<Int>.skipped(reason: "x")).kind == .skipped)
    let failed = AnyOutcomeBox(Outcome<Int>.failed(PlainError(code: 1)))
    #expect(failed.kind == .failed)
    #expect((failed.failureError as? PlainError) == PlainError(code: 1))
}

@Test func unwrapToCorrectTypeSucceeds() {
    let box = AnyOutcomeBox(Outcome<String>.success("hi"))
    #expect(box.unwrap(as: String.self) != nil)
}
```

> Note: the trap-on-type-mismatch path (`unwrap(as: Int.self)` on a `String` box) is intentionally NOT unit-tested, because it calls `preconditionFailure` which terminates the process. It is exercised only by code review and by the fact that a wiring bug would crash loudly. Do not add a test that triggers it.

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter AnyOutcomeBoxTests`
Expected: FAIL — `AnyOutcomeBox` not defined.

- [ ] **Step 3: Write the implementation**

`Sources/OperationPipeline/AnyOutcomeBox.swift`:
```swift
/// Type-erased `Outcome`. Stores a concrete `Outcome<V>` so heterogeneous
/// outcomes share one map. The single `as?` cast the API hides lives here.
public struct AnyOutcomeBox: Sendable {
    public enum Kind: Sendable, Equatable { case success, skipped, failed }

    public let kind: Kind
    /// The failure error, present only when `kind == .failed`.
    public let failureError: (any Error & Sendable)?
    private let storage: any Sendable   // holds the original Outcome<V>

    public init<V: Sendable>(_ outcome: Outcome<V>) {
        switch outcome {
        case .success: kind = .success;  failureError = nil
        case .skipped: kind = .skipped;  failureError = nil
        case .failed(let e): kind = .failed; failureError = e
        }
        storage = outcome
    }

    /// Recover the concrete outcome. Returns `nil` only when the key was absent
    /// (callers use optional chaining on the map). If the box is present but
    /// holds a different value type, that is a wiring bug and we trap.
    public func unwrap<Value: Sendable>(as type: Value.Type) -> Outcome<Value>? {
        guard let typed = storage as? Outcome<Value> else {
            preconditionFailure(
                "AnyOutcomeBox type mismatch: stored \(Swift.type(of: storage)), asked for Outcome<\(Value.self)>"
            )
        }
        return typed
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter AnyOutcomeBoxTests`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add AnyOutcomeBox type-erased outcome with trapping cast"
```

---

### Task 4: Operation & RunContext protocols + PipelineContext

**Files:**
- Create: `Sources/OperationPipeline/RunContext.swift`
- Create: `Sources/OperationPipeline/Operation.swift`
- Test: `Tests/OperationPipelineTests/ContextTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/ContextTests.swift`:
```swift
import Testing
@testable import OperationPipeline

private struct AOp: Operation {
    typealias Value = Int
    var dependsOn: [any Operation.Type] { [] }
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> Int { 1 }
}

private struct BOp: Operation {
    typealias Value = String
    var dependsOn: [any Operation.Type] { [AOp.self] }
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String { "b" }
}

@Test func contextReturnsTypedValueAndOutcome() {
    let ctx = PipelineContext(outcomes: [
        ObjectIdentifier(AOp.self): AnyOutcomeBox(Outcome<Int>.success(99)),
    ])
    #expect(ctx.value(of: AOp.self) == 99)
    #expect(ctx.succeeded(AOp.self) == true)
    if case .success(let v)? = ctx.outcome(of: AOp.self) { #expect(v == 99) }
    else { Issue.record("expected success outcome") }
}

@Test func contextReturnsNilForAbsentOrNonSuccess() {
    let ctx = PipelineContext(outcomes: [
        ObjectIdentifier(AOp.self): AnyOutcomeBox(Outcome<Int>.skipped(reason: "x")),
    ])
    #expect(ctx.value(of: AOp.self) == nil)        // skipped → no value
    #expect(ctx.succeeded(AOp.self) == false)
    #expect(ctx.value(of: BOp.self) == nil)        // absent key
    #expect(ctx.succeeded(BOp.self) == false)
}

@Test func defaultShouldRunRequiresAllDependenciesSucceeded() async {
    let b = BOp()
    let allGood = PipelineContext(outcomes: [
        ObjectIdentifier(AOp.self): AnyOutcomeBox(Outcome<Int>.success(1)),
    ])
    let depFailed = PipelineContext(outcomes: [
        ObjectIdentifier(AOp.self): AnyOutcomeBox(Outcome<Int>.failed(PlainError(code: 1))),
    ])
    #expect(await b.shouldRun(allGood) == true)
    #expect(await b.shouldRun(depFailed) == false)   // default-skip cascade
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter ContextTests`
Expected: FAIL — `Operation`, `RunContext`, `PipelineContext` not defined.

- [ ] **Step 3: Write `RunContext.swift`**

```swift
/// Read-only view of completed upstream outcomes, passed into each operation.
/// Immutable and `Sendable`: a snapshot taken when a node starts running.
public protocol RunContext: Sendable {
    /// The dependency's success value, or `nil` if absent / not a success.
    func value<O: Operation>(of type: O.Type) -> O.Value?
    /// The dependency's full outcome, or `nil` if absent.
    func outcome<O: Operation>(of type: O.Type) -> Outcome<O.Value>?
    /// Type-erased success check (used by the default cascade guard).
    func succeeded(_ type: any Operation.Type) -> Bool
}

/// Concrete `RunContext` backed by an immutable outcome map.
public struct PipelineContext: RunContext {
    private let outcomes: [ObjectIdentifier: AnyOutcomeBox]

    public init(outcomes: [ObjectIdentifier: AnyOutcomeBox]) {
        self.outcomes = outcomes
    }

    public func value<O: Operation>(of type: O.Type) -> O.Value? {
        guard case .success(let v)? = outcomes[ObjectIdentifier(type)]?.unwrap(as: O.Value.self)
        else { return nil }
        return v
    }

    public func outcome<O: Operation>(of type: O.Type) -> Outcome<O.Value>? {
        outcomes[ObjectIdentifier(type)]?.unwrap(as: O.Value.self)
    }

    public func succeeded(_ type: any Operation.Type) -> Bool {
        outcomes[ObjectIdentifier(type)]?.kind == .success
    }
}
```

- [ ] **Step 4: Write `Operation.swift`**

```swift
import Foundation

/// A unit of post-auth work. Its own type is its identity and its value key.
public protocol Operation: Sendable {
    associatedtype Value: Sendable

    /// Prerequisite operation types. e.g. `[EntitlementsOp.self]`.
    var dependsOn: [any Operation.Type] { get }
    /// Whether this op's failure aborts the pipeline.
    var failurePolicy: FailurePolicy { get }
    /// Optional per-op deadline. `nil` = no timeout.
    var timeout: Duration? { get }

    /// Guard + outcome-based blocking. See default below.
    func shouldRun(_ ctx: RunContext) async -> Bool
    /// The work. Throwing → `.failed`; returning → `.success`.
    func run(_ ctx: RunContext) async throws -> Value
}

public extension Operation {
    var dependsOn: [any Operation.Type] { [] }
    var timeout: Duration? { nil }

    /// Default cascade: run only if EVERY declared dependency succeeded.
    /// Override to take full control (the documented escape hatch for
    /// outcome-based blocking or running despite a failed-open dependency).
    func shouldRun(_ ctx: RunContext) async -> Bool {
        dependsOn.allSatisfy { ctx.succeeded($0) }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `swift test --filter ContextTests`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: add Operation and RunContext protocols with default-skip cascade"
```

---

### Task 5: AnyOperation — type-erased executable node

**Files:**
- Create: `Sources/OperationPipeline/AnyOperation.swift`
- Test: `Tests/OperationPipelineTests/AnyOperationTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/AnyOperationTests.swift`:
```swift
import Testing
@testable import OperationPipeline

private struct EchoOp: Operation {
    typealias Value = Int
    let out: Int
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> Int { out }
}

private struct ThrowingOp: Operation {
    typealias Value = Int
    var failurePolicy: FailurePolicy { .close }
    func run(_ ctx: RunContext) async throws -> Int { throw PlainError(code: 5) }
}

@Test func anyOperationCarriesIdentityAndPolicy() {
    let any = AnyOperation(EchoOp(out: 3))
    #expect(any.id == ObjectIdentifier(EchoOp.self))
    #expect(any.valueTypeID == ObjectIdentifier(Int.self))
    #expect(any.failurePolicy == .open)
    #expect(any.dependsOn.isEmpty)
}

@Test func anyOperationRunBoxesSuccess() async {
    let any = AnyOperation(EchoOp(out: 7))
    let box = await any.run(PipelineContext(outcomes: [:]))
    #expect(box.kind == .success)
    if case .success(let v)? = box.unwrap(as: Int.self) { #expect(v == 7) }
    else { Issue.record("expected success") }
}

@Test func anyOperationRunBoxesThrowAsFailed() async {
    let any = AnyOperation(ThrowingOp())
    let box = await any.run(PipelineContext(outcomes: [:]))
    #expect(box.kind == .failed)
    #expect((box.failureError as? PlainError) == PlainError(code: 5))
}

@Test func anyOperationFactoriesUseConcreteValueType() {
    let any = AnyOperation(EchoOp(out: 1))
    // skipped/failed boxes must unwrap as Int (the op's Value), not crash.
    #expect(any.makeSkipped(reason: "g").unwrap(as: Int.self) != nil)
    #expect(any.makeFailed(PlainError(code: 9)).unwrap(as: Int.self) != nil)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter AnyOperationTests`
Expected: FAIL — `AnyOperation` not defined.

- [ ] **Step 3: Write the implementation**

`Sources/OperationPipeline/AnyOperation.swift`:
```swift
import Foundation

/// Type-erased, executable wrapper around a concrete `Operation`. Captures the
/// op's `Value` type in closures so engine-created skipped/failed outcomes are
/// boxed with the SAME concrete type the op would produce (so later
/// `unwrap(as: O.Value.self)` reads never trap).
public struct AnyOperation: Sendable {
    public let id: ObjectIdentifier
    public let valueTypeID: ObjectIdentifier
    public let dependsOn: [ObjectIdentifier]
    public let dependsOnTypes: [any Operation.Type]   // retained for diagnostics
    public let failurePolicy: FailurePolicy
    public let timeout: Duration?
    public let typeName: String

    private let _shouldRun: @Sendable (RunContext) async -> Bool
    private let _run: @Sendable (RunContext) async -> AnyOutcomeBox
    private let _skipped: @Sendable (String) -> AnyOutcomeBox
    private let _failed: @Sendable (any Error & Sendable) -> AnyOutcomeBox

    public init<O: Operation>(_ op: O) {
        id = ObjectIdentifier(O.self)
        valueTypeID = ObjectIdentifier(O.Value.self)
        dependsOn = op.dependsOn.map { ObjectIdentifier($0) }
        dependsOnTypes = op.dependsOn
        failurePolicy = op.failurePolicy
        timeout = op.timeout
        typeName = String(describing: O.self)

        _shouldRun = { await op.shouldRun($0) }
        _run = { ctx in
            do { return AnyOutcomeBox(Outcome<O.Value>.success(try await op.run(ctx))) }
            catch { return AnyOutcomeBox(Outcome<O.Value>.failed(asSendable(error))) }
        }
        _skipped = { AnyOutcomeBox(Outcome<O.Value>.skipped(reason: $0)) }
        _failed  = { AnyOutcomeBox(Outcome<O.Value>.failed($0)) }
    }

    public func shouldRun(_ ctx: RunContext) async -> Bool { await _shouldRun(ctx) }
    public func run(_ ctx: RunContext) async -> AnyOutcomeBox { await _run(ctx) }
    public func makeSkipped(reason: String) -> AnyOutcomeBox { _skipped(reason) }
    public func makeFailed(_ error: any Error & Sendable) -> AnyOutcomeBox { _failed(error) }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter AnyOperationTests`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add AnyOperation type erasure with typed skipped/failed factories"
```

---

### Task 6: SessionScope actor — per-run state and dependency gates

**Files:**
- Create: `Sources/OperationPipeline/SessionScope.swift`
- Test: `Tests/OperationPipelineTests/SessionScopeTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/SessionScopeTests.swift`:
```swift
import Testing
@testable import OperationPipeline

private let idA = ObjectIdentifier(Int.self)        // arbitrary distinct identities
private let idB = ObjectIdentifier(String.self)

@Test func awaitOutcomeReturnsImmediatelyIfAlreadyComplete() async {
    let scope = SessionScope()
    await scope.beginRun()
    await scope.complete(idA, AnyOutcomeBox(Outcome<Int>.success(1)))
    let box = await scope.awaitOutcome(of: idA)
    #expect(box.kind == .success)
}

@Test func awaitOutcomeSuspendsUntilCompleted() async {
    let scope = SessionScope()
    await scope.beginRun()
    // Start a waiter before the value exists.
    async let waited = scope.awaitOutcome(of: idB)
    await scope.complete(idB, AnyOutcomeBox(Outcome<String>.success("x")))
    let box = await waited
    #expect(box.kind == .success)
}

@Test func snapshotMergesLiveAndWorkingWithWorkingWinning() async {
    let scope = SessionScope()
    await scope.beginRun()
    await scope.complete(idA, AnyOutcomeBox(Outcome<Int>.success(1)))
    await scope.publish()                  // idA → live
    await scope.beginRun()                 // new run
    await scope.complete(idA, AnyOutcomeBox(Outcome<Int>.skipped(reason: "rerun")))
    let snap = await scope.snapshot()
    #expect(snap[idA]?.kind == .skipped)   // working overrides live
}

@Test func publishMovesWorkingIntoLive() async {
    let scope = SessionScope()
    await scope.beginRun()
    await scope.complete(idA, AnyOutcomeBox(Outcome<Int>.success(1)))
    await scope.publish()
    let live = await scope.liveSnapshot()
    #expect(live[idA]?.kind == .success)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter SessionScopeTests`
Expected: FAIL — `SessionScope` not defined.

- [ ] **Step 3: Write the implementation**

`Sources/OperationPipeline/SessionScope.swift`:
```swift
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
    public func awaitOutcome(of id: ObjectIdentifier) async -> AnyOutcomeBox {
        if let o = working[id] { return o }
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

    private func flushWaiters(with sentinel: AnyOutcomeBox) {
        let pending = waiters
        waiters = [:]
        for (_, cs) in pending { for c in cs { c.resume(returning: sentinel) } }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter SessionScopeTests`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add SessionScope actor with dependency-gate continuations"
```

---

### Task 7: OperationEngine scheduler — structured children + gates (happy path)

**Files:**
- Create: `Sources/OperationPipeline/OperationEngine.swift`
- Create: `Sources/OperationPipeline/Pipeline.swift` (minimal; full validation in Task 11)
- Create: `Tests/OperationPipelineTests/TestSupport.swift`
- Test: `Tests/OperationPipelineTests/SchedulerTests.swift`

- [ ] **Step 1: Write shared test support**

`Tests/OperationPipelineTests/TestSupport.swift`:
```swift
import Foundation
import Testing            // needed once helpers (Task 8's OverrideD) use #expect
@testable import OperationPipeline

/// Records the order in which operations ran (thread-safe).
actor RunLog {
    private(set) var order: [String] = []
    func record(_ name: String) { order.append(name) }
}

/// A synthetic op that records when it runs and returns its name length.
/// Generic over a phantom marker so each becomes a distinct type/identity.
struct Probe<Marker>: Operation {
    typealias Value = String
    let name: String
    let log: RunLog
    let deps: [any Operation.Type]
    let policy: FailurePolicy
    let body: @Sendable (RunContext) async throws -> String

    var dependsOn: [any Operation.Type] { deps }
    var failurePolicy: FailurePolicy { policy }

    func run(_ ctx: RunContext) async throws -> String {
        await log.record(name)
        return try await body(ctx)
    }
}

// Distinct phantom markers → distinct Operation types → distinct identities.
enum MA {}; enum MB {}; enum MC {}; enum MD {}
typealias OpA = Probe<MA>
typealias OpB = Probe<MB>
typealias OpC = Probe<MC>
typealias OpD = Probe<MD>
```

- [ ] **Step 2: Write the failing scheduler test**

`Tests/OperationPipelineTests/SchedulerTests.swift`:
```swift
import Testing
@testable import OperationPipeline

@Test func runsAllIndependentOperations() async throws {
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [], policy: .open, body: { _ in "b" })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b)])

    let engine = OperationEngine()
    let scope = SessionScope()
    let result = await engine.run(pipeline, scope: scope)

    #expect(result.isCompleted)
    let order = await log.order
    #expect(Set(order) == ["A", "B"])
}

@Test func respectsDependencyOrdering() async throws {
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    // B depends on A.
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { ctx in
        #expect(ctx.value(of: OpA.self) == "a")   // A's value is available
        return "b"
    })
    let pipeline = try buildPipeline([AnyOperation(b), AnyOperation(a)])  // order shuffled

    let engine = OperationEngine()
    let result = await engine.run(pipeline, scope: SessionScope())

    #expect(result.isCompleted)
    let order = await log.order
    #expect(order == ["A", "B"])   // A always before B regardless of registration order
}

@Test func independentBranchesRunConcurrently() async throws {
    // A -> B, A -> C ; B and C are independent of each other.
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    let c = OpC(name: "C", log: log, deps: [OpA.self], policy: .open, body: { _ in "c" })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b), AnyOperation(c)])

    let result = await OperationEngine().run(pipeline, scope: SessionScope())
    #expect(result.isCompleted)
    let order = await log.order
    #expect(order.first == "A")            // A precedes both
    #expect(Set(order) == ["A", "B", "C"])
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `swift test --filter SchedulerTests`
Expected: FAIL — `buildPipeline`, `OperationEngine`, `RunResult` not defined.

- [ ] **Step 4: Write minimal `Pipeline.swift`**

```swift
/// A set of operations to run as one DAG. Full validation is added in Task 11;
/// this minimal version just stores the operations.
public struct Pipeline: Sendable {
    public let operations: [AnyOperation]
    public init(operations: [AnyOperation]) { self.operations = operations }
}

/// Minimal builder (replaced with validating version in Task 11).
public func buildPipeline(_ operations: [AnyOperation]) throws -> Pipeline {
    Pipeline(operations: operations)
}
```

- [ ] **Step 5: Write `OperationEngine.swift` (happy-path scheduler)**

```swift
import Foundation

/// Outcome of running one pipeline.
public enum RunResult: Sendable {
    case completed
    case aborted(any Error & Sendable)

    public var isCompleted: Bool { if case .completed = self { return true }; return false }
}

/// Stateless-across-sessions DAG scheduler. All mutable run state lives in the
/// passed `SessionScope`.
public actor OperationEngine {
    public init() {}

    public func run(_ pipeline: Pipeline, scope: SessionScope) async -> RunResult {
        await scope.beginRun()

        return await withTaskGroup(of: NodeResult.self) { group in
            for node in pipeline.operations {
                group.addTask {
                    await Self.runNode(node, scope: scope)
                }
            }

            var abortError: (any Error & Sendable)? = nil
            for await r in group {
                if r.outcome.kind == .failed,
                   r.failurePolicy == .close,
                   abortError == nil {
                    abortError = r.outcome.failureError
                    await scope.abort()
                    group.cancelAll()
                }
            }

            if let e = abortError { return .aborted(e) }
            await scope.publish()
            return .completed
        }
    }

    /// Run one node: gate on dependencies, evaluate the guard, then run.
    private static func runNode(_ node: AnyOperation, scope: SessionScope) async -> NodeResult {
        // The awaited box is a SIGNAL ONLY (it tells us the upstream completed);
        // we discard it and re-read everything from `scope.snapshot()` below.
        // This is why the abort sentinel's concrete type is irrelevant and never
        // risks a type-mismatch trap — nobody `unwrap`s the value returned here.
        for dep in node.dependsOn { _ = await scope.awaitOutcome(of: dep) }

        // NOTE: hoist the await — `await` is not allowed inside the `||`
        // right-hand autoclosure under Swift 6 strict concurrency.
        let aborted = await scope.isAborted
        if Task.isCancelled || aborted {
            let box = node.makeSkipped(reason: "cancelled")
            await scope.complete(node.id, box)
            return NodeResult(id: node.id, failurePolicy: node.failurePolicy, outcome: box)
        }

        let ctx = PipelineContext(outcomes: await scope.snapshot())
        let outcome: AnyOutcomeBox
        if await node.shouldRun(ctx) {
            outcome = await node.run(ctx)
        } else {
            outcome = node.makeSkipped(reason: "guard returned false")
        }
        await scope.complete(node.id, outcome)
        return NodeResult(id: node.id, failurePolicy: node.failurePolicy, outcome: outcome)
    }
}

/// Internal per-node result surfaced to the engine's collection loop.
struct NodeResult: Sendable {
    let id: ObjectIdentifier
    let failurePolicy: FailurePolicy
    let outcome: AnyOutcomeBox
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `swift test --filter SchedulerTests`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: add OperationEngine DAG scheduler with structured gates"
```

---

### Task 8: Cascade rules — default-skip and guard override

**Files:**
- Test: `Tests/OperationPipelineTests/CascadeTests.swift`
- (No implementation changes expected — the default `shouldRun` from Task 4 plus the scheduler from Task 7 already implement this. These tests pin the behavior. If a test fails, fix the engine/guard, not the test.)

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/CascadeTests.swift`:
```swift
import Testing
@testable import OperationPipeline

@Test func dependentDefaultsToSkippedWhenDependencyFailsOpen() async throws {
    let log = RunLog()
    // A fails-open; B depends on A with the DEFAULT guard → B must be skipped.
    let a = OpA(name: "A", log: log, deps: [], policy: .open,
                body: { _ in throw PlainError(code: 1) })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b)])

    let scope = SessionScope()
    let result = await OperationEngine().run(pipeline, scope: scope)

    #expect(result.isCompleted)                 // fail-open does not abort
    let order = await log.order
    #expect(order == ["A"])                      // B never ran
    let live = await scope.liveSnapshot()
    #expect(live[ObjectIdentifier(OpB.self)]?.kind == .skipped)
}

@Test func diamondWithGuardOverrideRunsDespiteFailedDependency() async throws {
    // A -> {B(fails-open), C(succeeds)} -> D. D overrides to run on C alone.
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open,
                body: { _ in throw PlainError(code: 2) })
    let c = OpC(name: "C", log: log, deps: [OpA.self], policy: .open, body: { _ in "c" })
    let d = OverrideD(name: "D", log: log)
    let pipeline = try buildPipeline([
        AnyOperation(a), AnyOperation(b), AnyOperation(c), AnyOperation(d),
    ])

    let scope = SessionScope()
    let result = await OperationEngine().run(pipeline, scope: scope)

    #expect(result.isCompleted)
    let order = await log.order
    #expect(order.contains("D"))                 // override made D run
    let live = await scope.liveSnapshot()
    #expect(live[ObjectIdentifier(OverrideD.self)]?.kind == .success)
}
```

Add the override op to `TestSupport.swift`:
```swift
/// D depends on B and C; overrides the default cascade to run when C succeeded,
/// even though B failed-open. Reads C's value, tolerates B being nil.
struct OverrideD: Operation {
    typealias Value = String
    let name: String
    let log: RunLog
    var dependsOn: [any Operation.Type] { [OpB.self, OpC.self] }
    var failurePolicy: FailurePolicy { .open }

    func shouldRun(_ ctx: RunContext) async -> Bool {
        ctx.succeeded(OpC.self)                  // run on C alone; ignore B
    }
    func run(_ ctx: RunContext) async throws -> String {
        await log.record(name)
        #expect(ctx.value(of: OpB.self) == nil)  // B failed → nil, handled
        return ctx.value(of: OpC.self) ?? "?"
    }
}
```
> Note: `#expect` inside this non-test helper requires `import Testing` at the top of `TestSupport.swift` — already added in Task 7, so no change needed here.

- [ ] **Step 2: Run test to verify it fails (or passes immediately)**

Run: `swift test --filter CascadeTests`
Expected: ideally PASS (behavior already implemented). If FAIL, debug the engine/guard — the cascade is supposed to already work. Do not weaken the tests.

- [ ] **Step 3: If any test failed, fix the implementation**

If `dependentDefaultsToSkippedWhenDependencyFailsOpen` fails: verify `OperationEngine.runNode` calls `node.shouldRun(ctx)` and that `PipelineContext.succeeded` reads `.kind == .success`. The default `shouldRun` must see B's snapshot containing A's `.failed` box.

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter CascadeTests`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test: pin default-skip cascade and guard-override semantics"
```

---

### Task 9: Fail policy — fail-close abort, fail-open prune

**Files:**
- Test: `Tests/OperationPipelineTests/FailPolicyTests.swift`
- (Implementation already in Task 7's engine. These tests pin abort/prune. Fix the engine if they fail.)

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/FailPolicyTests.swift`:
```swift
import Testing
@testable import OperationPipeline

@Test func failCloseAbortsRunWithError() async throws {
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .close,
                body: { _ in throw PlainError(code: 42) })
    let pipeline = try buildPipeline([AnyOperation(a)])

    let result = await OperationEngine().run(pipeline, scope: SessionScope())

    if case .aborted(let e) = result {
        #expect((e as? PlainError) == PlainError(code: 42))
    } else {
        Issue.record("expected .aborted")
    }
}

@Test func failOpenDoesNotAbortAndPrunesDependents() async throws {
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open,
                body: { _ in throw PlainError(code: 1) })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    let c = OpC(name: "C", log: log, deps: [], policy: .open, body: { _ in "c" })  // independent
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b), AnyOperation(c)])

    let result = await OperationEngine().run(pipeline, scope: SessionScope())

    #expect(result.isCompleted)
    let order = await log.order
    #expect(order.contains("C"))      // independent op still ran
    #expect(!order.contains("B"))     // dependent of failed-open A was pruned (skipped)
}

@Test func firstFailCloseWins() async throws {
    // Two close-failures; result carries the first observed error, run aborts.
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .close,
                body: { _ in throw PlainError(code: 1) })
    let b = OpB(name: "B", log: log, deps: [], policy: .close,
                body: { _ in throw PlainError(code: 2) })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b)])

    let result = await OperationEngine().run(pipeline, scope: SessionScope())
    if case .aborted = result { #expect(Bool(true)) } else { Issue.record("expected abort") }
}
```

- [ ] **Step 2: Run test to verify status**

Run: `swift test --filter FailPolicyTests`
Expected: ideally PASS. If `failOpenDoesNotAbortAndPrunesDependents` fails because the run hangs, the abort/cancellation path has a stuck continuation — see Step 3.

- [ ] **Step 3: If a fail-close test hangs, harden cancellation**

A hang means a node suspended in `awaitOutcome` was never resumed after `group.cancelAll()`. Verify `OperationEngine.run` calls `await scope.abort()` (which flushes waiters) BEFORE/with `group.cancelAll()`, and that `runNode` checks `await scope.isAborted` after its gate loop and returns a skipped box without running.

This combination guarantees every *gated* and every *cooperative* child finishes promptly on abort. It does NOT bound a node already inside a non-cooperative `run` that ignores cancellation — the engine's task group will await that node at scope exit. The contract (matching the spec): **an operation that may not return promptly MUST declare a `timeout`**, which (via Task 10's unstructured race) makes even a non-cooperative op non-blocking. Ops without a timeout are assumed cooperative. Do not weaken this to claim unconditional bounding.

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter FailPolicyTests`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test: pin fail-close abort and fail-open prune behavior"
```

---

### Task 10: Per-operation timeout

**Files:**
- Modify: `Sources/OperationPipeline/OperationEngine.swift` (wrap `node.run` in a timeout race)
- Test: `Tests/OperationPipelineTests/TimeoutTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/TimeoutTests.swift`:
```swift
import Testing
@testable import OperationPipeline

private struct SlowOp<M>: Operation {
    typealias Value = String
    let policy: FailurePolicy
    var failurePolicy: FailurePolicy { policy }
    var timeout: Duration? { .milliseconds(50) }
    func run(_ ctx: RunContext) async throws -> String {
        try? await Task.sleep(for: .seconds(10))   // far longer than the timeout
        return "done"
    }
}
private enum SM1 {}; private enum SM2 {}

@Test func failOpenTimeoutIsPrunedAndRunCompletes() async throws {
    let op = SlowOp<SM1>(policy: .open)
    let pipeline = try buildPipeline([AnyOperation(op)])
    let scope = SessionScope()

    let result = await OperationEngine().run(pipeline, scope: scope)

    #expect(result.isCompleted)                    // fail-open timeout does not abort
    let live = await scope.liveSnapshot()
    #expect(live[ObjectIdentifier(SlowOp<SM1>.self)]?.kind == .failed)
}

@Test func failCloseTimeoutAbortsRun() async throws {
    let op = SlowOp<SM2>(policy: .close)
    let pipeline = try buildPipeline([AnyOperation(op)])

    let result = await OperationEngine().run(pipeline, scope: SessionScope())

    if case .aborted = result { #expect(Bool(true)) } else { Issue.record("expected abort") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter TimeoutTests`
Expected: FAIL — currently the run waits ~10s (no timeout applied) or the assertions don't hold.

- [ ] **Step 3: Add a timeout race to the engine**

In `OperationEngine.swift`, replace the `outcome = await node.run(ctx)` line in `runNode` with a call to a new helper, and add the helper:

```swift
// In runNode, the guard-passed branch becomes:
if await node.shouldRun(ctx) {
    outcome = await runWithOptionalTimeout(node, ctx)
} else {
    outcome = node.makeSkipped(reason: "guard returned false")
}
```

Add to `OperationEngine`:
```swift
/// Race the op against its deadline. On expiry, produce a typed `.failed`
/// outcome (subject to the op's failurePolicy upstream).
///
/// IMPORTANT — why this is NOT a `withTaskGroup`: a task group always awaits
/// ALL its children at scope exit. If the op ignores cancellation, exiting the
/// group would block until the op returns — defeating the timeout entirely and
/// violating the spec's login-liveness guarantee. Instead the op runs in an
/// UNSTRUCTURED `Task` whose completion we never await: we read the first racer
/// to publish on an `AsyncStream` and return immediately. On timeout we request
/// cancellation of the op and ABANDON it (Swift has no forcible termination);
/// when it eventually finishes it writes into its own session scope, which is
/// safe by the write-boundary invariant.
private static func runWithOptionalTimeout(
    _ node: AnyOperation, _ ctx: RunContext
) async -> AnyOutcomeBox {
    guard let timeout = node.timeout else { return await node.run(ctx) }

    let (stream, continuation) = AsyncStream<AnyOutcomeBox>.makeStream()

    // The op runs unstructured; we will NOT await its completion.
    let work = Task {
        let box = await node.run(ctx)
        continuation.yield(box)
        continuation.finish()
    }
    // The timer races it.
    let timer = Task {
        if (try? await Task.sleep(for: timeout)) != nil {
            continuation.yield(node.makeFailed(TimeoutError(operation: node.typeName)))
        }
        continuation.finish()
    }

    var iterator = stream.makeAsyncIterator()
    // First racer to yield wins; if cancelled mid-wait, next() returns nil.
    let first = await iterator.next() ?? node.makeFailed(TimeoutError(operation: node.typeName))
    work.cancel()    // request cancellation of the op (cooperative; abandoned otherwise)
    timer.cancel()
    return first
}
```

Add a `TimeoutError` type to `Outcome.swift`:
```swift
public struct TimeoutError: Error, Sendable, Equatable {
    public let operation: String
    public init(operation: String) { self.operation = operation }
}
```

> Why `AsyncStream`, not `CheckedContinuation`: `AsyncStream.Continuation` is
> `Sendable`, so capturing it in the two `@Sendable` racer tasks is clean under
> Swift 6 strict concurrency (a bare `CheckedContinuation` is not `Sendable`).
> `AnyOutcomeBox` is `Sendable`, so the stream element type is fine.

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter TimeoutTests`
Expected: PASS (2 tests), each completing in well under a second.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add per-operation timeout race tied to failurePolicy"
```

---

### Task 11: Pipeline validation — duplicates, unregistered deps, cycles

**Files:**
- Modify: `Sources/OperationPipeline/Pipeline.swift` (replace the minimal builder)
- Test: `Tests/OperationPipelineTests/ValidationTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/ValidationTests.swift`:
```swift
import Testing
@testable import OperationPipeline

@Test func rejectsDuplicateOperationType() {
    let log = RunLog()
    let a1 = OpA(name: "A1", log: log, deps: [], policy: .open, body: { _ in "a" })
    let a2 = OpA(name: "A2", log: log, deps: [], policy: .open, body: { _ in "a" })
    #expect(throws: PipelineValidationError.self) {
        _ = try buildPipeline([AnyOperation(a1), AnyOperation(a2)])
    }
}

@Test func rejectsUnregisteredDependency() {
    let log = RunLog()
    // B depends on A, but A is not registered (also covers cross-pipeline edges).
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    #expect(throws: PipelineValidationError.self) {
        _ = try buildPipeline([AnyOperation(b)])
    }
}

@Test func rejectsCycle() {
    let log = RunLog()
    // A depends on B, B depends on A → cycle.
    let a = OpA(name: "A", log: log, deps: [OpB.self], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    #expect(throws: PipelineValidationError.self) {
        _ = try buildPipeline([AnyOperation(a), AnyOperation(b)])
    }
}

@Test func acceptsValidAcyclicGraph() throws {
    let log = RunLog()
    let a = OpA(name: "A", log: log, deps: [], policy: .open, body: { _ in "a" })
    let b = OpB(name: "B", log: log, deps: [OpA.self], policy: .open, body: { _ in "b" })
    let pipeline = try buildPipeline([AnyOperation(a), AnyOperation(b)])
    #expect(pipeline.operations.count == 2)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter ValidationTests`
Expected: FAIL — `PipelineValidationError` not defined; current `buildPipeline` never throws.

- [ ] **Step 3: Replace `Pipeline.swift` with the validating builder**

```swift
import Foundation

public struct Pipeline: Sendable {
    public let operations: [AnyOperation]
    init(validated operations: [AnyOperation]) { self.operations = operations }
}

public enum PipelineValidationError: Error, Equatable {
    case duplicateType(String)
    case unregisteredDependency(operation: String, missing: String)
    case cycle([String])
}

/// Build a validated pipeline. Fails fast (at startup) on a duplicate op type,
/// a dependency on an unregistered type (this also rejects cross-pipeline edges,
/// since the other pipeline's ops are not registered here), or a cycle.
public func buildPipeline(_ operations: [AnyOperation]) throws -> Pipeline {
    // 1. Duplicate detection by type identity.
    var byID: [ObjectIdentifier: AnyOperation] = [:]
    for op in operations {
        if byID[op.id] != nil { throw PipelineValidationError.duplicateType(op.typeName) }
        byID[op.id] = op
    }

    // 2. Every dependency must resolve to a registered op.
    for op in operations {
        for (depID, depType) in zip(op.dependsOn, op.dependsOnTypes) where byID[depID] == nil {
            throw PipelineValidationError.unregisteredDependency(
                operation: op.typeName, missing: String(describing: depType)
            )
        }
    }

    // 3. Cycle detection (DFS over dependency edges).
    var state: [ObjectIdentifier: Int] = [:]   // 0=unvisited, 1=on-stack, 2=done
    var stack: [String] = []
    func visit(_ id: ObjectIdentifier) throws {
        switch state[id] ?? 0 {
        case 2: return
        case 1: throw PipelineValidationError.cycle(stack)
        default: break
        }
        let op = byID[id]!
        state[id] = 1
        stack.append(op.typeName)
        for dep in op.dependsOn { try visit(dep) }
        stack.removeLast()
        state[id] = 2
    }
    for op in operations { try visit(op.id) }

    return Pipeline(validated: operations)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter ValidationTests`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full suite to confirm no regressions**

Run: `swift test`
Expected: all prior tests still PASS (they used valid graphs).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: add fail-fast pipeline validation (duplicates, deps, cycles)"
```

---

### Task 12: Re-run staging + atomic publish

**Files:**
- Test: `Tests/OperationPipelineTests/ReRunTests.swift`
- (Implementation already supported by `SessionScope.beginRun`/`publish` and `snapshot`. These tests verify atomicity. Fix the scope if they fail.)

- [ ] **Step 1: Write the failing test**

`Tests/OperationPipelineTests/ReRunTests.swift`:
```swift
import Testing
@testable import OperationPipeline

@Test func reRunOverwritesPublishedValuesWithLatest() async throws {
    let log = RunLog()
    // The op returns a different value on each run via an external counter.
    let counter = Counter()
    let a = OpA(name: "A", log: log, deps: [], policy: .open,
                body: { _ in "v\(await counter.next())" })
    let pipeline = try buildPipeline([AnyOperation(a)])
    let engine = OperationEngine()
    let scope = SessionScope()

    _ = await engine.run(pipeline, scope: scope)
    let first = await scope.liveSnapshot()[ObjectIdentifier(OpA.self)]
    #expect(first?.unwrap(as: String.self)?.successValue == "v1")

    _ = await engine.run(pipeline, scope: scope)         // re-run, same scope
    let second = await scope.liveSnapshot()[ObjectIdentifier(OpA.self)]
    #expect(second?.unwrap(as: String.self)?.successValue == "v2")  // latest wins
}

@Test func crossPipelineReadsSeeLastPublishedValueDuringNoRun() async throws {
    // After a gating run publishes, a separate (post-login) op reading via a
    // fresh run sees the published value through the shared live map.
    let log = RunLog()
    let gating = OpA(name: "G", log: log, deps: [], policy: .open, body: { _ in "ent" })
    let gatingPipeline = try buildPipeline([AnyOperation(gating)])

    // Post-login op reads OpA's published value from the shared live map.
    let post = ReadsA(log: log)
    let postPipeline = try buildPipeline([AnyOperation(post)])

    let engine = OperationEngine()
    let scope = SessionScope()
    _ = await engine.run(gatingPipeline, scope: scope)
    _ = await engine.run(postPipeline, scope: scope)

    let live = await scope.liveSnapshot()
    #expect(live[ObjectIdentifier(ReadsA.self)]?.unwrap(as: String.self)?.successValue == "saw:ent")
}
```

Add helpers to `TestSupport.swift`:
```swift
actor Counter { private var n = 0; func next() -> Int { n += 1; return n } }

extension Outcome {
    var successValue: Value? { if case .success(let v) = self { return v }; return nil }
}

/// Post-login-style op (no dependency edge) that reads OpA's value from the
/// shared session context (live map), proving cross-pipeline reads work.
struct ReadsA: Operation {
    typealias Value = String
    let log: RunLog
    var dependsOn: [any Operation.Type] { [] }   // NO edge — reads via shared context
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String {
        await log.record("ReadsA")
        return "saw:\(ctx.value(of: OpA.self) ?? "nil")"
    }
}
```
> Note: `ReadsA` reading `OpA` WITHOUT a `dependsOn` edge is exactly the cross-pipeline pattern — the value comes from the shared live map, not a dependency gate. This is allowed; cross-pipeline *edges* are what validation rejects, not cross-pipeline *reads*.

- [ ] **Step 2: Run test to verify status**

Run: `swift test --filter ReRunTests`
Expected: ideally PASS. If `crossPipelineReadsSeeLastPublishedValue...` fails with `nil`, verify `runNode` builds the context from `await scope.snapshot()` (which merges live + working), not from working alone.

- [ ] **Step 3: Fix if needed**

If the re-run test shows stale values, verify `SessionScope.publish()` merges working into live with working winning, and `beginRun()` clears working so the next run re-computes.

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter ReRunTests`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test: verify re-run overwrite and cross-pipeline shared-context reads"
```

---

### Task 13: Auth core — AuthState, AuthError, GatingRunner, SessionScopeBroker

**Files:**
- Create: `Sources/Auth/AuthState.swift`
- Create: `Sources/Auth/GatingRunner.swift`
- Create: `Sources/Auth/SessionScopeBroker.swift`
- Delete: `Sources/Auth/Placeholder.swift`
- Test: `Tests/AuthTests/AuthCoreTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/AuthTests/AuthCoreTests.swift`:
```swift
import Testing
import OperationPipeline
@testable import Auth

@Test func authStateEqualityIgnoresUnderlyingErrorIdentity() {
    let e1 = AuthError(failingOperation: "Ent", underlying: PlainAuthError())
    let e2 = AuthError(failingOperation: "Ent", underlying: PlainAuthError())
    #expect(AuthState.unauthenticated(e1) == AuthState.unauthenticated(e2))
    #expect(AuthState.authenticated == AuthState.authenticated)
    #expect(AuthState.unauthenticated(nil) != AuthState.authenticated)
}

@Test func brokerBeginCreatesFreshScopeAndEndDrops() async {
    let broker = SessionScopeBroker()
    let s1 = await broker.beginSession()
    #expect(await broker.currentSession() === s1)
    let s2 = await broker.beginSession()           // re-login mints a new scope
    #expect(s2 !== s1)
    await broker.endSession()
    #expect(await broker.currentSession() == nil)
}

struct PlainAuthError: Error, Sendable {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter AuthCoreTests`
Expected: FAIL — `AuthError`, `AuthState`, `SessionScopeBroker` not defined.

- [ ] **Step 3: Write `AuthState.swift`**

```swift
import OperationPipeline

/// Error the auth module surfaces when gating aborts login. Wraps the engine's
/// fail-close error and identifies the blocking operation, if known.
public struct AuthError: Error, Sendable {
    public let failingOperation: String?
    public let underlying: any Error & Sendable
    public init(failingOperation: String?, underlying: any Error & Sendable) {
        self.failingOperation = failingOperation
        self.underlying = underlying
    }
}

/// Auth state published by the auth module. The host drives login UI off this.
public enum AuthState: Sendable {
    case unauthenticated(AuthError?)   // nil = clean / initial
    case authenticating
    case authenticated
}

extension AuthState: Equatable {
    public static func == (lhs: AuthState, rhs: AuthState) -> Bool {
        switch (lhs, rhs) {
        case (.authenticating, .authenticating),
             (.authenticated, .authenticated):
            return true
        case let (.unauthenticated(a), .unauthenticated(b)):
            // Compare by presence + failing-operation label, not error identity.
            return a?.failingOperation == b?.failingOperation && (a == nil) == (b == nil)
        default:
            return false
        }
    }
}
```

- [ ] **Step 4: Write `GatingRunner.swift`**

```swift
/// The single opaque dependency the auth module awaits during login. The auth
/// module knows nothing about concrete operations behind this protocol.
public protocol GatingRunner: Sendable {
    func run() async -> GatingResult
}

public enum GatingResult: Sendable {
    case success
    case aborted(any Error & Sendable)
}
```

- [ ] **Step 5: Write `SessionScopeBroker.swift`**

```swift
import OperationPipeline

/// Shares ONE `SessionScope` between the gating runner and the post-login
/// coordinator for a single auth session. `beginSession` mints a fresh scope
/// (dropping any prior one — re-login/forced-reauth are treated like logout);
/// `endSession` drops it (the write-boundary clear).
public actor SessionScopeBroker {
    private var current: SessionScope?
    public init() {}

    public func beginSession() -> SessionScope {
        let scope = SessionScope()
        current = scope
        return scope
    }

    public func currentSession() -> SessionScope? { current }

    public func endSession() { current = nil }
}
```

- [ ] **Step 6: Delete the placeholder and run tests**

```bash
git rm Sources/Auth/Placeholder.swift
```
Run: `swift test --filter AuthCoreTests`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: add AuthState, AuthError, GatingRunner, SessionScopeBroker"
```

---

### Task 14: EngineGatingRunner adapter

**Files:**
- Create: `Sources/Auth/EngineGatingRunner.swift`
- Test: `Tests/AuthTests/EngineGatingRunnerTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/AuthTests/EngineGatingRunnerTests.swift`:
```swift
import Testing
import OperationPipeline
@testable import Auth

private enum GM1 {}; private enum GM2 {}
private struct GateOp<M>: Operation {
    typealias Value = String
    let policy: FailurePolicy
    let shouldThrow: Bool
    var failurePolicy: FailurePolicy { policy }
    func run(_ ctx: RunContext) async throws -> String {
        if shouldThrow { throw GateError() }
        return "ok"
    }
}
private struct GateError: Error, Sendable {}

@Test func gatingRunnerReturnsSuccessWhenPipelineCompletes() async throws {
    let op = GateOp<GM1>(policy: .open, shouldThrow: false)
    let pipeline = try buildPipeline([AnyOperation(op)])
    let broker = SessionScopeBroker()
    let runner = EngineGatingRunner(engine: OperationEngine(), pipeline: pipeline, broker: broker)

    let result = await runner.run()

    if case .success = result { #expect(Bool(true)) } else { Issue.record("expected success") }
    #expect(await broker.currentSession() != nil)   // session kept on success
}

@Test func gatingRunnerReturnsAbortedAndEndsSessionOnFailClose() async throws {
    let op = GateOp<GM2>(policy: .close, shouldThrow: true)
    let pipeline = try buildPipeline([AnyOperation(op)])
    let broker = SessionScopeBroker()
    let runner = EngineGatingRunner(engine: OperationEngine(), pipeline: pipeline, broker: broker)

    let result = await runner.run()

    if case .aborted = result { #expect(Bool(true)) } else { Issue.record("expected aborted") }
    #expect(await broker.currentSession() == nil)    // session dropped on abort
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter EngineGatingRunnerTests`
Expected: FAIL — `EngineGatingRunner` not defined.

- [ ] **Step 3: Write the implementation**

`Sources/Auth/EngineGatingRunner.swift`:
```swift
import OperationPipeline

/// Implements `GatingRunner` by running the gating pipeline on a fresh session
/// scope obtained from the broker. On abort, drops the session (login failed).
public struct EngineGatingRunner: GatingRunner {
    private let engine: OperationEngine
    private let pipeline: Pipeline
    private let broker: SessionScopeBroker

    public init(engine: OperationEngine, pipeline: Pipeline, broker: SessionScopeBroker) {
        self.engine = engine
        self.pipeline = pipeline
        self.broker = broker
    }

    public func run() async -> GatingResult {
        let scope = await broker.beginSession()
        switch await engine.run(pipeline, scope: scope) {
        case .completed:
            return .success
        case .aborted(let error):
            await broker.endSession()   // login failed → drop the session context
            return .aborted(error)
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter EngineGatingRunnerTests`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add EngineGatingRunner adapter over OperationEngine"
```

---

### Task 15: AuthModule state machine

**Files:**
- Create: `Sources/Auth/AuthModule.swift`
- Test: `Tests/AuthTests/AuthModuleTests.swift`

- [ ] **Step 1: Write the failing test**

`Tests/AuthTests/AuthModuleTests.swift`:
```swift
import Testing
import OperationPipeline
@testable import Auth

/// Stub gating runner returning a fixed result.
private struct StubGating: GatingRunner {
    let result: GatingResult
    func run() async -> GatingResult { result }
}

/// Stub backend that succeeds or throws.
private struct StubBackend: AuthBackend {
    let fails: Bool
    func authenticate(_ credentials: Credentials) async throws {
        if fails { throw BackendError() }
    }
}
private struct BackendError: Error, Sendable {}

@Test func successfulLoginReachesAuthenticated() async throws {
    let module = AuthModule(backend: StubBackend(fails: false), gating: StubGating(result: .success))

    await module.login(Credentials(username: "u", password: "p"))
    #expect(await module.state == .authenticated)

    // Drain the buffered stream deterministically. AsyncStream's default buffer
    // is `.unbounded`, so every yielded state is retained; iterate until we see
    // `.authenticated`, then break (the stream never finishes on its own).
    var seen: [AuthState] = []
    for await s in module.stateStream {
        seen.append(s)
        if s == .authenticated { break }
    }
    #expect(seen.contains(.authenticating))
    #expect(seen.contains(.authenticated))
}

@Test func gatingAbortKeepsUnauthenticatedWithWrappedError() async throws {
    let module = AuthModule(
        backend: StubBackend(fails: false),
        gating: StubGating(result: .aborted(BackendError()))
    )
    await module.login(Credentials(username: "u", password: "p"))
    if case .unauthenticated(let err) = await module.state {
        #expect(err != nil)
    } else {
        Issue.record("expected unauthenticated with error")
    }
}

@Test func backendFailureNeverRunsGating() async throws {
    let module = AuthModule(backend: StubBackend(fails: true), gating: StubGating(result: .success))
    await module.login(Credentials(username: "u", password: "p"))
    if case .unauthenticated(let err) = await module.state { #expect(err != nil) }
    else { Issue.record("expected unauthenticated") }
}

@Test func logoutReturnsToCleanUnauthenticated() async throws {
    let module = AuthModule(backend: StubBackend(fails: false), gating: StubGating(result: .success))
    await module.login(Credentials(username: "u", password: "p"))
    await module.logout()
    #expect(await module.state == .unauthenticated(nil))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `swift test --filter AuthModuleTests`
Expected: FAIL — `AuthModule`, `AuthBackend`, `Credentials` not defined.

- [ ] **Step 3: Write the implementation**

`Sources/Auth/AuthModule.swift`:
```swift
import OperationPipeline

public struct Credentials: Sendable {
    public let username: String
    public let password: String
    public init(username: String, password: String) {
        self.username = username
        self.password = password
    }
}

/// The backend auth call the module already owns.
public protocol AuthBackend: Sendable {
    func authenticate(_ credentials: Credentials) async throws
}

/// The self-contained auth module. Owns the state machine and publishes
/// `AuthState`. Knows nothing about concrete operations — it only awaits the
/// injected `GatingRunner`. UI presentation is the host's concern, driven by
/// the published state.
public actor AuthModule {
    private let backend: AuthBackend
    private let gating: GatingRunner

    public private(set) var state: AuthState = .unauthenticated(nil) {
        didSet { continuation.yield(state) }
    }

    public nonisolated let stateStream: AsyncStream<AuthState>
    private let continuation: AsyncStream<AuthState>.Continuation

    public init(backend: AuthBackend, gating: GatingRunner) {
        self.backend = backend
        self.gating = gating
        (stateStream, continuation) = AsyncStream.makeStream(of: AuthState.self)
    }

    /// Called on app start; host presents login UI when state is unauthenticated.
    public func start() {
        continuation.yield(state)   // emit current state to fresh observers
    }

    public func login(_ credentials: Credentials) async {
        state = .authenticating

        do {
            try await backend.authenticate(credentials)
        } catch {
            state = .unauthenticated(AuthError(failingOperation: nil, underlying: asSendable(error)))
            return
        }

        switch await gating.run() {
        case .success:
            state = .authenticated
        case .aborted(let error):
            state = .unauthenticated(AuthError(failingOperation: nil, underlying: error))
        }
    }

    public func logout() {
        state = .unauthenticated(nil)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `swift test --filter AuthModuleTests`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add AuthModule state machine with AsyncStream publishing"
```

---

### Task 16: PostLoginCoordinator + write-boundary security + lifecycle

**Files:**
- Create: `Sources/Auth/PostLoginCoordinator.swift`
- Create: `Tests/AuthTests/AuthTestSupport.swift`
- Test: `Tests/AuthTests/PostLoginCoordinatorTests.swift`
- Test: `Tests/AuthTests/WriteBoundaryTests.swift`

- [ ] **Step 1: Write shared auth test support (deterministic, no sleeps)**

`Tests/AuthTests/AuthTestSupport.swift`:
```swift
import Foundation

/// Set-once flag a post-login op flips when it runs.
actor RanFlag { private(set) var value = false; func set() { value = true } }

/// Poll `condition` until it becomes true, yielding between attempts. Returns
/// false if the bound is exhausted. Deterministic and CI-safe — no fixed sleep
/// window to lose a race against.
func poll(
    until condition: @Sendable () async -> Bool,
    attempts: Int = 10_000
) async -> Bool {
    for _ in 0..<attempts {
        if await condition() { return true }
        await Task.yield()
    }
    return false
}
```

- [ ] **Step 2: Write the failing coordinator test**

`Tests/AuthTests/PostLoginCoordinatorTests.swift`:
```swift
import Testing
import OperationPipeline
@testable import Auth

private enum PM {}
private struct PostOp<M>: Operation {
    typealias Value = String
    let ran: RanFlag
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String { await ran.set(); return "post" }
}

@Test func runsPostLoginPipelineOnAuthenticated() async throws {
    let ran = RanFlag()
    let pipeline = try buildPipeline([AnyOperation(PostOp<PM>(ran: ran))])
    let broker = SessionScopeBroker()
    _ = await broker.beginSession()   // simulate gating already created the session

    let (stream, cont) = AsyncStream.makeStream(of: AuthState.self)
    let coordinator = PostLoginCoordinator(engine: OperationEngine(), pipeline: pipeline, broker: broker)
    let observation = coordinator.observe(stream)

    cont.yield(.authenticated)
    let didRun = await poll { await ran.value }   // deterministic, no sleep
    observation.cancel()

    #expect(didRun)
}

@Test func endsSessionOnUnauthenticated() async throws {
    let ran = RanFlag()
    let pipeline = try buildPipeline([AnyOperation(PostOp<PM>(ran: ran))])
    let broker = SessionScopeBroker()
    _ = await broker.beginSession()

    let (stream, cont) = AsyncStream.makeStream(of: AuthState.self)
    let coordinator = PostLoginCoordinator(engine: OperationEngine(), pipeline: pipeline, broker: broker)
    let observation = coordinator.observe(stream)

    cont.yield(.unauthenticated(nil))
    let cleared = await poll { await broker.currentSession() == nil }
    observation.cancel()

    #expect(cleared)   // session cleared on logout
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `swift test --filter PostLoginCoordinatorTests`
Expected: FAIL — `PostLoginCoordinator` not defined.

- [ ] **Step 4: Write the implementation**

`Sources/Auth/PostLoginCoordinator.swift`:
```swift
import OperationPipeline

/// Observes the auth module's state stream and runs the post-login pipeline on
/// the SHARED session scope when authentication completes; cancels and clears
/// the session on logout. The auth module has no knowledge of this.
public final class PostLoginCoordinator: Sendable {
    private let engine: OperationEngine
    private let pipeline: Pipeline
    private let broker: SessionScopeBroker

    public init(engine: OperationEngine, pipeline: Pipeline, broker: SessionScopeBroker) {
        self.engine = engine
        self.pipeline = pipeline
        self.broker = broker
    }

    /// Start observing. Returns the observation task; cancel it to stop observing.
    ///
    /// Note: cancelling the returned task stops consuming the stream, but does
    /// NOT itself cancel an in-flight post-login run (`runTask` is unstructured).
    /// The post-login run is cancelled by the `.unauthenticated` branch below —
    /// i.e. logout cancels the run, which is the guarantee we actually rely on.
    public func observe(_ stream: AsyncStream<AuthState>) -> Task<Void, Never> {
        let engine = engine, pipeline = pipeline, broker = broker
        return Task {
            var runTask: Task<Void, Never>?
            for await state in stream {
                switch state {
                case .authenticated:
                    runTask?.cancel()
                    if let scope = await broker.currentSession() {
                        runTask = Task { _ = await engine.run(pipeline, scope: scope) }
                    }
                case .unauthenticated:
                    runTask?.cancel()                 // cancel in-flight post-login
                    await broker.endSession()         // drop the session context
                case .authenticating:
                    break
                }
            }
            runTask?.cancel()
        }
    }
}
```

- [ ] **Step 5: Run coordinator test to verify it passes**

Run: `swift test --filter PostLoginCoordinatorTests`
Expected: PASS (2 tests).

- [ ] **Step 6: Write the write-boundary security test**

`Tests/AuthTests/WriteBoundaryTests.swift`:
```swift
import Testing
import OperationPipeline
@testable import Auth

private enum LM {}
/// A post-login op that blocks until released, then completes — simulating an
/// op still in-flight when logout fires.
private struct SlowPostOp<M>: Operation {
    typealias Value = String
    let gate: Gate
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String {
        await gate.wait()
        return "late"
    }
}
actor Gate {
    private var continuations: [CheckedContinuation<Void, Never>] = []
    private var open = false
    func wait() async {
        if open { return }
        await withCheckedContinuation { continuations.append($0) }
    }
    func release() { open = true; continuations.forEach { $0.resume() }; continuations = [] }
}

@Test func lateCompletingOpDoesNotLeakIntoNewSession() async throws {
    let gate = Gate()
    let pipeline = try buildPipeline([AnyOperation(SlowPostOp<LM>(gate: gate))])
    let broker = SessionScopeBroker()
    let engine = OperationEngine()

    // Session 1 begins; start the slow post-login run on scope 1.
    let scope1 = await broker.beginSession()
    let run1 = Task { _ = await engine.run(pipeline, scope: scope1) }

    // Logout: drop the session, then a NEW session begins (re-login).
    await broker.endSession()
    let scope2 = await broker.beginSession()
    #expect(scope2 !== scope1)

    // Now let the stale op finish — it writes into scope1 (discarded), never scope2.
    await gate.release()
    _ = await run1.value

    let live2 = await scope2.liveSnapshot()
    #expect(live2[ObjectIdentifier(SlowPostOp<LM>.self)] == nil)   // no leak into new session
}
```

- [ ] **Step 7: Run the write-boundary test**

Run: `swift test --filter WriteBoundaryTests`
Expected: PASS (1 test). The stale op's write landed in `scope1`; `scope2` never observes it.

- [ ] **Step 8: Run the full suite**

Run: `swift test`
Expected: ALL tests pass across both targets.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: add PostLoginCoordinator with write-boundary security guarantee"
```

---

### Task 17: End-to-end integration + worked example operations

**Files:**
- Create: `Tests/AuthTests/IntegrationTests.swift`

- [ ] **Step 1: Write the integration test**

`Tests/AuthTests/IntegrationTests.swift`:
```swift
import Testing
import OperationPipeline
@testable import Auth

// Worked example mirroring the spec: EntitlementsOp (gating, fail-close) →
// PaywallOp (gating, fail-open, outcome-based guard). PushOp (post-login).
private struct Entitlements: Sendable { let showPaywall: Bool }

private struct EntitlementsOp: Operation {
    typealias Value = Entitlements
    var failurePolicy: FailurePolicy { .close }
    func run(_ ctx: RunContext) async throws -> Entitlements { Entitlements(showPaywall: true) }
}
private struct PaywallOp: Operation {
    typealias Value = String
    var dependsOn: [any Operation.Type] { [EntitlementsOp.self] }
    var failurePolicy: FailurePolicy { .open }
    func shouldRun(_ ctx: RunContext) async -> Bool {
        guard let ent = ctx.value(of: EntitlementsOp.self) else { return false }
        return ent.showPaywall
    }
    func run(_ ctx: RunContext) async throws -> String { "paywall-configured" }
}
private struct PushOp: Operation {
    typealias Value = String
    let ran: RanFlag
    var failurePolicy: FailurePolicy { .open }
    func run(_ ctx: RunContext) async throws -> String { await ran.set(); return "registered" }
}

@Test func fullLoginRunsGatingThenPostLogin() async throws {
    let broker = SessionScopeBroker()
    let engine = OperationEngine()

    let gatingPipeline = try buildPipeline([AnyOperation(EntitlementsOp()), AnyOperation(PaywallOp())])
    let pushRan = RanFlag()
    let postPipeline = try buildPipeline([AnyOperation(PushOp(ran: pushRan))])

    let gatingRunner = EngineGatingRunner(engine: engine, pipeline: gatingPipeline, broker: broker)
    let module = AuthModule(backend: AlwaysOKBackend(), gating: gatingRunner)
    let coordinator = PostLoginCoordinator(engine: engine, pipeline: postPipeline, broker: broker)
    let observation = coordinator.observe(module.stateStream)

    await module.login(Credentials(username: "u", password: "p"))
    #expect(await module.state == .authenticated)

    let didRun = await poll { await pushRan.value }   // deterministic; from AuthTestSupport
    observation.cancel()
    #expect(didRun)

    // The shared session context carries the gating entitlements value.
    let scope = await broker.currentSession()
    let live = await scope?.liveSnapshot() ?? [:]
    #expect(live[ObjectIdentifier(EntitlementsOp.self)]?.kind == .success)
}

private struct AlwaysOKBackend: AuthBackend {
    func authenticate(_ credentials: Credentials) async throws {}
}
```
> Note: `RanFlag` and `poll(until:)` already live in `Tests/AuthTests/AuthTestSupport.swift` (created in Task 16), so this file uses them directly — no declarations to move.

- [ ] **Step 2: Run the integration test**

Run: `swift test --filter IntegrationTests`
Expected: PASS (1 test).

- [ ] **Step 3: Run the full suite**

Run: `swift test`
Expected: ALL tests pass.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test: end-to-end gating + post-login integration with worked example"
```

---

## Self-Review

**Spec coverage check** (each spec section → task):

- Two pipelines, shared engine → Tasks 7, 14, 16, 17.
- Operation contract (type-as-identity, dependsOn, failurePolicy, shouldRun, run) → Task 4.
- Type erasure / `AnyOutcomeBox` trapping cast → Task 3.
- `AnyOperation` + typed skipped/failed factories → Task 5.
- Registration & per-pipeline graph validation (unregistered, cycle, cross-pipeline edge) → Task 11.
- Scheduling: structured `addTask` children + engine-fulfilled gates → Tasks 6, 7.
- Per-op lifecycle (wait, default cascade, guard, run, fail-policy) → Tasks 7, 8, 9.
- Default-skip cascade + guard override + diamond → Tasks 4, 8.
- Override re-arms fail-close → covered by Task 9 mechanics (a `.close` op that runs after override aborts); pinned implicitly by FailPolicy tests.
- Per-op timeout (fail-open prune / fail-close abort) → Task 10.
- Data flow typed-value-via-context, fan-out/diamond reads → Tasks 4, 7, 12.
- Context lifecycle (session-scoped, shared, cleared at session end) → Tasks 13 (broker), 16 (coordinator clears).
- Write-boundary invariant + security test → Tasks 6 (scope ownership), 16 (test).
- Re-run staging + atomic publish → Tasks 6, 12.
- Concurrency/cancellation (actor engine, structured scope) → Tasks 6, 7, 9.
- Error surfacing (engine error → `AuthError` on state) → Tasks 13, 15.
- Gating return contract (`GatingResult`) → Tasks 13, 14, 15.
- Post-login trigger via `AsyncStream` observation → Task 16.
- Testing strategy (engine isolation, validation, auth module, coordinator, lifecycle, write-boundary, re-run atomicity) → Tasks 7–12, 15, 16, 17.

One intentional deferral: the spec notes a pipeline-level *overall* deadline ("a pipeline **may** also carry an overall deadline"). Only the per-operation timeout is implemented (Task 10). The spec hedges this as optional; a pipeline-wide deadline can be added later by wrapping `OperationEngine.run` in the same AsyncStream-race pattern. No other gaps.

**Placeholder scan:** No "TBD"/"TODO"/"handle edge cases" present; all code blocks are complete. The one intentionally-untested path (trap-on-type-mismatch in Task 3) is documented as deliberate, not a placeholder.

**Type consistency check:** `buildPipeline` / `Pipeline` / `RunResult` / `RunResult.isCompleted` / `AnyOutcomeBox.kind` / `.failureError` / `.unwrap(as:)` / `makeSkipped(reason:)` / `makeFailed(_:)` / `SessionScope.beginRun`/`complete`/`awaitOutcome`/`snapshot`/`liveSnapshot`/`publish`/`abort`/`isAborted` / `OperationEngine.run(_:scope:)` / `GatingResult` / `SessionScopeBroker.beginSession`/`currentSession`/`endSession` / `AuthState` / `AuthError(failingOperation:underlying:)` are used identically across all tasks. `Probe`/`OpA..OpD`, `RunLog`, `Counter`, `RanFlag`, `Gate` test helpers are defined once and reused consistently (with the noted `RanFlag` relocation in Task 17).

**Scope check:** Single cohesive engine + auth integration; one implementation plan is appropriate.
