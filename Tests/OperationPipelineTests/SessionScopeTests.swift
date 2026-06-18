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
