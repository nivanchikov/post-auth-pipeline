@testable import OperationPipeline
import Testing

private let idA = ObjectIdentifier(Int.self) // arbitrary distinct identities
private let idB = ObjectIdentifier(String.self)

@Test func awaitOutcomeReturnsImmediatelyIfAlreadyComplete() async {
    let scope = RunState()
    await scope.complete(idA, AnyOutcomeBox(Outcome<Int>.success(1)))
    let box = await scope.awaitOutcome(of: idA)
    #expect(box.kind == .success)
}

@Test func awaitOutcomeSuspendsUntilCompleted() async {
    let scope = RunState()
    // Start a waiter before the value exists.
    async let waited = scope.awaitOutcome(of: idB)
    await scope.complete(idB, AnyOutcomeBox(Outcome<String>.success("x")))
    let box = await waited
    #expect(box.kind == .success)
}

@Test func snapshotContainsCompletedOutcomes() async {
    let scope = RunState()
    await scope.complete(idA, AnyOutcomeBox(Outcome<Int>.success(1)))
    let snap = await scope.snapshot()
    #expect(snap[idA]?.kind == .success)
}

@Test func resetClearsOutcomesAndWaiters() async {
    let scope = RunState()
    await scope.complete(idA, AnyOutcomeBox(Outcome<Int>.success(1)))
    await scope.reset()
    let snap = await scope.snapshot()
    #expect(snap[idA] == nil)
}
