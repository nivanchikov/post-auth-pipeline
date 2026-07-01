import Foundation

/// Set-once flag a post-login op flips when it runs.
actor RanFlag { private(set) var value = false
    func set() {
        value = true
    }
}

/// Poll `condition` until it becomes true, yielding between attempts. Returns
/// false if the bound is exhausted. Deterministic and CI-safe — no fixed sleep
/// window to lose a race against.
func poll(
    until condition: @Sendable () async -> Bool,
    attempts: Int = 10000
) async -> Bool {
    for _ in 0 ..< attempts {
        if await condition() {
            return true
        }
        await Task.yield()
    }
    return false
}
