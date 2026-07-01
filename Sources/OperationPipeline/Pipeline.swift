import Foundation

public struct Pipeline: Sendable {
    let operations: [AnyOperation]
    init(validated operations: [AnyOperation]) {
        self.operations = operations
    }
}

public enum PipelineValidationError: Error, Equatable {
    case duplicateType(String)
    case unregisteredDependency(operation: String, missing: String)
    case cycle([String])
    /// An `awaitsExternalInput` op could be abort-cancelled mid-suspension: a
    /// fail-close op is a concurrent sibling (incomparable in the DAG), so its
    /// failure would `cancelAll()` while the interactive op is parked on a
    /// signal it cannot free itself from. Make `racing` a transitive ancestor or
    /// descendant of `interactive`, or give the interactive op cancellation
    /// handling.
    case interactiveOperationRacesFailClose(interactive: String, racing: String)
}

/// Build a validated pipeline. Fails fast (at startup) on a duplicate op type,
/// a dependency on an unregistered type (this also rejects cross-pipeline edges,
/// since the other pipeline's ops are not registered here), or a cycle.
public func buildPipeline(_ operations: [AnyOperation]) throws -> Pipeline {
    // 1. Duplicate detection by type identity.
    var byID: [ObjectIdentifier: AnyOperation] = [:]
    for op in operations {
        if byID[op.id] != nil {
            throw PipelineValidationError.duplicateType(op.typeName)
        }
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
    var state: [ObjectIdentifier: Int] = [:] // 0=unvisited, 1=on-stack, 2=done
    var stack: [String] = []
    func visit(_ id: ObjectIdentifier) throws {
        switch state[id] ?? 0 {
        case 2: return
        case 1: throw PipelineValidationError.cycle(stack)
        default: break
        }
        guard let op = byID[id] else {
            return
        }
        state[id] = 1
        stack.append(op.typeName)
        for dep in op.dependsOn {
            try visit(dep)
        }
        stack.removeLast()
        state[id] = 2
    }
    for op in operations {
        try visit(op.id)
    }

    // 4. Interactive-op invariant. An op that suspends on external input cannot
    // free itself if abort-cancelled, so no fail-close op may run concurrently
    // with it. Concurrent == incomparable in the DAG partial order, so every
    // fail-close op must be a transitive ancestor or descendant of it.
    func ancestors(of start: ObjectIdentifier) -> Set<ObjectIdentifier> {
        var seen: Set<ObjectIdentifier> = []
        var stack = byID[start]?.dependsOn ?? []
        // Every dependency is registered (validated in step 2), so byID lookups
        // below always resolve.
        while let id = stack.popLast() {
            if seen.insert(id).inserted {
                stack.append(contentsOf: byID[id]?.dependsOn ?? [])
            }
        }
        return seen
    }

    for interactive in operations where interactive.awaitsExternalInput {
        let upstream = ancestors(of: interactive.id)
        for failClose in operations
            where failClose.failurePolicy == .close && failClose.id != interactive.id {
            let comparable = upstream.contains(failClose.id)
                || ancestors(of: failClose.id).contains(interactive.id)
            if !comparable {
                throw PipelineValidationError.interactiveOperationRacesFailClose(
                    interactive: interactive.typeName, racing: failClose.typeName
                )
            }
        }
    }

    return Pipeline(validated: operations)
}
