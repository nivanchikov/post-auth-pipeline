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
