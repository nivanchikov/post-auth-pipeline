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
