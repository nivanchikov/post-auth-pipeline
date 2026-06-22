// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "Auth",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "OperationPipeline", targets: ["OperationPipeline"]),
        .library(name: "Auth", targets: ["Auth"])
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
        )
    ]
)
