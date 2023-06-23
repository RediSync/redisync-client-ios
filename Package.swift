// swift-tools-version:5.8

import PackageDescription

let package = Package(
	name: "RediSync",
	products: [
		.library(
			name: "RediSync",
			targets: ["RediSync"]
		),
    ],
	dependencies: [
		.package(
			url: "https://github.com/socketio/socket.io-client-swift",
			.upToNextMinor(from: "16.0.0")
		)
	],
	targets: [
		.target(
			name: "RediSync",
			dependencies: [
				.product(name: "SocketIO", package: "socket.io-client-swift")
			]
		),
		.testTarget(
			name: "RediSyncTests",
			dependencies: ["RediSync"]
		),
	]
)
