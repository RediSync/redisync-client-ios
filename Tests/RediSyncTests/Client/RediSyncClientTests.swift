import XCTest
@testable import RediSync

@available(macOS 13.0, *)
final class RediSyncClientTests: XCTestCase
{
	func testConnect() async throws {
		let client = RediSyncClient(appKey: "qh73BqoXEbX725hIHvGvQRdbJptHpIRJkD9zuFE6")
		
		let didConnect = await client.connect()
		
		XCTAssertEqual(didConnect, true)
		XCTAssertEqual(client.status, .connected)
	}
}
