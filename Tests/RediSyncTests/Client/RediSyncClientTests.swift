import XCTest
@testable import RediSync

@available(macOS 13.0, *)
final class RediSyncClientTests: XCTestCase
{
	func testSuccessfulConnect() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		XCTAssertEqual(client.status, .connected)
	}
	
	func testGetReturnsNilForBadKey() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let value = await client.get(key: "bad-key")

		XCTAssertEqual(value, nil)
	}

	func testSetCorrectlySetsValue() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key = UUID().uuidString
		let expectedValue = UUID().uuidString

		let setResult = await client.set(key: key, value: expectedValue)
		XCTAssertEqual(setResult, true)

		let actualValue = await client.get(key: key)
		XCTAssertEqual(actualValue, expectedValue)
	}
}
