import XCTest
@testable import RediSync

@available(macOS 13.0, *)
final class RediSyncClientTests: XCTestCase
{
	func testSuccessfulConnect() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		XCTAssertEqual(client.status, .connected)
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
	
	func testGetReturnsNilForBadKey() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let value = await client.get(key: "bad-key")

		XCTAssertNil(value)
	}
	
	func testGetAutomaticallyConnectsClient() async throws {
		let client = try await RediSyncTestClientFactory.create(doConnect: false)
		
		let value = await client.get(key: "should-automatically-connect")
		
		XCTAssertNil(value)
		XCTAssertEqual(client.status, .connected)
	}
	
	func testGetIntReturnsNumericValue() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		let expectedValue = Int.random(in: 0...100000)
		
		await client.set(key: key1, value: expectedValue)
		await client.set(key: key2, value: String(expectedValue))
		
		let actualValue1 = await client.getInt(key: key1)
		let actualValue2 = await client.getInt(key: key2)
		
		XCTAssertEqual(actualValue1, expectedValue)
		XCTAssertEqual(actualValue2, expectedValue)
	}
	
	func testGetIntReturnsNilForStringValue() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key = UUID().uuidString
		
		await client.set(key: key, value: "string-value")
		
		let actualValue = await client.getInt(key: key)
		
		XCTAssertNil(actualValue)
	}
	
	func testDelReturnsZeroIfKeyDoesntExist() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key = UUID().uuidString
		
		let result = await client.del(key)
		
		XCTAssertEqual(result, 0)
	}
	
	func testDelDeletesSingleKeyIfItExists() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key = UUID().uuidString

		await client.set(key: key, value: UUID().uuidString)
		
		let result = await client.del(key)
		
		XCTAssertEqual(result, 1)
	}
	
	func testDelDeletesMultipleKeysIfTheyExist() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		let key3 = UUID().uuidString
		
		await client.set(key: key1, value: UUID().uuidString)
		await client.set(key: key2, value: Int.random(in: 0...100000))
		
		let result = await client.del(key1, key2, key3)
		
		XCTAssertEqual(result, 2)
	}
	
	func testKeysReturnsAllKeysForAsteriskPattern() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let oldKeys = await client.keys(pattern: "*")
		if oldKeys.count > 0 {
			await client.del(oldKeys)
		}
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		let key3 = UUID().uuidString
		
		await client.set(key: key1, value: UUID().uuidString)
		await client.set(key: key2, value: Int.random(in: 0...100000))
		await client.set(key: key3, value: UUID().uuidString)
		
		let result = await client.keys(pattern: "*")
		
		XCTAssertEqual(result.count, 3)
		XCTAssertTrue(result.contains { $0 == key1 })
		XCTAssertTrue(result.contains { $0 == key2 })
		XCTAssertTrue(result.contains { $0 == key3 })
	}
}
