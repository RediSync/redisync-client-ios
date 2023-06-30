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
	
	func testAppendAppendsStringValue() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key = UUID().uuidString
		let string1 = "Hello"
		let string2 = " World"
		let expectedString = "\(string1)\(string2)"
		
		let append1Result = await client.append(key: key, value: string1)
		XCTAssertEqual(append1Result, string1.count)
		
		let append2Result = await client.append(key: key, value: string2)
		XCTAssertEqual(append2Result, expectedString.count)
		
		let result = await client.get(key: key)
		XCTAssertEqual(result, expectedString)
	}
	
	func testCopyCopiesValueFromSourceToDestination() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.set(key: key1, value: "sheep")
		
		let copy1Result = await client.copy(source: key1, destination: key2)
		XCTAssertTrue(copy1Result)
		
		let get1Result = await client.get(key: key2)
		XCTAssertEqual(get1Result, "sheep")
		
		await client.set(key: key1, value: "baah")
		
		let copy2Result = await client.copy(source: key1, destination: key2)
		XCTAssertFalse(copy2Result)
		
		let get2Result = await client.get(key: key2)
		XCTAssertEqual(get2Result, "sheep")
		
		let copy3Result = await client.copy(source: key1, destination: key2, replace: true)
		XCTAssertTrue(copy3Result)
		
		let get3Result = await client.get(key: key2)
		XCTAssertEqual(get3Result, "baah")
	}
	
	func testDecrDecrementsAValue() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		let key2 = UUID().uuidString

		await client.set(key: key1, value: 10)
		
		let decr1Result = await client.decr(key: key1)
		XCTAssertEqual(decr1Result, 9)
		
		let decr2Result = await client.decr(key: key2)
		XCTAssertEqual(decr2Result, -1)
		
		let key1ActualValue = await client.getInt(key: key1)
		XCTAssertEqual(key1ActualValue, 9)

		let key2ActualValue = await client.getInt(key: key2)
		XCTAssertEqual(key2ActualValue, -1)
	}
	
	func testDecrbyDecrementsValueBySpecificAmount() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		
		await client.set(key: key1, value: 10)
		
		let decrby1Result = await client.decrby(key: key1, decrement: 3)
		XCTAssertEqual(decrby1Result, 7)
	}
	
	func testExistsReturnsNumberOfKeysThatHaveValue() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString

		await client.set(key: key1, value: "Hello")
		await client.set(key: key2, value: "World")

		let exists1Result = await client.exists(key1)
		XCTAssertEqual(exists1Result, 1)
		
		let exists2Result = await client.exists("no-such-key")
		XCTAssertEqual(exists2Result, 0)
		
		let exists3Result = await client.exists(key1, key2, "no-such-key")
		XCTAssertEqual(exists3Result, 2)
	}
	
	func testExpireSetsATimeoutOnAKey() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.set(key: key1, value: "Hello")

		let expire1Result = await client.expire(key: key1, seconds: 10)
		XCTAssertTrue(expire1Result)
		
		let ttl1 = await client.ttl(key: key1)
		XCTAssertEqual(ttl1, 10)
		
		await client.set(key: key1, value: "World")
		
		let ttl2 = await client.ttl(key: key1)
		XCTAssertEqual(ttl2, -1)
		
		let expire2Result = await client.expire(key: key1, seconds: 10, expireToken: .XX)
		XCTAssertFalse(expire2Result)
		
		let ttl3 = await client.ttl(key: key1)
		XCTAssertEqual(ttl3, -1)
		
		let expire3Result = await client.expire(key: key1, seconds: 10, expireToken: .NX)
		XCTAssertTrue(expire3Result)

		let ttl4 = await client.ttl(key: key1)
		XCTAssertEqual(ttl4, 10)
	}
	
	func testExpireAtHasSameEffectAsExpireButTakesUnixTimestamp() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.set(key: key1, value: "Hello")
		
		let existsBeforeExpiration = await client.exists(key1)
		XCTAssertEqual(existsBeforeExpiration, 1)
		
		let expireResult = await client.expireat(key: key1, unixTimeSeconds: 1293840000)
		XCTAssertTrue(expireResult)
		
		let doesntExistAfterExpiration = await client.exists(key1)
		XCTAssertEqual(doesntExistAfterExpiration, 0)
	}
	
	func testExpireTimeReturnsTheAbsoluteUnixTimestampInSeconds() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.set(key: key1, value: "Hello")
		
		let expireTime1Result = await client.expiretime(key: key1)
		XCTAssertEqual(expireTime1Result, -1)
		
		let expectedExpireTime = 33177117420
		
		let expireAtResult = await client.expireat(key: key1, unixTimeSeconds: expectedExpireTime)
		XCTAssertTrue(expireAtResult)
		
		let expireTime2Result = await client.expiretime(key: key1)
		XCTAssertEqual(expireTime2Result, expectedExpireTime)
		
		let expireTime3Result = await client.expiretime(key: key2)
		XCTAssertEqual(expireTime3Result, -2)
	}
}
