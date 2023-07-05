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
	
	func testGetDelGetsTheValueOfAKeyAndDeletesTheKey() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		let expectedValue = "Hello"
		
		await client.set(key: key1, value: "Hello")
		
		let getDel1Result = await client.getdel(key: key1)
		XCTAssertEqual(getDel1Result, expectedValue)
		
		let get1Result = await client.get(key: key1)
		XCTAssertNil(get1Result)
	}
	
	func testGetExGetsTheValueOfAKeyAndOptionallySetsItExpiration() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		let expectedValue = "Hello"

		await client.set(key: key1, value: expectedValue)

		let getEx1Result = await client.getex(key: key1)
		XCTAssertEqual(getEx1Result, expectedValue)
		
		let ttl1Result = await client.ttl(key: key1)
		XCTAssertEqual(ttl1Result, -1)
		
		let getEx2Result = await client.getex(key: key1, expiration: .EX(seconds: 60))
		XCTAssertEqual(getEx2Result, expectedValue)

		let ttl2Result = await client.ttl(key: key1)
		XCTAssertEqual(ttl2Result, 60)
	}
	
	func testGetRangeGetsTheSubstringValueStoredAtKey() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		let expectedValue = "This is a string"

		await client.set(key: key1, value: expectedValue)
		
		let getRange1Result = await client.getrange(key: key1, start: 0, end: 3)
		XCTAssertEqual(getRange1Result, "This")
		
		let getRange2Result = await client.getrange(key: key1, start: -3, end: -1)
		XCTAssertEqual(getRange2Result, "ing")
		
		let getRange3Result = await client.getrange(key: key1, start: 0, end: -1)
		XCTAssertEqual(getRange3Result, "This is a string")
		
		let getRange4Result = await client.getrange(key: key1, start: 10, end: 100)
		XCTAssertEqual(getRange4Result, "string")
	}
	
	func testHDelRemovesTheSpecifiedFieldsStoredAtKey() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		let hset1 = await client.hset(key: key1, fieldValues: ("field1", "foo"))
		XCTAssertEqual(hset1, 1)
		
		let hdel1 = await client.hdel(key: key1, fields: "field1")
		XCTAssertEqual(hdel1, 1)
		
		let hdel2 = await client.hdel(key: key1, fields: "field1")
		XCTAssertEqual(hdel2, 0)
		
		let hdel3 = await client.hdel(key: key1, fields: "field2")
		XCTAssertEqual(hdel3, 0)
	}
	
	func testHExistsReturnsIfFieldExistsInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		
		await client.hset(key: key1, fieldValues: ("field1", "foo"))

		let hexists1 = await client.hexists(key: key1, field: "field1")
		XCTAssertTrue(hexists1)

		let hexists2 = await client.hexists(key: key1, field: "field2")
		XCTAssertFalse(hexists2)
	}
	
	func testHGetReturnsValueAsssociatedWithFieldInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		
		await client.hset(key: key1, fieldValues: ("field1", "foo"))
		
		let hget1 = await client.hget(key: key1, field: "field1")
		XCTAssertEqual(hget1, "foo")
		
		let hget2 = await client.hget(key: key1, field: "field2")
		XCTAssertNil(hget2)
	}
	
	func testHGetAllReturnsAllFieldsAndValuesInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.hset(key: key1, fieldValues: ("field1", "Hello"))
		await client.hset(key: key1, fieldValues: ("field2", "World"))
		await client.hset(key: key1, fieldValues: ("field3", 13))

		let hgetall1 = await client.hgetall(key: key1)
		XCTAssertEqual(hgetall1.count, 3)
		XCTAssertEqual(hgetall1["field1"], "Hello")
		XCTAssertEqual(hgetall1["field2"], "World")
		XCTAssertEqual(hgetall1["field3"], "13")
		
		let hgetall2 = await client.hgetall(key: key2)
		XCTAssertEqual(hgetall2.count, 0)
	}
	
	func testHIncrbyIncrementsNumberStoredAtFieldInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		
		await client.hset(key: key1, fieldValues: ("field", 5))

		let hincrby1 = await client.hincrby(key: key1, field: "field", increment: 1)
		XCTAssertEqual(hincrby1, 6)

		let hincrby2 = await client.hincrby(key: key1, field: "field", increment: -1)
		XCTAssertEqual(hincrby2, 5)

		let hincrby3 = await client.hincrby(key: key1, field: "field", increment: -10)
		XCTAssertEqual(hincrby3, -5)
	}
	
	func testHIncrbyfloatIncrementsFloatNumberStoredAtFieldInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		
		await client.hset(key: key1, fieldValues: ("field", 10.50))

		let hincrbyfloat1 = await client.hincrbyfloat(key: key1, field: "field", increment: 0.1)
		XCTAssertEqual(hincrbyfloat1, 10.6)

		let hincrbyfloat2 = await client.hincrbyfloat(key: key1, field: "field", increment: -5)
		XCTAssertEqual(hincrbyfloat2, 5.6)
	}
	
	func testHKeysReturnsAllFieldNamesInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.hset(key: key1, field: "field1", value: "Hello")
		await client.hset(key: key1, field: "field2", value: "World")
		
		let hkeys1 = await client.hkeys(key: key1)
		XCTAssertEqual(hkeys1.count, 2)
		XCTAssertTrue(hkeys1.contains { $0 == "field1" })
		XCTAssertTrue(hkeys1.contains { $0 == "field2" })
	}
	
	func testHLenReturnsNumberOfFieldsInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.hset(key: key1, field: "field1", value: "Hello")
		await client.hset(key: key1, field: "field2", value: "World")
		
		let hlen1 = await client.hlen(key: key1)
		XCTAssertEqual(hlen1, 2)
	}
	
	func testHMGetReturnsValuesAssociatedWithSpecifiedFieldsInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.hset(key: key1, field: "field1", value: "Hello")
		await client.hset(key: key1, field: "field2", value: "World")
		
		let hmget1 = await client.hmget(key: key1, fields: "field1", "field2", "nofield")
		XCTAssertEqual(hmget1.count, 3)
		XCTAssertEqual(hmget1[0], "Hello")
		XCTAssertEqual(hmget1[1], "World")
		XCTAssertNil(hmget1[2])
	}
	
	func testHSetSetsTheSpecifiedFieldsInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		let hset1 = await client.hset(key: key1, field: "field1", value: "Hello")
		XCTAssertTrue(hset1)
		
		let hget1 = await client.hget(key: key1, field: "field1")
		XCTAssertEqual(hget1, "Hello")
		
		let hset2 = await client.hset(key: key1, fieldValues: ("field2", "Hi"), ("field3", "World"))
		XCTAssertEqual(hset2, 2)
		
		let hget2 = await client.hget(key: key1, field: "field2")
		XCTAssertEqual(hget2, "Hi")
		
		let hget3 = await client.hget(key: key1, field: "field3")
		XCTAssertEqual(hget3, "World")
		
		let hgetall1 = await client.hgetall(key: key1)
		XCTAssertEqual(hgetall1.count, 3)
		XCTAssertEqual(hgetall1["field1"], "Hello")
		XCTAssertEqual(hgetall1["field2"], "Hi")
		XCTAssertEqual(hgetall1["field3"], "World")
	}
	
	func testHSetNXOnlySetsFieldToValueIfFieldDoesntExistInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		
		let hsetnx1 = await client.hsetnx(key: key1, field: "field", value: "Hello")
		XCTAssertTrue(hsetnx1)
		
		let hsetnx2 = await client.hsetnx(key: key1, field: "field", value: "World")
		XCTAssertFalse(hsetnx2)
		
		let hget1 = await client.hget(key: key1, field: "field")
		XCTAssertEqual(hget1, "Hello")
	}
	
	func testHStrLenReturnsLengthOfStringAtFieldOnHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.hset(key: key1, fieldValues: ("f1", "HelloWorld"), ("f2", 99), ("f3", -256))
		
		let hstrlen1 = await client.hstrlen(key: key1, field: "f1")
		XCTAssertEqual(hstrlen1, 10)
		
		let hstrlen2 = await client.hstrlen(key: key1, field: "f2")
		XCTAssertEqual(hstrlen2, 2)
		
		let hstrlen3 = await client.hstrlen(key: key1, field: "f3")
		XCTAssertEqual(hstrlen3, 4)
	}
	
	func testHValsReturnsAllValuesInHash() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.hset(key: key1, field: "field1", value: "Hello")
		await client.hset(key: key1, field: "field2", value: "World")

		let hvals1 = await client.hvals(key: key1)
		XCTAssertEqual(hvals1.count, 2)
		XCTAssertTrue(hvals1.contains { $0 == "Hello" })
		XCTAssertTrue(hvals1.contains { $0 == "World" })
	}
	
	func testIncrIncrementsTheValueAtKeyByOne() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.set(key: key1, value: 10)
		
		let incr1 = await client.incr(key: key1)
		XCTAssertEqual(incr1, 11)
		
		let get1 = await client.get(key: key1)
		XCTAssertEqual(get1, "11")
	}
	
	func testIncrbyIncrementsTheValueAtKeyBySpecifiedAmount() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString

		await client.set(key: key1, value: 10)
		
		let incrby1 = await client.incrby(key: key1, increment: 5)
		XCTAssertEqual(incrby1, 15)
	}
	
	func testIncrbyfloatIncrementsTheFloatValueAtKeyBySpecifiedAmount() async throws {
		let client = try await RediSyncTestClientFactory.create()

		let key1 = UUID().uuidString
		
		await client.set(key: key1, value: 10.5)
		
		let incrbyfloat1 = await client.incrbyfloat(key: key1, increment: 0.1)
		XCTAssertEqual(incrbyfloat1, 10.6)
		
		let incrbyfloat2 = await client.incrbyfloat(key: key1, increment: -5)
		XCTAssertEqual(incrbyfloat2, 5.6)
	}
	
	func testLIndexReturnsTheElementAtIndexInList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		await client.lpush(key: key1, elements: "World")
		await client.lpush(key: key1, elements: "Hello")
		
		let lindex1 = await client.lindex(key: key1, index: 0)
		XCTAssertEqual(lindex1, "Hello")
		
		let lindex2 = await client.lindex(key: key1, index: -1)
		XCTAssertEqual(lindex2, "World")
		
		let lindex3 = await client.lindex(key: key1, index: 3)
		XCTAssertNil(lindex3)
	}
	
	func testLInsertInsertsElementInListBeforeOrAfterPivotPoint() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		await client.rpush(key: key1, elements: "Hello")
		await client.rpush(key: key1, elements: "World")
		
		let linsert1 = await client.linsert(key: key1, beforeOrAfter: .before, pivot: "World", element: "There")
		XCTAssertEqual(linsert1, 3)
		
		let items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(items.count, 3)
		XCTAssertEqual(items[0], "Hello")
		XCTAssertEqual(items[1], "There")
		XCTAssertEqual(items[2], "World")
	}
	
	func testLLenReturnsTheLengthOfAList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		await client.lpush(key: key1, elements: "World")
		await client.lpush(key: key1, elements: "Hello")
		
		let llen1 = await client.llen(key: key1)
		XCTAssertEqual(llen1, 2)
	}
	
	func testLMoveReturnsAndRemovesTheFirstOrLastElement() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString

		await client.rpush(key: key1, elements: "one")
		await client.rpush(key: key1, elements: "two")
		await client.rpush(key: key1, elements: "three")
		
		let lmove1 = await client.lmove(source: key1, destination: key2, sourceLeftOrRight: .right, destinationLeftOrRight: .left)
		let lmove2 = await client.lmove(source: key1, destination: key2, sourceLeftOrRight: .left, destinationLeftOrRight: .right)
		
		XCTAssertEqual(lmove1, "three")
		XCTAssertEqual(lmove2, "one")
		
		let key1Items = await client.lrange(key: key1, start: 0, stop: -1)
		let key2Items = await client.lrange(key: key2, start: 0, stop: -1)
		
		XCTAssertEqual(key1Items.count, 1)
		XCTAssertEqual(key1Items[0], "two")
		
		XCTAssertEqual(key2Items.count, 2)
		XCTAssertEqual(key2Items[0], "three")
		XCTAssertEqual(key2Items[1], "one")
	}
	
	func testLPopRemovesAndReturnsTheFirstElementsOfAList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		await client.rpush(key: key1, elements: "one", "two", "three", "four", "five")
		
		let lpop1 = await client.lpop(key: key1)
		XCTAssertEqual(lpop1, "one")
		
		let lpop2 = await client.lpop(key: key1, count: 2)
		XCTAssertEqual(lpop2.count, 2)
		XCTAssertEqual(lpop2[0], "two")
		XCTAssertEqual(lpop2[1], "three")
		
		let remainingItems = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(remainingItems.count, 2)
		XCTAssertEqual(remainingItems[0], "four")
		XCTAssertEqual(remainingItems[1], "five")
		
		let lpop3 = await client.lpop(key: key1, count: 1)
		XCTAssertEqual(lpop3.count, 1)
		XCTAssertEqual(lpop3[0], "four")
	}
	
	func testLPushInsertsElementsAtTheHeadOfAList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		let lpush1 = await client.lpush(key: key1, elements: "world")
		XCTAssertEqual(lpush1, 1)

		let lpush2 = await client.lpush(key: key1, elements: "hello")
		XCTAssertEqual(lpush2, 2)
		
		let items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(items.count, 2)
		XCTAssertEqual(items[0], "hello")
		XCTAssertEqual(items[1], "world")
	}
	
	func testLPushXInsertsElementAtTheHeadOfAListOnlyIfKeyAlreadyExistsAndHoldsAList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.lpush(key: key1, elements: "World")
		
		let lpushx1 = await client.lpushx(key: key1, elements: "Hello")
		XCTAssertEqual(lpushx1, 2)

		let lpushx2 = await client.lpushx(key: key2, elements: "Hello")
		XCTAssertEqual(lpushx2, 0)
		
		let key1Items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(key1Items.count, 2)
		XCTAssertEqual(key1Items[0], "Hello")
		XCTAssertEqual(key1Items[1], "World")
		
		let key2Items = await client.lrange(key: key2, start: 0, stop: -1)
		XCTAssertEqual(key2Items.count, 0)
	}
	
	func testLRangeReturnsTheSpecifiedElementsOfAList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		await client.rpush(key: key1, elements: "one", "two", "three")
		
		let lrange1 = await client.lrange(key: key1, start: 0, stop: 0)
		XCTAssertEqual(lrange1.count, 1)
		XCTAssertEqual(lrange1[0], "one")
		
		let lrange2 = await client.lrange(key: key1, start: -3, stop: 2)
		XCTAssertEqual(lrange2.count, 3)
		XCTAssertEqual(lrange2[0], "one")
		XCTAssertEqual(lrange2[1], "two")
		XCTAssertEqual(lrange2[2], "three")
		
		let lrange3 = await client.lrange(key: key1, start: -100, stop: 100)
		XCTAssertEqual(lrange3.count, 3)
		XCTAssertEqual(lrange3[0], "one")
		XCTAssertEqual(lrange3[1], "two")
		XCTAssertEqual(lrange3[2], "three")
		
		let lrange4 = await client.lrange(key: key1, start: 5, stop: 10)
		XCTAssertEqual(lrange4.count, 0)
	}
	
	func testLRemRemoveTheFirstOccurrencesOfElementFromList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		await client.rpush(key: key1, elements: "hello", "hello", "foo", "hello")
		
		let lrem1 = await client.lrem(key: key1, count: -2, element: "hello")
		XCTAssertEqual(lrem1, 2)
		
		let items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(items.count, 2)
		XCTAssertEqual(items[0], "hello")
		XCTAssertEqual(items[1], "foo")
	}
	
	func testLSetSetsTheListElementAtIndex() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		await client.rpush(key: key1, elements: "one", "two", "three")
		
		let lset1 = await client.lset(key: key1, index: 0, element: "four")
		XCTAssertTrue(lset1)

		let lset2 = await client.lset(key: key1, index: -2, element: "five")
		XCTAssertTrue(lset2)
		
		let items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(items.count, 3)
		XCTAssertEqual(items[0], "four")
		XCTAssertEqual(items[1], "five")
		XCTAssertEqual(items[2], "three")
	}
	
	func testLTrimTrimsAnExistingListToOnlyContainASpecifiedRange() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		await client.rpush(key: key1, elements: "one", "two", "three")

		let ltrim1 = await client.ltrim(key: key1, start: 1, stop: -1)
		XCTAssertTrue(ltrim1)
		
		let items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(items.count, 2)
		XCTAssertEqual(items[0], "two")
		XCTAssertEqual(items[1], "three")
	}
	
	func testRPopRemovesAndReturnsTheLastElementOfAList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		await client.rpush(key: key1, elements: "one", "two", "three", "four", "five")
		
		let rpop1 = await client.rpop(key: key1)
		XCTAssertEqual(rpop1, "five")
		
		let rpop2 = await client.rpop(key: key1, count: 2)
		XCTAssertEqual(rpop2.count, 2)
		XCTAssertEqual(rpop2[0], "four")
		XCTAssertEqual(rpop2[1], "three")
		
		let items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(items.count, 2)
		XCTAssertEqual(items[0], "one")
		XCTAssertEqual(items[1], "two")
	}
	
	func testRPushInsertsElementAtTheTailOfList() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		let rpush1 = await client.rpush(key: key1, elements: "hello")
		XCTAssertEqual(rpush1, 1)

		let rpush2 = await client.rpush(key: key1, elements: "world")
		XCTAssertEqual(rpush2, 2)
		
		let items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(items.count, 2)
		XCTAssertEqual(items[0], "hello")
		XCTAssertEqual(items[1], "world")
	}
	
	func testRPushXInsertsElementsAtTheTailOfListOnlyIfListExists() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.rpush(key: key1, elements: "Hello")

		let rpushx1 = await client.rpushx(key: key1, elements: "World")
		XCTAssertEqual(rpushx1, 2)

		let rpushx2 = await client.rpushx(key: key2, elements: "World")
		XCTAssertEqual(rpushx2, 0)
		
		let key1Items = await client.lrange(key: key1, start: 0, stop: -1)
		XCTAssertEqual(key1Items.count, 2)
		XCTAssertEqual(key1Items[0], "Hello")
		XCTAssertEqual(key1Items[1], "World")

		let key2Items = await client.lrange(key: key2, start: 0, stop: -1)
		XCTAssertEqual(key2Items.count, 0)
	}
	
	func testSAddAddsMembersToASet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		let sadd1 = await client.sadd(key: key1, members: "Hello")
		XCTAssertEqual(sadd1, 1)

		let sadd2 = await client.sadd(key: key1, members: "World")
		XCTAssertEqual(sadd2, 1)

		let sadd3 = await client.sadd(key: key1, members: "World")
		XCTAssertEqual(sadd3, 0)
		
		let sadd4 = await client.sadd(key: key1, members: "World", "one", "Hello", "two")
		XCTAssertEqual(sadd4, 2)
		
		let members = await client.smembers(key: key1)
		XCTAssertEqual(members.count, 4)
		XCTAssertTrue(members.contains("Hello"))
		XCTAssertTrue(members.contains("World"))
		XCTAssertTrue(members.contains("one"))
		XCTAssertTrue(members.contains("two"))
		XCTAssertFalse(members.contains("three"))
	}
	
	func testSCardReturnsTheCountOfMembersInSet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		await client.sadd(key: key1, members: "Hello", "World")
		
		let scard1 = await client.scard(key: key1)
		
		XCTAssertEqual(scard1, 2)
	}
	
	func testSDiffReturnsTheMembersInSetThatDoNotExistInOtherSets() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.sadd(key: key1, members: "a", "b", "c")
		await client.sadd(key: key2, members: "c", "d", "e")
		
		let sdiff1 = await client.sdiff(key: key1, keys: key2)
		XCTAssertEqual(sdiff1.count, 2)
		XCTAssertTrue(sdiff1.contains("a"))
		XCTAssertTrue(sdiff1.contains("b"))
	}
	
	func testSDiffStoreStoresTheMembersThatResultFromTakingTheDifferenceOfSets() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		let destination = UUID().uuidString
		
		await client.sadd(key: key1, members: "a", "b", "c")
		await client.sadd(key: key2, members: "c", "d", "e")

		let sdiffstore1 = await client.sdiffstore(destination: destination, key: key1, keys: key2)
		XCTAssertEqual(sdiffstore1, 2)
		
		let members = await client.smembers(key: destination)
		XCTAssertEqual(members.count, 2)
		XCTAssertTrue(members.contains("a"))
		XCTAssertTrue(members.contains("b"))
	}
	
	func testSetRangeOverridesPartOfString() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.set(key: key1, value: "Hello World")
		
		let setrange1 = await client.setrange(key: key1, offset: 6, value: "Redis")
		XCTAssertEqual(setrange1, 11)
		
		let value1 = await client.get(key: key1)
		XCTAssertEqual(value1, "Hello Redis")
		
		let setrange2 = await client.setrange(key: key2, offset: 6, value: "Redis")
		XCTAssertEqual(setrange2, 11)
		
		let value2 = await client.get(key: key2)
		XCTAssertEqual(value2, "\0\0\0\0\0\0Redis")
	}
	
	func testSInterReturnsTheMembersOfASetThatIntersectWithAGivenSet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.sadd(key: key1, members: "a", "b", "c")
		await client.sadd(key: key2, members: "c", "d", "e")

		let sinter1 = await client.sinter(key: key1, keys: key2)
		XCTAssertEqual(sinter1.count, 1)
		XCTAssertTrue(sinter1.contains("c"))
	}
	
	func testSIntercardReturnsTheNumberOfMembersResultingFromTheIntersectionOfSets() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.sadd(key: key1, members: "a", "b", "c", "d")
		await client.sadd(key: key2, members: "c", "d", "e")
		
		let sintercard1 = await client.sintercard(key: key1, keys: key2)
		XCTAssertEqual(sintercard1, 2)
	}
	
	func testSInterstoreStoresTheIntersectionOfSets() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		let destination = UUID().uuidString
		
		await client.sadd(key: key1, members: "a", "b", "c")
		await client.sadd(key: key2, members: "c", "d", "e")

		let sinterstore1 = await client.sinterstore(destination: destination, key: key1, keys: key2)
		XCTAssertEqual(sinterstore1, 1)
		
		let members = await client.smembers(key: destination)
		XCTAssertEqual(members.count, 1)
		XCTAssertTrue(members.contains("c"))
	}
	
	func testSIsmemberReturnsTrueIfMemberIsPartOfSet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		await client.sadd(key: key1, members: "one")

		let sismember1 = await client.sismember(key: key1, member: "one")
		XCTAssertTrue(sismember1)

		let sismember2 = await client.sismember(key: key1, member: "two")
		XCTAssertFalse(sismember2)
	}
	
	func testSMembersReturnsAllTheMembersOfASet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		await client.sadd(key: key1, members: "Hello", "World")
		
		let members = await client.smembers(key: key1)
		XCTAssertEqual(members.count, 2)
		XCTAssertTrue(members.contains("Hello"))
		XCTAssertTrue(members.contains("World"))
		XCTAssertFalse(members.contains("Redis"))
	}
	
	func testSMIsmemberReturnsWhetherMultipleMembersArePartOfSet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		
		await client.sadd(key: key1, members: "one")

		let smismember1 = await client.smismember(key: key1, members: "one", "two")
		XCTAssertEqual(smismember1.count, 2)
		XCTAssertTrue(smismember1[0])
		XCTAssertFalse(smismember1[1])
	}
	
	func testSMoveMovesMemberFromSourceSetToDestination() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		
		await client.sadd(key: key1, members: "one", "two")
		await client.sadd(key: key2, members: "three")

		let smove1 = await client.smove(source: key1, destination: key2, member: "two")
		XCTAssertTrue(smove1)
		
		let key1Members = await client.smembers(key: key1)
		XCTAssertEqual(key1Members.count, 1)
		XCTAssertTrue(key1Members.contains("one"))
		
		let key2Members = await client.smembers(key: key2)
		XCTAssertEqual(key2Members.count, 2)
		XCTAssertTrue(key2Members.contains("two"))
		XCTAssertTrue(key2Members.contains("three"))
	}
	
	func testSPopRemovesAndReturnsOneOrMoreRandomElementsFromSet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		var members: Set<String> = ["one", "two", "three"]
		
		await client.sadd(key: key1, members: Array(members))
		
		let poppedMember = await client.spop(key: key1)
		XCTAssertNotNil(poppedMember)
		XCTAssertTrue(members.contains(poppedMember!))
		
		let membersAfterPop = await client.smembers(key: key1)
		XCTAssertEqual(membersAfterPop.count, 2)
		XCTAssertFalse(membersAfterPop.contains(poppedMember!))
		
		members.remove(poppedMember!)
		members.insert("four")
		members.insert("five")
		await client.sadd(key: key1, members: "four", "five")
		
		let poppedMembers = await client.spop(key: key1, count: 3)
		XCTAssertEqual(poppedMembers.count, 3)
		
		for member in poppedMembers {
			XCTAssertTrue(member.contains(member))
		}
		
		let membersAfterPop2 = await client.smembers(key: key1)
		XCTAssertEqual(membersAfterPop2.count, 1)
	}
	
	func testSRandmemberReturnsARandomElementFromSet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		var members: Set<String> = ["one", "two", "three"]
		
		await client.sadd(key: key1, members: Array(members))
		
		let randMember = await client.srandmember(key: key1)
		XCTAssertNotNil(randMember)
		XCTAssertTrue(members.contains(randMember!))
		
		let randMembers1 = await client.srandmember(key: key1, count: 2)
		XCTAssertEqual(randMembers1.count, 2)
		XCTAssertTrue(members.contains(randMembers1[0]))
		XCTAssertTrue(members.contains(randMembers1[1]))
		
		let randMembers2 = await client.srandmember(key: key1, count: -5)
		XCTAssertEqual(randMembers2.count, 5)
		XCTAssertTrue(members.contains(randMembers2[0]))
		XCTAssertTrue(members.contains(randMembers2[1]))
		XCTAssertTrue(members.contains(randMembers2[2]))
		XCTAssertTrue(members.contains(randMembers2[3]))
		XCTAssertTrue(members.contains(randMembers2[4]))
	}
	
	func testSRemRemovesSpecifiedMemberFromSet() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString

		await client.sadd(key: key1, members: "one", "two", "three")
		
		let srem1 = await client.srem(key: key1, members: "one")
		XCTAssertEqual(srem1, 1)
		
		let srem2 = await client.srem(key: key1, members: "four")
		XCTAssertEqual(srem2, 0)

		let members = await client.smembers(key: key1)
		XCTAssertEqual(members.count, 2)
		XCTAssertTrue(members.contains("two"))
		XCTAssertTrue(members.contains("three"))
	}
	
	func testSUnionReturnsTheMembersResultingFromTheUnionOfSets() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString

		await client.sadd(key: key1, members: "a", "b", "c")
		await client.sadd(key: key1, members: "c", "d", "e")

		let sunion1 = await client.sunion(key: key1, keys: key2)
		XCTAssertEqual(sunion1.count, 5)
		XCTAssertTrue(sunion1.contains("a"))
		XCTAssertTrue(sunion1.contains("b"))
		XCTAssertTrue(sunion1.contains("c"))
		XCTAssertTrue(sunion1.contains("d"))
		XCTAssertTrue(sunion1.contains("e"))
	}
	
	func testSUnionstoreStoresTheResultOfUnionOfSetsAndReturnsNumberOfMemebers() async throws {
		let client = try await RediSyncTestClientFactory.create()
		
		let key1 = UUID().uuidString
		let key2 = UUID().uuidString
		let destination = UUID().uuidString
		
		await client.sadd(key: key1, members: "a", "b", "c")
		await client.sadd(key: key2, members: "c", "d", "e")

		let sunionstore = await client.sunionstore(destination: destination, key: key1, keys: key2)
		XCTAssertEqual(sunionstore, 5)
		
		let members = await client.smembers(key: destination)
		XCTAssertEqual(members.count, 5)
		XCTAssertTrue(members.contains("a"))
		XCTAssertTrue(members.contains("b"))
		XCTAssertTrue(members.contains("c"))
		XCTAssertTrue(members.contains("d"))
		XCTAssertTrue(members.contains("e"))
	}
}
