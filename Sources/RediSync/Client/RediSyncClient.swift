//
//  RediSyncClient.swift
//  
//
//  Created by Mike Richards on 6/23/23.
//

import Foundation
import os

@available(macOS 13.0, *)
@objcMembers
open class RediSyncClient: RediSyncEventEmitter
{
	public private(set) var status: RediSyncConnectionStatus = .notConnected
	
	private let appKey: String
	private let logger = Logger(subsystem: "RediSync", category: "RediSyncClient")
	private let primaryApi = RediSyncAPI(url: URL(string: "https://api-dev.redisync.io/")!)
	
	private var api: RediSyncAPI?
	private var sockets: RediSyncSocketManager?
	
	@objc
	public init(appKey: String) {
		self.appKey = appKey
		
		super.init()
	}
	
	@objc
	@discardableResult
	public func connect() async -> Bool {
		guard status != .connected else {
			return true
		}
		
		if status == .connecting {
			await waitForOneOf("connected", "error")
			return status == .connected
		}
		
		status = .connecting
		
		logger.debug("Connecting to API")
		
		let initResult = await primaryApi.initApiCall(appKey: appKey)
		
		if let initResult = initResult, let apiUrl = initResult.apiUrl, let socketUrls = initResult.socketUrls, let key = initResult.key {
			api = RediSyncAPI(url: apiUrl)
			sockets = RediSyncSocketManager(socketUrls: socketUrls, key: key, rs: initResult.rs)
			
			sockets?.on("connected") { [weak self] _ in
				self?.status = .connected
				self?.emit("connected")
			}
			
			sockets?.on("disconnected")  { [weak self] _ in
				self?.status = .notConnected
				self?.emit("disconnected")
			}
			
			return await connect()
		}
		
		status = .notConnected
		
		emit("error", "RediSync Connection Error")
		
		return false
	}
	
	@discardableResult
	public func append(key: String, value: RediSyncValue) async -> Int {
		await connectIfNotConnected()
		return await sockets?.append(key: key, value: value) ?? 0
	}
	
	@discardableResult
	public func copy(source: String, destination: String, replace: Bool? = nil) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.copy(source: source, destination: destination, replace: replace) == 1
	}
	
	@discardableResult
	public func decr(key: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.decr(key: key) ?? 0
	}
	
	@discardableResult
	public func decrby(key: String, decrement: Int) async -> Int? {
		await connectIfNotConnected()
		return await sockets?.decrby(key: key, decrement: decrement)
	}
	
	@discardableResult
	public func del(_ keys: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.del(keys: keys) ?? 0
	}
	
	@discardableResult
	public func del(_ keys: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.del(keys: keys) ?? 0
	}
	
	@discardableResult
	public func exists(_ keys: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.exists(keys: keys) ?? 0
	}
	
	@discardableResult
	public func exists(_ keys: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.exists(keys: keys) ?? 0
	}
	
	@discardableResult
	public func expire(key: String, seconds: Int, expireToken: RediSyncExpireToken? = nil) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.expire(key: key, seconds: seconds, expireToken: expireToken) == 1
	}
	
	@discardableResult
	public func expireat(key: String, unixTimeSeconds: Int, expireToken: RediSyncExpireToken? = nil) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.expireat(key: key, unixTimeSeconds: unixTimeSeconds, expireToken: expireToken) == 1
	}
	
	public func expiretime(key: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.expiretime(key: key) ?? -3
	}
	
	public func get(key: String) async -> String? {
		await connectIfNotConnected()
		return await sockets?.get(key: key)
	}
	
	public func getAndWatch(key: String) async -> RediSyncKey<String> {
		await connectIfNotConnected()
		let key = await RediSyncKey.forKey(key, sockets: sockets) { [weak self] in
			return await self?.get(key: key)
		}
		return await key.startWatching()
	}
	
	public func getdel(key: String) async -> String? {
		await connectIfNotConnected()
		return await sockets?.getdel(key: key)
	}
	
	public func getex(key: String, expiration: RediSyncGetExExpiration? = nil) async -> String? {
		await connectIfNotConnected()
		return await sockets?.getex(key: key, expiration: expiration)
	}
	
	public func getInt(key: String) async -> Int? {
		await connectIfNotConnected()
		return await sockets?.getInt(key: key)
	}
	
	public func getIntAndWatch(key: String) async -> RediSyncKey<Int> {
		await connectIfNotConnected()
		let key = await RediSyncKey.forKey(key, sockets: sockets) { [weak self] in
			return await self?.getInt(key: key)
		}
		return await key.startWatching()
	}
	
	public func getrange(key: String, start: Int, end: Int) async -> String {
		await connectIfNotConnected()
		return await sockets?.getrange(key: key, start: start, end: end) ?? ""
	}
	
	public func hdel(key: String, fields: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hdel(key: key, fields: fields) ?? 0
	}
	
	public func hdel(key: String, fields: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hdel(key: key, fields: fields) ?? 0
	}
	
	public func hexists(key: String, field: String) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.hexists(key: key, field: field) == 1
	}
	
	public func hget(key: String, field: String) async -> String? {
		await connectIfNotConnected()
		return await sockets?.hget(key: key, field: field)
	}
	
	public func hgetall(key: String) async -> [String: String] {
		await connectIfNotConnected()
		return await sockets?.hgetall(key: key) ?? [:]
	}
	
	public func hgetallAndWatch(key: String) async -> RediSyncKey<[String: String]> {
		await connectIfNotConnected()
		let key = await RediSyncKey.forKey(key, sockets: sockets) { [weak self] in
			return await self?.hgetall(key: key)
		}
		return await key.startWatching()
	}
	
	public func hgetInt(key: String, field: String) async -> Int? {
		await connectIfNotConnected()
		return await sockets?.hgetInt(key: key, field: field)
	}
	
	@discardableResult
	public func hincrby(key: String, field: String, increment: Int) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hincrby(key: key, field: field, increment: increment) ?? 0
	}
	
	@discardableResult
	public func hincrybydouble(key: String, field: String, increment: Double) async -> Double {
		await connectIfNotConnected()
		return await sockets?.hincrbydouble(key: key, field: field, increment: increment) ?? 0.0
	}
	
	@discardableResult
	public func hincrbyfloat(key: String, field: String, increment: Float) async -> Float {
		await connectIfNotConnected()
		return await sockets?.hincrbyfloat(key: key, field: field, increment: increment) ?? 0.0
	}
	
	public func hkeys(key: String) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.hkeys(key: key) ?? []
	}
	
	public func hlen(key: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hlen(key: key) ?? 0
	}
	
	public func hmget(key: String, fields: String...) async -> [String?] {
		await connectIfNotConnected()
		return await sockets?.hmget(key: key, fields: fields) ?? []
	}
	
	public func hmget(key: String, fields: [String]) async -> [String?] {
		await connectIfNotConnected()
		return await sockets?.hmget(key: key, fields: fields) ?? []
	}
	
	@discardableResult
	public func hset(key: String, fieldValues: (String, RediSyncValue)...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hset(key: key, fieldValues: fieldValues) ?? 0
	}
	
	@discardableResult
	public func hset(key: String, fieldValues: [(String, RediSyncValue)]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hset(key: key, fieldValues: fieldValues) ?? 0
	}
	
	@discardableResult
	public func hset(key: String, fieldValues: [String: RediSyncValue]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hset(key: key, fieldValues: fieldValues) ?? 0
	}

	@discardableResult
	public func hset(key: String, field: String, value: RediSyncValue) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.hset(key: key, fieldValues: (field, value)) == 1
	}
		
	@discardableResult
	public func hsetnx(key: String, field: String, value: RediSyncValue) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.hsetnx(key: key, field: field, value: value) == 1
	}
	
	public func hstrlen(key: String, field: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hstrlen(key: key, field: field) ?? 0
	}
	
	public func hvals(key: String) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.hvals(key: key) ?? []
	}
	
	@discardableResult
	public func incr(key: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.incr(key: key) ?? 0
	}
	
	@discardableResult
	public func incrby(key: String, increment: Int) async -> Int {
		await connectIfNotConnected()
		return await sockets?.incrby(key: key, increment: increment) ?? 0
	}
	
	@discardableResult
	public func incrbyfloat(key: String, increment: Float) async -> Float {
		await connectIfNotConnected()
		return await sockets?.incrbyfloat(key: key, increment: increment) ?? 0.0
	}
	
	public func keys(pattern: String) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.keys(pattern: pattern) ?? []
	}
	
	public func lindex(key: String, index: Int) async -> String? {
		await connectIfNotConnected()
		return await sockets?.lindex(key: key, index: index)
	}
	
	@discardableResult
	public func linsert(key: String, beforeOrAfter: RediSyncBeforeOrAfter, pivot: RediSyncValue, element: RediSyncValue) async -> Int {
		await connectIfNotConnected()
		return await sockets?.linsert(key: key, beforeOrAfter: beforeOrAfter, pivot: pivot, element: element) ?? -1
	}
	
	public func llen(key: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.llen(key: key) ?? 0
	}
	
	@discardableResult
	public func lmove(source: String, destination: String, sourceLeftOrRight: RediSyncLeftOrRight, destinationLeftOrRight: RediSyncLeftOrRight) async -> String? {
		await connectIfNotConnected()
		return await sockets?.lmove(source: source, destination: destination, sourceLeftRight: sourceLeftOrRight, destinationLeftRight: destinationLeftOrRight)
	}
	
	@discardableResult
	public func lpop(key: String) async -> String? {
		await connectIfNotConnected()
		return await sockets?.lpop(key: key)
	}
	
	@discardableResult
	public func lpop(key: String, count: Int) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.lpop(key: key, count: count) ?? []
	}
	
	@discardableResult
	public func lpush(key: String, elements: RediSyncValue...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.lpush(key: key, elements: elements) ?? 0
	}
	
	@discardableResult
	public func lpush(key: String, elements: [RediSyncValue]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.lpush(key: key, elements: elements) ?? 0
	}
	
	@discardableResult
	public func lpushx(key: String, elements: RediSyncValue...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.lpushx(key: key, elements: elements) ?? 0
	}
	
	@discardableResult
	public func lpushx(key: String, elements: [RediSyncValue]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.lpushx(key: key, elements: elements) ?? 0
	}
	
	public func lrange(key: String, start: Int, stop: Int) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.lrange(key: key, start: start, stop: stop) ?? []
	}
	
	public func lrangeAndWatch(key: String, start: Int, stop: Int) async -> RediSyncKey<[String]> {
		await connectIfNotConnected()
		let key = await RediSyncKey.forKey(key, sockets: sockets) { [weak self] in
			return await self?.lrange(key: key, start: start, stop: stop)
		}
		return await key.startWatching()
	}
	
	public func lrem(key: String, count: Int, element: RediSyncValue) async -> Int {
		await connectIfNotConnected()
		return await sockets?.lrem(key: key, count: count, element: element) ?? 0
	}
	
	public func lset(key: String, index: Int, element: RediSyncValue) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.lset(key: key, index: index, element: element) ?? false
	}
	
	public func ltrim(key: String, start: Int, stop: Int) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.ltrim(key: key, start: start, stop: stop) ?? false
	}
	
	public func lwatch(key: String) async -> RediSyncKey<[String]> {
		await connectIfNotConnected()
		let key = await RediSyncKey.forKey(key, sockets: sockets) { [weak self] in
			return await self?.lrange(key: key, start: 0, stop: -1)
		}
		return await key.startWatching()
	}
	
	public func mget(keys: String...) async -> [String?] {
		await connectIfNotConnected()
		return await sockets?.mget(keys: keys) ?? []
	}
	
	public func mget(keys: [String]) async -> [String?] {
		await connectIfNotConnected()
		return await sockets?.mget(keys: keys) ?? []
	}
	
	@discardableResult
	public func mset(keyValues: (String, RediSyncValue)...) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.mset(keyValues: keyValues) ?? false
	}
	
	@discardableResult
	public func mset(keyValues: [(String, RediSyncValue)]) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.mset(keyValues: keyValues) ?? false
	}
	
	@discardableResult
	public func mset(keyValues: [String: RediSyncValue]) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.mset(keyValues: keyValues) ?? false
	}
	
	@discardableResult
	public func msetnx(keyValues: (String, RediSyncValue)...) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.msetnx(keyValues: keyValues) ?? false
	}
	
	@discardableResult
	public func msetnx(keyValues: [(String, RediSyncValue)]) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.msetnx(keyValues: keyValues) ?? false
	}
	
	@discardableResult
	public func msetnx(keyValues: [String: RediSyncValue]) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.msetnx(keyValues: keyValues) ?? false
	}
	
	@discardableResult
	public func rpop(key: String) async -> String? {
		await connectIfNotConnected()
		return await sockets?.rpop(key: key)
	}
	
	@discardableResult
	public func rpop(key: String, count: Int) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.rpop(key: key, count: count) ?? []
	}
	
	@discardableResult
	public func rpush(key: String, elements: RediSyncValue...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.rpush(key: key, elements: elements) ?? 0
	}
	
	@discardableResult
	public func rpush(key: String, elements: [RediSyncValue]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.rpush(key: key, elements: elements) ?? 0
	}

	@discardableResult
	public func rpushx(key: String, elements: RediSyncValue...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.rpushx(key: key, elements: elements) ?? 0
	}
	
	@discardableResult
	public func rpushx(key: String, elements: [RediSyncValue]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.rpushx(key: key, elements: elements) ?? 0
	}
	
	@discardableResult
	public func sadd(key: String, members: RediSyncValue...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sadd(key: key, members: members) ?? 0
	}
	
	@discardableResult
	public func sadd(key: String, members: [RediSyncValue]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sadd(key: key, members: members) ?? 0
	}
	
	public func scard(key: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.scard(key: key) ?? 0
	}
	
	public func sdiff(key: String, keys: String...) async -> Set<String> {
		await connectIfNotConnected()
		return Set(await sockets?.sdiff(key: key, keys: keys) ?? [])
	}

	public func sdiff(key: String, keys: [String]) async -> Set<String> {
		await connectIfNotConnected()
		return Set(await sockets?.sdiff(key: key, keys: keys) ?? [])
	}
	
	@discardableResult
	public func sdiffstore(destination: String, key: String, keys: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sdiffstore(destination: destination, key: key, keys: keys) ?? 0
	}

	@discardableResult
	public func sdiffstore(destination: String, key: String, keys: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sdiffstore(destination: destination, key: key, keys: keys) ?? 0
	}

	@discardableResult
	public func set(key: String, value: RediSyncValue) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.set(key: key, value: value) ?? false
	}
	
	@discardableResult
	public func setrange(key: String, offset: Int, value: RediSyncValue) async -> Int {
		await connectIfNotConnected()
		return await sockets?.setrange(key: key, offset: offset, value: value) ?? 0
	}
	
	public func sinter(key: String, keys: String...) async -> Set<String> {
		await connectIfNotConnected()
		return Set(await sockets?.sinter(key: key, keys: keys) ?? [])
	}
	
	public func sinter(key: String, keys: [String]) async -> Set<String> {
		await connectIfNotConnected()
		return Set(await sockets?.sinter(key: key, keys: keys) ?? [])
	}
	
	public func sintercard(key: String, keys: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sintercard(key: key, keys: keys) ?? 0
	}
	
	public func sintercard(key: String, keys: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sintercard(key: key, keys: keys) ?? 0
	}
	
	@discardableResult
	public func sinterstore(destination: String, key: String, keys: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sinterstore(destination: destination, key: key, keys: keys) ?? 0
	}
	
	@discardableResult
	public func sinterstore(destination: String, key: String, keys: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sinterstore(destination: destination, key: key, keys: keys) ?? 0
	}
	
	public func sismember(key: String, member: RediSyncValue) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.sismember(key: key, member: member) == 1
	}
	
	public func smembers(key: String) async -> Set<String> {
		await connectIfNotConnected()
		return Set(await sockets?.smembers(key: key) ?? [])
	}
	
	public func smembersAndWatch(key: String) async -> RediSyncKey<Set<String>> {
		await connectIfNotConnected()
		let key = await RediSyncKey.forKey(key, sockets: sockets) { [weak self] in
			return await self?.smembers(key: key)
		}
		return await key.startWatching()
	}
	
	public func smismember(key: String, members: RediSyncValue...) async -> [Bool] {
		await connectIfNotConnected()
		return (await sockets?.smismember(key: key, members: members) ?? []).map { $0 == 1 }
	}
	
	public func smismember(key: String, members: [RediSyncValue]) async -> [Bool] {
		await connectIfNotConnected()
		return (await sockets?.smismember(key: key, members: members) ?? []).map { $0 == 1 }
	}
	
	@discardableResult
	public func smove(source: String, destination: String, member: RediSyncValue) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.smove(source: source, destination: destination, member: member) == 1
	}
	
	@discardableResult
	public func spop(key: String) async -> String? {
		await connectIfNotConnected()
		return await sockets?.spop(key: key)
	}
	
	@discardableResult
	public func spop(key: String, count: Int) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.spop(key: key, count: count) ?? []
	}
	
	public func srandmember(key: String) async -> String? {
		await connectIfNotConnected()
		return await sockets?.srandmember(key: key)
	}
	
	public func srandmember(key: String, count: Int) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.srandmember(key: key, count: count) ?? []
	}
	
	@discardableResult
	public func srem(key: String, members: RediSyncValue...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.srem(key: key, members: members) ?? 0
	}
	
	@discardableResult
	public func srem(key: String, members: [RediSyncValue]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.srem(key: key, members: members) ?? 0
	}
	
	public func strlen(key: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.strlen(key: key) ?? 0
	}
	
	public func sunion(key: String, keys: String...) async -> Set<String> {
		await connectIfNotConnected()
		return Set(await sockets?.sunion(key: key, keys: keys) ?? [])
	}
	
	public func sunion(key: String, keys: [String]) async -> Set<String> {
		await connectIfNotConnected()
		return Set(await sockets?.sunion(key: key, keys: keys) ?? [])
	}
	
	@discardableResult
	public func sunionstore(destination: String, key: String, keys: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sunionstore(destination: destination, key: key, keys: keys) ?? 0
	}
	
	@discardableResult
	public func sunionstore(destination: String, key: String, keys: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.sunionstore(destination: destination, key: key, keys: keys) ?? 0
	}
	
	@discardableResult
	public func touch(keys: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.touch(keys: keys) ?? 0
	}
	
	@discardableResult
	public func touch(keys: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.touch(keys: keys) ?? 0
	}
	
	public func ttl(key: String) async -> Int {
		await connectIfNotConnected()
		return await sockets?.ttl(key: key) ?? -1
	}
	
	@discardableResult
	private func connectIfNotConnected() async -> Bool {
		guard status != .connected else { return true }
		
		return await connect()
	}
}
