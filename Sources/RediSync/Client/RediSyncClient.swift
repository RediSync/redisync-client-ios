//
//  RediSyncClient.swift
//  
//
//  Created by Mike Richards on 6/23/23.
//

import Foundation
import os


@available(macOS 13.0, *)
@objc
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
		
		emit("error", args: "RediSync Connection Error")
		
		return false
	}
	
	@discardableResult
	public func append(key: String, value: String) async -> Int {
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
	
	@discardableResult
	public func hincrby(key: String, field: String, increment: Int) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hincrby(key: key, field: field, increment: increment) ?? 0
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
	
	@discardableResult
	public func hset(key: String, fieldValues: (String, Any)...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hset(key: key, fieldValues: fieldValues) ?? 0
	}
	
	@discardableResult
	public func hset(key: String, fieldValues: [(String, Any)]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hset(key: key, fieldValues: fieldValues) ?? 0
	}
	
	@discardableResult
	public func hset(key: String, field: String, value: Any) async -> Int {
		await connectIfNotConnected()
		return await sockets?.hset(key: key, fieldValues: (field, value)) ?? 0
	}
	
	public func keys(pattern: String) async -> [String] {
		await connectIfNotConnected()
		return await sockets?.keys(pattern: pattern) ?? []
	}
	
	@discardableResult
	public func set(key: String, value: String) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.set(key: key, value: value) ?? false
	}
	
	@discardableResult
	public func set(key: String, value: Int) async -> Bool {
		await connectIfNotConnected()
		return await sockets?.set(key: key, value: value) ?? false
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
