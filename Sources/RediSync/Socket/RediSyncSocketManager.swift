//
//  RediSyncSocketManager.swift
//  
//
//  Created by Mike Richards on 6/28/23.
//

import Foundation
import os

@available(macOS 11.0, *)
class RediSyncSocketManager: RediSyncEventEmitter
{
	public var isConnected: Bool {
		return sockets.contains(where: { $0.state != .notConnected })
	}
	
	private let logger = Logger(subsystem: "RediSync", category: "RediSyncSocketManager")
	
	private var sockets: [RediSyncSocket] = []
	
	init(socketUrls: [URL], key: String, rs: String?) {
		super.init()
		
		for socketUrl in socketUrls {
			logger.debug("Connecting to socket '\(socketUrl, privacy: .public)'")
			
			let socket = RediSyncSocket(url: socketUrl, key: key, rs: rs)
			
			socket.on("connected") { [weak self] _ in
				self?.socketConnected(socket)
			}
			
			socket.on("disconnected") { [weak self] _ in
				self?.socketDisconnected(socket)
			}
			
			sockets.append(socket)
		}
	}
	
	func append(key: String, value: String) async -> Int? {
		let result = await sendToSockets { await $0.append(key: key, value: value) }
		return result?.value
	}
	
	func copy(source: String, destination: String, replace: Bool? = nil) async -> Int? {
		let result = await sendToSockets { await $0.copy(source: source, destination: destination, replace: replace) }
		return result?.value
	}
	
	func decr(key: String) async -> Int? {
		let result = await sendToSockets { await $0.decr(key: key) }
		return result?.value
	}
	
	func decrby(key: String, decrement: Int) async -> Int? {
		let result = await sendToSockets { await $0.decrby(key: key, decrement: decrement) }
		return result?.value
	}
	
	func del(keys: String...) async -> Int? {
		let result = await sendToSockets { await $0.del(keys: keys) }
		return result?.value
	}
	
	func del(keys: [String]) async -> Int? {
		let result = await sendToSockets { await $0.del(keys: keys) }
		return result?.value
	}
	
	func exists(keys: String...) async -> Int? {
		let result = await sendToSockets { await $0.exists(keys: keys) }
		return result?.value
	}
	
	func exists(keys: [String]) async -> Int? {
		let result = await sendToSockets { await $0.exists(keys: keys) }
		return result?.value
	}
	
	func expire(key: String, seconds: Int, expireToken: RediSyncExpireToken?) async -> Int? {
		let result = await sendToSockets { await $0.expire(key: key, seconds: seconds, expireToken: expireToken) }
		return result?.value
	}
	
	func expireat(key: String, unixTimeSeconds: Int, expireToken: RediSyncExpireToken?) async -> Int? {
		let result = await sendToSockets { await $0.expireat(key: key, unixTimeSeconds: unixTimeSeconds, expireToken: expireToken) }
		return result?.value
	}
	
	func expiretime(key: String) async -> Int? {
		let result = await sendToSockets { await $0.expiretime(key: key) }
		return result?.value
	}
	
	func get(key: String) async -> String? {
		let result = await sendToSockets { await $0.get(key: key) }
		return result?.value
	}
	
	func getdel(key: String) async -> String? {
		let result = await sendToSockets { await $0.getdel(key: key) }
		return result?.value
	}
	
	func getex(key: String, expiration: RediSyncGetExExpiration?) async -> String? {
		let result = await sendToSockets { await $0.getex(key: key, expiration: expiration) }
		return result?.value
	}
	
	func getInt(key: String) async -> Int? {
		let result = await sendToSockets { await $0.getInt(key: key) }
		return result?.value
	}
	
	func getrange(key: String, start: Int, end: Int) async -> String? {
		let result = await sendToSockets { await $0.getrange(key: key, start: start, end: end) }
		return result?.value
	}
	
	func hdel(key: String, fields: String...) async -> Int? {
		let result = await sendToSockets { await $0.hdel(key: key, fields: fields) }
		return result?.value
	}
	
	func hdel(key: String, fields: [String]) async -> Int? {
		let result = await sendToSockets { await $0.hdel(key: key, fields: fields) }
		return result?.value
	}
	
	func hexists(key: String, field: String) async -> Int? {
		let result = await sendToSockets { await $0.hexists(key: key, field: field) }
		return result?.value
	}
	
	func hget(key: String, field: String) async -> String? {
		let result = await sendToSockets { await $0.hget(key: key, field: field) }
		return result?.value
	}
	
	func hgetall(key: String) async -> [String: String]? {
		let result = await sendToSockets { await $0.hgetall(key: key) }
		return result?.value
	}
	
	func hincrby(key: String, field: String, increment: Int) async -> Int? {
		let result = await sendToSockets { await $0.hincrby(key: key, field: field, increment: increment) }
		return result?.value
	}
	
	func hincrbyfloat(key: String, field: String, increment: Float) async -> Float? {
		let result = await sendToSockets { await $0.hincrbyfloat(key: key, field: field, increment: increment) }
		return result?.value
	}
	
	func hkeys(key: String) async -> [String]? {
		let result = await sendToSockets { await $0.hkeys(key: key) }
		return result?.value
	}
	
	func hlen(key: String) async -> Int? {
		let result = await sendToSockets { await $0.hlen(key: key)}
		return result?.value
	}
	
	func hmget(key: String, fields: String...) async -> [String?]? {
		let result = await sendToSockets { await $0.hmget(key: key, fields: fields) }
		return result?.value
	}
	
	func hmget(key: String, fields: [String]) async -> [String?]? {
		let result = await sendToSockets { await $0.hmget(key: key, fields: fields) }
		return result?.value
	}
	
	func hset(key: String, fieldValues: (String, Any)...) async -> Int? {
		let result = await sendToSockets { await $0.hset(key: key, fieldValues: fieldValues) }
		return result?.value
	}
	
	func hset(key: String, fieldValues: [(String, Any)]) async -> Int? {
		let result = await sendToSockets { await $0.hset(key: key, fieldValues: fieldValues) }
		return result?.value
	}
	
	func hsetnx(key: String, field: String, value: String) async -> Int? {
		let result = await sendToSockets { await $0.hsetnx(key: key, field: field, value: value) }
		return result?.value
	}
	
	func hsetnx(key: String, field: String, value: Int) async -> Int? {
		let result = await sendToSockets { await $0.hsetnx(key: key, field: field, value: value) }
		return result?.value
	}
	
	func hstrlen(key: String, field: String) async -> Int? {
		let result = await sendToSockets { await $0.hstrlen(key: key, field: field) }
		return result?.value
	}
	
	func hvals(key: String) async -> [String]? {
		let result = await sendToSockets { await $0.hvals(key: key) }
		return result?.value
	}
	
	func incr(key: String) async -> Int? {
		let result = await sendToSockets { await $0.incr(key: key) }
		return result?.value
	}
	
	func incrby(key: String, increment: Int) async -> Int? {
		let result = await sendToSockets { await $0.incrby(key: key, increment: increment) }
		return result?.value
	}
	
	func incrbyfloat(key: String, increment: Float) async -> Float? {
		let result = await sendToSockets { await $0.incrbyfloat(key: key, increment: increment) }
		return result?.value
	}
	
	func keys(pattern: String) async -> [String]? {
		let result = await sendToSockets { await $0.keys(pattern: pattern) }
		return result?.value
	}
	
	func lindex(key: String, index: Int) async -> String? {
		let result = await sendToSockets { await $0.lindex(key: key, index: index) }
		return result?.value
	}
	
	func linsert(key: String, beforeOrAfter: RediSyncBeforeOrAfter, pivot: RediSyncValue, element: RediSyncValue) async -> Int? {
		let result = await sendToSockets { await $0.linsert(key: key, beforeOrAfter: beforeOrAfter, pivot: pivot, element: element) }
		return result?.value
	}
	
	func llen(key: String) async -> Int? {
		let result = await sendToSockets { await $0.llen(key: key) }
		return result?.value
	}
	
	func lmove(source: String, destination: String, sourceLeftRight: RediSyncLeftOrRight, destinationLeftRight: RediSyncLeftOrRight) async -> String? {
		let result = await sendToSockets { await $0.lmove(source: source, destination: destination, sourceLeftRight: sourceLeftRight, destinationLeftRight: destinationLeftRight) }
		return result?.value
	}
	
	func lpop(key: String) async -> String? {
		let result = await sendToSockets { await $0.lpop(key: key) }
		return result?.value
	}
	
	func lpop(key: String, count: Int) async -> [String]? {
		let result = await sendToSockets { await $0.lpop(key: key, count: count)}
		return result?.value
	}
	
	func lpush(key: String, elements: RediSyncValue...) async -> Int? {
		let result = await sendToSockets { await $0.lpush(key: key, elements: elements) }
		return result?.value
	}
	
	func lpush(key: String, elements: [RediSyncValue]) async -> Int? {
		let result = await sendToSockets { await $0.lpush(key: key, elements: elements) }
		return result?.value
	}
	
	func lpushx(key: String, elements: RediSyncValue...) async -> Int? {
		let result = await sendToSockets { await $0.lpushx(key: key, elements: elements) }
		return result?.value
	}
	
	func lpushx(key: String, elements: [RediSyncValue]) async -> Int? {
		let result = await sendToSockets { await $0.lpushx(key: key, elements: elements) }
		return result?.value
	}
	
	func lrange(key: String, start: Int, stop: Int) async -> [String]? {
		let result = await sendToSockets { await $0.lrange(key: key, start: start, stop: stop)}
		return result?.value
	}
	
	func lrem(key: String, count: Int, element: RediSyncValue) async -> Int? {
		let result = await sendToSockets { await $0.lrem(key: key, count: count, element: element) }
		return result?.value
	}
	
	func lset(key: String, index: Int, element: RediSyncValue) async -> Bool? {
		let result = await sendToSockets { await $0.lset(key: key, index: index, element: element)}
		return result?.ok
	}
	
	func ltrim(key: String, start: Int, stop: Int) async -> Bool? {
		let result = await sendToSockets { await $0.ltrim(key: key, start: start, stop: stop) }
		return result?.ok
	}
	
	func rpop(key: String) async -> String? {
		let result = await sendToSockets { await $0.rpop(key: key) }
		return result?.value
	}
	
	func rpop(key: String, count: Int) async -> [String]? {
		let result = await sendToSockets { await $0.rpop(key: key, count: count)}
		return result?.value
	}
	
	func rpush(key: String, elements: RediSyncValue...) async -> Int? {
		let result = await sendToSockets { await $0.rpush(key: key, elements: elements) }
		return result?.value
	}

	func rpush(key: String, elements: [RediSyncValue]) async -> Int? {
		let result = await sendToSockets { await $0.rpush(key: key, elements: elements) }
		return result?.value
	}

	func rpushx(key: String, elements: RediSyncValue...) async -> Int? {
		let result = await sendToSockets { await $0.rpushx(key: key, elements: elements) }
		return result?.value
	}
	
	func rpushx(key: String, elements: [RediSyncValue]) async -> Int? {
		let result = await sendToSockets { await $0.rpushx(key: key, elements: elements) }
		return result?.value
	}

	func set(key: String, value: String) async -> Bool? {
		let result = await sendToSockets { await $0.set(key: key, value: value) }
		return result?.ok
	}
	
	func set(key: String, value: Int) async -> Bool? {
		let result = await sendToSockets { await $0.set(key: key, value: value) }
		return result?.ok
	}
	
	func set(key: String, value: Float) async -> Bool? {
		let result = await sendToSockets { await $0.set(key: key, value: value) }
		return result?.ok
	}
	
	func ttl(key: String) async -> Int? {
		let result = await sendToSockets { await $0.ttl(key: key) }
		return result?.value
	}
	
	private func sendToSockets<T: RediSyncSocketResponse>(_ handler: @escaping RediSyncSocketMessageHandler<T>) async -> T? {
		return await withCheckedContinuation { continuation in
			let continuationBlockDuplicates = RediSyncContinuationBlockingDuplicates(continuation: continuation)
			
			for socket in sockets {
				Task {
					continuationBlockDuplicates.returnResult(await handler(socket))
				}
			}
		}
	}
	
	private func socketConnected(_ socket: RediSyncSocket) {
		logger.debug("Socket '\(socket.url, privacy: .public)' connected")
		
		if sockets.contains(where: { $0.state == .connected }) {
			emit("connected")
		}
	}
	
	private func socketDisconnected(_ socket: RediSyncSocket) {
		logger.debug("Socket '\(socket.url, privacy: .public)' disconnected")
		
		if !isConnected {
			emit("disconnected")
		}
	}
}

@available(macOS 11.0, *)
typealias RediSyncSocketMessageHandler<T: RediSyncSocketResponse> = (RediSyncSocket) async -> (T?)
