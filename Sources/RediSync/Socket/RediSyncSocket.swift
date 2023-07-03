//
//  RediSyncSocket.swift
//  
//
//  Created by Mike Richards on 6/27/23.
//

import Foundation
import os
import SocketIO

enum RediSyncSocketState: Int
{
	case notConnected
	case connecting
	case connected
	case reconnecting
	case reconnectOnDisconnect
}

@available(macOS 11.0, *)
final class RediSyncSocket: RediSyncEventEmitter
{
	public private(set) var state: RediSyncSocketState = .notConnected {
		didSet {
			logger.debug("Socket '\(self.url, privacy: .public)' state changed: \(self.state.rawValue, privacy: .public)")
			
			emit("state-changed", args: state)
			
			if state == .connected {
				emit("connected")
			}
			else if state == .notConnected {
				emit("disconnected")
			}
		}
	}
	
	internal let url: URL

	private let logger = Logger(subsystem: "RediSync", category: "RediSyncSocket")
	private let rs: String?
	
	private var key: String
	private var socket: SocketIOClient?
	private var socketManager: SocketManager?
	
	init(url: URL, key: String, rs: String?) {
		self.rs = rs
		self.url = url
		self.key = key
		
		super.init()
		
		Task {
			await connect()
		}
	}
	
	deinit {
		dispose()
	}
	
	func append(key: String, value: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("append", key, value))
	}
	
	func copy(source: String, destination: String, replace: Bool? = nil) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("copy", source, destination, replace == true ? 1 : 0))
	}
	
	func decr(key: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("decr", key))
	}
	
	func decrby(key: String, decrement: Int) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("decrby", key, decrement))
	}
	
	func del(keys: String...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("del", params: keys))
	}
	
	func del(keys: [String]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("del", params: keys))
	}
	
	func exists(keys: String...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("exists", params: keys))
	}
										 
	func exists(keys: [String]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("exists", params: keys))
	}
	
	func expire(key: String, seconds: Int, expireToken: RediSyncExpireToken? = nil) async -> RediSyncSocketIntResponse? {
		if let expireToken = expireToken {
			return RediSyncSocketIntResponse(await emitRedis("expire", key, seconds, expireToken.rawValue))
		}
		
		return RediSyncSocketIntResponse(await emitRedis("expire", key, seconds))
	}
	
	func expireat(key: String, unixTimeSeconds: Int, expireToken: RediSyncExpireToken? = nil) async -> RediSyncSocketIntResponse? {
		if let expireToken = expireToken {
			return RediSyncSocketIntResponse(await emitRedis("expireat", key, unixTimeSeconds, expireToken.rawValue))
		}
		
		return RediSyncSocketIntResponse(await emitRedis("expireat", key, unixTimeSeconds))
	}
	
	func expiretime(key: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("expiretime", key))
	}
	
	func get(key: String) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("get", key))
	}
	
	func getdel(key: String) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("getdel", key))
	}
	
	func getex(key: String, expiration: RediSyncGetExExpiration?) async -> RediSyncSocketStringResponse? {
		var getExParams: [Any] = [key]
		
		if let expiration = expiration {
			switch expiration {
			case let .EX(seconds):
				getExParams.append("EX")
				getExParams.append(seconds)
			case let .PX(milliseconds):
				getExParams.append("PX")
				getExParams.append(milliseconds)
			case let .EXAT(timestampSeconds):
				getExParams.append("EXAT")
				getExParams.append(timestampSeconds)
			case let .PXAT(timestampMilliseconds):
				getExParams.append("PXAT")
				getExParams.append(timestampMilliseconds)
			case .PERSIST:
				getExParams.append("PERSIST")
			}
		}
		
		return RediSyncSocketStringResponse(await emitRedis("getex", params: getExParams))
	}
	
	func getInt(key: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("get", key))
	}
	
	func getrange(key: String, start: Int, end: Int) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("getrange", key, start, end))
	}
	
	func hdel(key: String, fields: String...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("hdel", params: [key] + fields))
	}
	
	func hdel(key: String, fields: [String]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("hdel", params: [key] + fields))
	}
	
	func hexists(key: String, field: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("hexists", key, field))
	}
	
	func hget(key: String, field: String) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("hget", key, field))
	}
	
	func hgetall(key: String) async -> RediSyncSocketDictResponse? {
		return RediSyncSocketDictResponse(await emitRedis("hgetall", key))
	}
	
	func hincrby(key: String, field: String, increment: Int) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("hincrby", key, field, increment))
	}
	
	func hincrbyfloat(key: String, field: String, increment: Float) async -> RediSyncSocketFloatResponse? {
		return RediSyncSocketFloatResponse(await emitRedis("hincrbyfloat", key, field, String(increment)))
	}
	
	func hkeys(key: String) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("hkeys", key))
	}
	
	func hlen(key: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("hlen", key))
	}
	
	func hmget(key: String, fields: String...) async -> RediSyncSocketArrayResponse<String?>? {
		return RediSyncSocketArrayResponse(await emitRedis("hmget", params: [key] + fields))
	}
	
	func hmget(key: String, fields: [String]) async -> RediSyncSocketArrayResponse<String?>? {
		return RediSyncSocketArrayResponse(await emitRedis("hmget", params: [key] + fields))
	}
	
	func hset(key: String, fieldValues: (String, Any)...) async -> RediSyncSocketIntResponse? {
		let hsetParams = [key] + fieldValues.flatMap { [$0.0, $0.1] }
		return RediSyncSocketIntResponse(await emitRedis("hset", params: hsetParams))
	}

	func hset(key: String, fieldValues: [(String, Any)]) async -> RediSyncSocketIntResponse? {
		let hsetParams = [key] + fieldValues.flatMap { [$0.0, $0.1] }
		return RediSyncSocketIntResponse(await emitRedis("hset", params: hsetParams))
	}
	
	func hsetnx(key: String, field: String, value: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("hsetnx", key, field, value))
	}

	func hsetnx(key: String, field: String, value: Int) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("hsetnx", key, field, value))
	}
	
	func hstrlen(key: String, field: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("hstrlen", key, field))
	}
	
	func hvals(key: String) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("hvals", key))
	}
	
	func incr(key: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("incr", key))
	}

	func incrby(key: String, increment: Int) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("incrby", key, increment))
	}
	
	func incrbyfloat(key: String, increment: Float) async -> RediSyncSocketFloatResponse? {
		return RediSyncSocketFloatResponse(await emitRedis("incrbyfloat", key, String(increment)))
	}

	func keys(pattern: String) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("keys", pattern))
	}
	
	func lindex(key: String, index: Int) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("lindex", key, index))
	}
	
	func linsert(key: String, beforeOrAfter: RediSyncBeforeOrAfter, pivot: RediSyncValue, element: RediSyncValue) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("linsert", key, beforeOrAfter.rawValue, pivot, element))
	}
	
	func llen(key: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("llen", key))
	}
	
	func lmove(source: String, destination: String, sourceLeftRight: RediSyncLeftOrRight, destinationLeftRight: RediSyncLeftOrRight) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("lmove", source, destination, sourceLeftRight.rawValue, destinationLeftRight.rawValue))
	}
	
	func lpop(key: String) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("lpop", key))
	}
	
	func lpop(key: String, count: Int) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("lpop", key, count))
	}
	
	func lpush(key: String, elements: RediSyncValue...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("lpush", params: [key] + elements))
	}
	
	func lpush(key: String, elements: [RediSyncValue]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("lpush", params: [key] + elements))
	}
	
	func lpushx(key: String, elements: RediSyncValue...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("lpushx", params: [key] + elements))
	}
	
	func lpushx(key: String, elements: [RediSyncValue]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("lpushx", params: [key] + elements))
	}
	
	func lrange(key: String, start: Int, stop: Int) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("lrange", key, start, stop))
	}
	
	func lrem(key: String, count: Int, element: RediSyncValue) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("lrem", key, count, element))
	}
	
	func lset(key: String, index: Int, element: RediSyncValue) async -> RediSyncSocketOKResponse? {
		return RediSyncSocketOKResponse(await emitRedis("lset", key, index, element))
	}
	
	func ltrim(key: String, start: Int, stop: Int) async -> RediSyncSocketOKResponse? {
		return RediSyncSocketOKResponse(await emitRedis("ltrim", key, start, stop))
	}
	
	func rpop(key: String) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("rpop", key))
	}
	
	func rpop(key: String, count: Int) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("rpop", key, count))
	}

	func rpush(key: String, elements: RediSyncValue...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("rpush", params: [key] + elements))
	}
	
	func rpush(key: String, elements: [RediSyncValue]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("rpush", params: [key] + elements))
	}
	
	func rpushx(key: String, elements: RediSyncValue...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("rpushx", params: [key] + elements))
	}
	
	func rpushx(key: String, elements: [RediSyncValue]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("rpushx", params: [key] + elements))
	}
	
	func sadd(key: String, members: RediSyncValue...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sadd", params: [key] + members))
	}

	func sadd(key: String, members: [RediSyncValue]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sadd", params: [key] + members))
	}
	
	func scard(key: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("scard", key))
	}
	
	func sdiff(key: String, keys: String...) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("sdiff", params: [key] + keys))
	}
	
	func sdiff(key: String, keys: [String]) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("sdiff", params: [key] + keys))
	}
	
	func sdiffstore(destination: String, key: String, keys: String...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sdiffstore", params: [destination, key] + keys))
	}

	func sdiffstore(destination: String, key: String, keys: [String]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sdiffstore", params: [destination, key] + keys))
	}

	func set(key: String, value: String) async -> RediSyncSocketOKResponse? {
		return RediSyncSocketOKResponse(await emitRedis("set", key, value))
	}
	
	func set(key: String, value: Int) async -> RediSyncSocketOKResponse? {
		return RediSyncSocketOKResponse(await emitRedis("set", key, value))
	}
	
	func set(key: String, value: Float) async -> RediSyncSocketOKResponse? {
		return RediSyncSocketOKResponse(await emitRedis("set", key, String(value)))
	}
	
	func setrange(key: String, offset: Int, value: RediSyncValue) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("setrange", key, offset, value))
	}
	
	func sinter(key: String, keys: String...) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("sinter", params: [key] + keys))
	}
	
	func sinter(key: String, keys: [String]) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("sinter", params: [key] + keys))
	}
	
	func sintercard(key: String, keys: String...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sintercard", params: [key] + keys))
	}
	
	func sintercard(key: String, keys: [String]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sintercard", params: [key] + keys))
	}
	
	func sinterstore(destination: String, key: String, keys: String...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sinterstore", params: [destination, key] + keys))
	}
	
	func sinterstore(destination: String, key: String, keys: [String]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sinterstore", params: [destination, key] + keys))
	}
	
	func sismember(key: String, member: RediSyncValue) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("sismember", key, member))
	}
	
	func smembers(key: String) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("smembers", key))
	}
	
	func smismember(key: String, members: RediSyncValue...) async -> RediSyncSocketIntArrayResponse? {
		return RediSyncSocketIntArrayResponse(await emitRedis("smismember", params: [key] + members))
	}
	
	func smismember(key: String, members: [RediSyncValue]) async -> RediSyncSocketIntArrayResponse? {
		return RediSyncSocketIntArrayResponse(await emitRedis("smismember", params: [key] + members))
	}
	
	func smove(source: String, destination: String, member: RediSyncValue) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("smove", source, destination, member))
	}
	
	func spop(key: String) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("spop", key))
	}
	
	func spop(key: String, count: Int) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("spop", key, count))
	}
	
	func srandmember(key: String) async -> RediSyncSocketStringResponse? {
		return RediSyncSocketStringResponse(await emitRedis("srandmember", key))
	}
	
	func srandmember(key: String, count: Int) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("srandmember", key, count))
	}
	
	func srem(key: String, members: RediSyncValue...) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("srem", params: [key] + members))
	}
	
	func srem(key: String, members: [RediSyncValue]) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("srem", params: [key] + members))
	}
	
	func sunion(key: String, keys: String...) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("sunion", params: [key] + keys))
	}
	
	func sunion(key: String, keys: [String]) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("sunion", params: [key] + keys))
	}
	
	func ttl(key: String) async -> RediSyncSocketIntResponse? {
		return RediSyncSocketIntResponse(await emitRedis("ttl", key))
	}
	
	@discardableResult
	private func connect(state: RediSyncSocketState = .connecting) async -> Bool {
		guard state == .connecting || state == .reconnecting else { return false }
		
		guard self.state != .connected else { return true }
		
		logger.debug("Connecting to socketUrl '\(self.url, privacy: .public)' with rs: \(self.rs ?? "nil", privacy: .public)")
		
		self.state = state
		
		socketManager = SocketManager(socketURL: url, config: [.forceNew(true), .forceWebsockets(true)])
		socket = socketManager?.defaultSocket
		
		socket?.on(clientEvent: .connect) { [weak self] _, _ in
			self?.onConnect()
		}

		socket?.on(clientEvent: .disconnect) { [weak self] data, _ in
			self?.onDisconnect(data: data)
		}
		
		socket?.on(clientEvent: .error) { [weak self] data, _ in
			self?.onError(data: data)
		}
				
		socket?.on("redisync-error") { [weak self] data, ack in
			self?.onRedisyncError(data: data, ack: ack)
		}
				
		return await withCheckedContinuation { continuation in
			var callbackCalled = false
			
			func returnResult(_ result: Bool) {
				guard !callbackCalled else { return }
				
				callbackCalled = true
				
				off(id: stateChangedListenerId)
				
				continuation.resume(returning: result)
			}
			
			let stateChangedListenerId = on("state-changed") { (state: RediSyncSocketState?) in
				if state == .connected {
					returnResult(true)
				}
				else if state == .notConnected {
					returnResult(false)
				}
			}
						
			var socketPayload: [String: Any] = [:]
			
			if let rs = rs {
				socketPayload["rs"] = rs
			}

			socket?.connect(withPayload: socketPayload, timeoutAfter: 10) { [weak self] in
				self?.reconnect()
			}
		}
	}
	
	private func dispose() {
		logger.debug("dispose")
		
		socketManager?.disconnect()
	}
	
	private func emitRedis(_ redisFunction: String, _ params: Any...) async -> [Any] {
		return await emitToSocket("redis", [redisFunction] + params)
	}
	
	private func emitRedis(_ redisFunction: String, params: [Any]) async -> [Any] {
		return await emitToSocket("redis", [redisFunction] + params)
	}
	
	private func emitToSocket(_ event: String, _ params: Any...) async -> [Any] {
		logger.debug("emitToSocket(\(event, privacy: .public): \(params, privacy: .public)")
		
		return await withCheckedContinuation { continuation in
			socket?.emitWithAck(event, with: params as! [any SocketData]).timingOut(after: 10) { [weak self] data in
				self?.logger.debug("emitToSocket(\(event, privacy: .public) ack: \(data, privacy: .public)")
				
				continuation.resume(returning: data)
			}
		}
	}
	
	private func emitToSocket(_ event: String, _ params: [Any]) async -> [Any] {
		logger.debug("emitToSocket(\(event, privacy: .public): \(params, privacy: .public)")
		
		return await withCheckedContinuation { continuation in
			socket?.emitWithAck(event, with: params as! [any SocketData]).timingOut(after: 10) { [weak self] data in
				self?.logger.debug("emitToSocket(\(event, privacy: .public) ack: \(data, privacy: .public)")
				
				continuation.resume(returning: data)
			}
		}
	}
	
	private func initConnection() async {
		logger.debug("Initializing connection")
		
		let ack = await emitToSocket(
			"init",
			[
				"key": key
			]
		)
		
		guard let response = RediSyncSocketInitResponse(ack) else {
			logger.error("Initialization failed")
			state = .notConnected
			return
		}
		
		key = response.key
		
		logger.debug("Connection initialized")
		
		state = .connected
	}
	
	private func onConnect() {
		logger.debug("Socket connected")
		
		Task {
			await initConnection()
		}
	}
	
	private func onDisconnect(data: [Any]) {
		logger.debug("Socket disconnected - \(data, privacy: .public)")
		
		if state == .reconnectOnDisconnect {
			logger.debug("Manually reconnecting")
			
			Task {
				await connect(state: .reconnecting)
			}
		}
		else {
			logger.debug("Completely disconnected")
			state = .notConnected
		}
	}
	
	private func onError(data: [Any]) {
		logger.error("ERROR - \(data, privacy: .public)")
	}
	
	private func onRedisyncError(data: [Any], ack: SocketAckEmitter) {
		
	}
	
	private func reconnect() {
		socketManager?.disconnect()
		
		socket = nil
		socketManager = nil
		
		DispatchQueue.global(qos: .background).asyncAfter(deadline: DispatchTime.now() + 1) { [weak self] in
			guard let self = self else { return }
			
			Task {
				await self.connect()
			}
		}
	}

}

public enum RediSyncExpireToken: String {
	case NX = "NX"
	case XX = "XX"
	case GT = "GT"
	case LT = "LT"
}

public enum RediSyncGetExExpiration {
	case EX(seconds: Int)
	case PX(milliseconds: Int)
	case EXAT(timestampSeconds: Int)
	case PXAT(timestampMilliseconds: Int)
	case PERSIST
}

public enum RediSyncBeforeOrAfter: String {
	case before = "BEFORE"
	case after = "AFTER"
}

public enum RediSyncLeftOrRight: String {
	case left = "LEFT"
	case right = "RIGHT"
}

public protocol RediSyncValue { }
extension Int: RediSyncValue { }
extension String: RediSyncValue { }
