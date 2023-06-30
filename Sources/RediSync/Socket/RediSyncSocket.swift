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
	
	func keys(pattern: String) async -> RediSyncSocketStringArrayResponse? {
		return RediSyncSocketStringArrayResponse(await emitRedis("keys", pattern))
	}
	
	func set(key: String, value: String) async -> RediSyncSocketOKResponse? {
		return RediSyncSocketOKResponse(await emitRedis("set", key, value))
	}
	
	func set(key: String, value: Int) async -> RediSyncSocketOKResponse? {
		return RediSyncSocketOKResponse(await emitRedis("set", key, value))
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
