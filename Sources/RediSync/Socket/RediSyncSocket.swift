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
	
	@discardableResult
	private func connect(state: RediSyncSocketState = .connecting) async -> Bool {
		guard state == .connecting || state == .reconnecting else { return false }
		
		guard self.state != .connected else { return true }
		
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
	
	func dispose() {
		logger.debug("dispose")
		
		socketManager?.disconnect()
	}
	
	private func emitToSocket<T: RediSyncSocketResponse>(_ event: String, params: [String: Any]) async -> T? {
		logger.debug("emitToSocket(\(event, privacy: .public): \(params, privacy: .public)")
		
		return await withCheckedContinuation { continuation in
			socket?.emitWithAck(event, with: [params]).timingOut(after: 10) { [weak self] data in
				self?.logger.debug("emitToSocket(\(event, privacy: .public) ack: \(data, privacy: .public)")

				guard let dataDictionary = data.first as? [String: Any] else {
					continuation.resume(returning: nil)
					return
				}
				
				continuation.resume(returning: T(dataDictionary) )
			}
		}
	}
	
	private func initConnection() async {
		logger.debug("Initializing connection")
		
		let result: RediSyncSocketInitResponse? = await emitToSocket(
			"init",
			params: [
				"key": key
			]
		)
		
		guard let result = result, let key = result.key else {
			logger.error("Initialization failed")
			state = .notConnected
			return
		}
		
		self.key = key
		
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
