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
	public func del(_ keys: String...) async -> Int {
		await connectIfNotConnected()
		return await sockets?.del(keys: keys) ?? 0
	}
	
	@discardableResult
	public func del(_ keys: [String]) async -> Int {
		await connectIfNotConnected()
		return await sockets?.del(keys: keys) ?? 0
	}
	
	public func get(key: String) async -> String? {
		await connectIfNotConnected()
		return await sockets?.get(key: key)
	}
	
	public func getInt(key: String) async -> Int? {
		await connectIfNotConnected()
		return await sockets?.getInt(key: key)
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
	
	@discardableResult
	private func connectIfNotConnected() async -> Bool {
		guard status != .connected else { return true }
		
		return await connect()
	}
}
