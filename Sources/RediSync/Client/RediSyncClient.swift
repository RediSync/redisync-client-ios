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
			await waitForOneOf("connect", "error")
			return status == .connected
		}
		
		status = .connecting
		
		logger.debug("Connecting to API")
		
		let initResult = await primaryApi.initApiCall(appKey: appKey)
		
		if let apiUrl = initResult?.apiUrl {
			api = RediSyncAPI(url: apiUrl)
			
			status = .connected
			
			return true
		}
		
		status = .notConnected
		
		emit("error", args: "RediSync Connection Error")
		
		return false
	}
}
