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
