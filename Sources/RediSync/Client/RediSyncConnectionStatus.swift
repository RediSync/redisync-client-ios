//
//  RediSyncConnectionStatus.swift
//  
//
//  Created by Mike Richards on 6/23/23.
//

@objc
public enum RediSyncConnectionStatus: Int {
	case notConnected = 0
	case connecting = 1
	case connected = 2
}
