//
//  RediSyncTestClientFactory.swift
//  
//
//  Created by Mike Richards on 6/29/23.
//

import RediSync
import XCTest

@available(macOS 13.0, *)
class RediSyncTestClientFactory
{
	static let testAppKey = "qh73BqoXEbX725hIHvGvQRdbJptHpIRJkD9zuFE6"
	
	@discardableResult
	static func create() async throws -> RediSyncClient {
		let client = RediSyncClient(appKey: testAppKey)
		
		let didConnect = await client.connect()
		
		if !didConnect {
			XCTFail("RediSync client did not successfully connect")
		}
		
		return client
	}
}
