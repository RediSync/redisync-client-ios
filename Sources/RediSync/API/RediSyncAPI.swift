//
//  RediSyncAPI.swift
//  
//
//  Created by Mike Richards on 6/26/23.
//

import Foundation
import os

@available(macOS 13.0, *)
final class RediSyncAPI
{
	private let logger = Logger(subsystem: "RediSync", category: "RediSyncAPI")
	private let url: URL
	
	init(url: URL) {
		self.url = url
	}
	
	func initApiCall(appKey: String) async -> RediSyncAPIInitResponse? {
		logger.debug("INIT started")
		
		let params = [
			"iosBundleID": Bundle.main.bundleIdentifier ?? ""
		]

		let result = await request(path: "auth/init/\(appKey.addingPercentEncoding(withAllowedCharacters: .urlHostAllowed)!)", method: .post, params: params)

		if let json = result.json {
			logger.debug("INIT result: \(json, privacy: .public)")
			
			return RediSyncAPIInitResponse(json, headers: result.response?.allHeaderFields)
		}
		
		logger.warning("INIT returned nil")
		
		return nil
	}
	
	private func request(path: any StringProtocol, method: RediSyncRequestMethod = .get, headers: [String: String] = [:], params: [String: Any] = [:]) async -> (json: [String: Any]?, response: HTTPURLResponse?)  {
		let request = RediSyncRequest(url.appending(path: path), method: method, headers: headers, params: params)
		
		let result = await request.fetch()
					
		return (result.data != nil ? try? JSONSerialization.jsonObject(with: result.data!) as? [String: Any] : nil, result.response)
	}
}
