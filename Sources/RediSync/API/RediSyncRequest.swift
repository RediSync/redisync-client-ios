//
//  RediSyncRequest.swift
//  
//
//  Created by Mike Richards on 6/27/23.
//

import Foundation
import os

enum RediSyncRequestMethod: String
{
	case delete = "DELETE"
	case get = "GET"
	case post = "POST"
	case put = "PUT"
}

@available(macOS 11.0, *)
final class RediSyncRequest
{
	private let headers: [String: String]
	private let method: RediSyncRequestMethod
	private let params: [String: Any]
	private let url: URL
	
	init(_ url: URL, method: RediSyncRequestMethod = .get, headers: [String: String] = [:], params: [String: Any] = [:]) {
		self.headers = headers
		self.method = method
		self.params = params
		self.url = url
	}
	
	private let logger = Logger(subsystem: "RediSync", category: "RediSyncRequest")
	
	func fetch() async -> (data: Data?, response: HTTPURLResponse?) {
		var urlString = url.absoluteString
		
		// set parameters as querystring for a GET call
		if method == .get, params.count > 0 {
			let queryString = params.map{ "\($0)=\(($1 as? String)?.addingPercentEncoding(withAllowedCharacters: .urlHostAllowed) ?? $1)" }.joined(separator: "&")
			urlString += "?\(queryString)"
		}
		
		// create request
		var request = URLRequest(url: URL(string: urlString)!)
		request.httpMethod = method.rawValue
		
		// Ignore any cached files. This method ALWAYS makes the request.
		request.cachePolicy = .reloadIgnoringLocalAndRemoteCacheData
		
		// add headers
		for (headerName, headerValue) in headers {
			request.addValue(headerValue, forHTTPHeaderField: headerName)
		}
		
		// If Content-Type is application/json, we need to serialize the json from params
		if !params.isEmpty, method != .get, headers["Content-Type"] == "application/json" {
			request.httpBody = try? JSONSerialization.data(withJSONObject: params)
		}
		
		logger.debug("fetch \(self.method.rawValue, privacy: .public) \(urlString, privacy: .public) \(self.params, privacy: .public)")
		
		// Make request
		let response = try? await URLSession.shared.data(for: request)

		// Parse response
		let data = response?.0
		let httpResponse = response?.1 as? HTTPURLResponse
		
		logger.debug("fetch response \(self.method.rawValue, privacy: .public) \(urlString, privacy: .public) \(httpResponse?.statusCode ?? -1, privacy: .public)")

		return (data, httpResponse)
	}
}
