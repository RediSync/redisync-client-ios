//
//  RediSyncAPIInitResponse.swift
//  
//
//  Created by Mike Richards on 6/27/23.
//

import Foundation

final class RediSyncAPIInitResponse: RediSyncAPIBaseResponse
{
	let apiUrl: URL?
	let cookie: String?
	let key: String?
	let socketUrls: [URL]?
	let rs: String?
	
	init(_ json: [String: Any], headers: [AnyHashable: Any]?) {
		let apiUrlValue = json["apiUrl"] as? String
		let key = json["key"] as? String
		let socketUrlValues = json["socketUrls"] as? [String]
		
		self.apiUrl = apiUrlValue != nil ? URL(string: apiUrlValue!) : nil
		self.cookie = headers != nil ? headers?["set-cookie"] as? String : nil
		self.key = key
		self.socketUrls = socketUrlValues?.map { URL(string: $0)! }
		self.rs = json["rs"] as? String
		
		super.init(json)
	}
}
