//
//  RediSyncSocketInitResponse.swift
//  
//
//  Created by Mike Richards on 6/28/23.
//

import Foundation

class RediSyncSocketInitResponse: RediSyncSocketResponse
{
	let key: String
	
	override init?(_ data: [Any]?) {
		guard let dataDict = data?.first as? [String: Any], let key = dataDict["key"] as? String else { return nil }
		
		self.key = key

		super.init(data)
	}
}
