//
//  RediSyncSocketInitResponse.swift
//  
//
//  Created by Mike Richards on 6/28/23.
//

import Foundation

class RediSyncSocketInitResponse: RediSyncSocketResponse
{
	let key: String?
	
	required init(_ data: [String : Any]) {
		key = data["key"] as? String
	}
}
