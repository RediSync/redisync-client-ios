//
//  RediSyncSocketSetResponse.swift
//  
//
//  Created by Mike Richards on 6/28/23.
//

import Foundation

class RediSyncSocketSetResponse: RediSyncSocketResponse
{
	let ok: Bool
	
	override init?(_ data: [Any]?) {
		guard let ok = data?.first as? String else { return nil }
		
		self.ok = ok == "OK"
		
		super.init(data)
	}
}
