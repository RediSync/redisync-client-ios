//
//  RediSyncSocketResponse.swift
//  
//
//  Created by Mike Richards on 6/28/23.
//

class RediSyncSocketResponse
{
	let data: [Any]?
	let error: RediSyncSocketErrorResponse?
	
	init?(_ data: [Any]?) {
		guard let data = data else { return nil }
		
		self.data = data
		self.error = RediSyncSocketErrorResponse(error: (data.first as? [String: Any])?["error"] as? [String: Any])
	}
}

