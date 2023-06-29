//
//  RediSyncSocketErrorResponse.swift
//  
//
//  Created by Mike Richards on 6/28/23.
//

class RediSyncSocketErrorResponse
{
	let code: String
	
	init?(error: [String: Any]?) {
		guard let error = error, let code = error["code"] as? String else { return nil }
		
		self.code = code
	}
}
