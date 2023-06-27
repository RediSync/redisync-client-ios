//
//  RediSyncAPIErrorResponse.swift
//  
//
//  Created by Mike Richards on 6/27/23.
//

import Foundation

final class RediSyncAPIErrorResponse
{
	let code: String
	
	init?(_ json: [String: Any]) {
		guard let code = json["code"] as? String else { return nil }
		
		self.code = code
	}
}
