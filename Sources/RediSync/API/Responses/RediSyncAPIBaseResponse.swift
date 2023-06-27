//
//  RediSyncAPIBaseReponse.swift
//  
//
//  Created by Mike Richards on 6/27/23.
//

import Foundation

internal class RediSyncAPIBaseResponse
{
	let error: RediSyncAPIErrorResponse?
	
	init(_ json: [String: Any]) {
		if let error = json["error"] as? [String: Any] {
			self.error = RediSyncAPIErrorResponse(error)
		}
		else {
			self.error = nil
		}
	}
}
