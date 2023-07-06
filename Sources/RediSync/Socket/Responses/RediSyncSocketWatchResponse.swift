//
//  RediSyncSocketWatchResponse.swift
//  
//
//  Created by Mike Richards on 7/5/23.
//

class RediSyncSocketWatchResponse: RediSyncSocketResponse
{
	let id: String
	
	override init?(_ data: [Any]?) {
		guard let json = data?.first as? [String: Any], let id = json["id"] as? String else { return nil }
		
		self.id = id
		
		super.init(data)
	}

}
