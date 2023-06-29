//
//  RediSyncSocketStringResponse.swift
//  
//
//  Created by Mike Richards on 6/29/23.
//

class RediSyncSocketStringResponse: RediSyncSocketResponse
{
	let value: String
	
	override init?(_ data: [Any]?) {
		guard let value = data?.first as? String else { return nil }
		
		self.value = value
		
		super.init(data)
	}
}
