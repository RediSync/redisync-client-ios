//
//  RediSyncSocketDictResponse.swift
//  
//
//  Created by Mike Richards on 6/30/23.
//

class RediSyncSocketDictResponse: RediSyncSocketResponse
{
	let value: [String: String]
	
	override init?(_ data: [Any]?) {
		guard let value = data?.first as? [String: String] else { return nil }
		
		self.value = value
		
		super.init(data)
	}
}
