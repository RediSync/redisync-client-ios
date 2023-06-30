//
//  RediSyncSocketArrayResponse.swift
//
//
//  Created by Mike Richards on 6/29/23.
//

class RediSyncSocketArrayResponse<T>: RediSyncSocketResponse
{
	let value: [T]
	
	override init?(_ data: [Any]?) {
		guard let value = data?.first as? [T] else { return nil }
		
		self.value = value
		
		super.init(data)
	}
}
