//
//  RediSyncSocketIntResponse.swift
//  
//
//  Created by Mike Richards on 6/29/23.
//

class RediSyncSocketIntResponse: RediSyncSocketResponse
{
	let value: Int
	
	override init?(_ data: [Any]?) {
		if let value = data?.first as? Int {
			self.value = value
		}
		else if let valueString = data?.first as? String, let value = Int(valueString) {
			self.value = value
		}
		else {
			return nil
		}
				
		super.init(data)
	}
}
