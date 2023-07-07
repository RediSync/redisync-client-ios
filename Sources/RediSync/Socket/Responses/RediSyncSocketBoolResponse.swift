//
//  RediSyncSocketBoolResponse.swift
//  
//
//  Created by Mike Richards on 7/7/23.
//

class RediSyncSocketBoolResponse: RediSyncSocketResponse
{
	let value: Bool
	
	override init?(_ data: [Any]?) {
		if let value = data?.first as? Bool {
			self.value = value
		}
		else if let value = data?.first as? Int {
			self.value = value >= 1
		}
		else if let valueString = data?.first as? String, let value = Int(valueString) {
			self.value = value >= 1
		}
		else {
			return nil
		}
				
		super.init(data)
	}
}

