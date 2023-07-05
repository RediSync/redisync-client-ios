//
//  RediSyncSocketDoubleResponse.swift
//  
//
//  Created by Mike Richards on 7/5/23.
//

class RediSyncSocketDoubleResponse: RediSyncSocketResponse
{
	let value: Double
	
	override init?(_ data: [Any]?) {
		if let value = data?.first as? Double {
			self.value = value
		}
		else if let valueString = data?.first as? String, let value = Double(valueString) {
			self.value = value
		}
		else if let value = data?.first as? Int {
			self.value = Double(value)
		}
		else {
			return nil
		}
				
		super.init(data)
	}
}
