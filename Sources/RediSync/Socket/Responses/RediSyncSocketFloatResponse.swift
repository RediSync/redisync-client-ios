//
//  RediSyncSocketFloatResponse.swift
//
//
//  Created by Mike Richards on 6/29/23.
//

class RediSyncSocketFloatResponse: RediSyncSocketResponse
{
	let value: Float
	
	override init?(_ data: [Any]?) {
		if let value = data?.first as? Float {
			self.value = value
		}
		else if let valueString = data?.first as? String, let value = Float(valueString) {
			self.value = value
		}
		else if let value = data?.first as? Int {
			self.value = Float(value)
		}
		else {
			return nil
		}
				
		super.init(data)
	}
}
