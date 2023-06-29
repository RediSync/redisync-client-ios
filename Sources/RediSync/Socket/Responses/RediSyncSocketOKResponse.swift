//
//  RediSyncSocketOKResponse.swift
//  
//
//  Created by Mike Richards on 6/29/23.
//

class RediSyncSocketOKResponse: RediSyncSocketStringResponse
{
	var ok: Bool {
		return value == "OK"
	}
}
