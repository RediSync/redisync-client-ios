//
//  RediSyncContinuationBlockingDuplicatesAndNils.swift
//  
//
//  Created by Mike Richards on 7/5/23.
//

@available(macOS 10.15, *)
internal class RediSyncContinuationBlockingDuplicatesAndNils<T>: RediSyncContinuationBlockingDuplicates<T>
{
	private let maxNils: Int
	
	private var results = 0
	
	init(continuation: CheckedContinuation<T, Never>, maxNils: Int) {
		self.maxNils = maxNils
		
		super.init(continuation: continuation)
	}
	
	override func returnResult(_ result: T) {
		results += 1
		
		if (result as T?) != nil || results >= maxNils {
			super.returnResult(result)
		}
	}
}
