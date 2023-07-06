//
//  RediSyncContinuationBlockingDuplicates.swift
//  
//
//  Created by Mike Richards on 6/29/23.
//

@available(macOS 10.15, *)
internal class RediSyncContinuationBlockingDuplicates<T>
{
	private let continuation: CheckedContinuation<T, Never>
	
	private var continuationCalled = false
	
	init(continuation: CheckedContinuation<T, Never>) {
		self.continuation = continuation
	}
	
	func returnResult(_ result: T) {
		guard !continuationCalled else { return }
		
		continuationCalled = true
		
		continuation.resume(returning: result)
	}
}
