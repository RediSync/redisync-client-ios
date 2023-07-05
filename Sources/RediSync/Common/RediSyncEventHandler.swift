//
//  RediSyncEventHandler.swift
//  
//
//  Created by Mike Richards on 6/26/23.
//

import Foundation

class RediSyncEventHandler<T>: NSObject
{
	public let event: String
	public let handler: RediSyncEventHandlerCallback<T>
	public let id = UUID()
	
	public internal(set) var isActive = true

	private let once: Bool
	
	init(_ event: String, once: Bool = false, handler: @escaping RediSyncEventHandlerCallback<T>) {
		self.event = event
		self.once = once
		self.handler = handler
	}
	
	func emit(_ args: T?) {
		guard isActive else { return }
		
		if once {
			isActive = false
		}
		
		handler(args)
	}
}

public typealias RediSyncEventHandlerCallback<T> = (T?) -> ()

@available(macOS 10.15, *)
open class RediSyncEventEmitter: NSObject
{
	private var handlers: [UUID: Any] = [:]
	private var handlerIdsByEvent: [String: [UUID]] = [:]
	
	open func emit(_ event: String, args: Any? = nil) {
		let handlerIds = handlerIdsByEvent[event] ?? []
		
		Task {
			for handlerId in handlerIds {
				handler(handlerId)?.emit(args)
			}
		}
	}
	
	@objc
	open func off(_ event: String) {
		let handlerIds = handlerIdsByEvent[event] ?? []
		
		for handlerId in handlerIds {
			handler(handlerId)?.isActive = false
			handlers[handlerId] = nil
		}
		
		handlerIdsByEvent[event] = nil
	}
	
	@objc
	open func off(_ event: String, id: UUID) {
		handler(id)?.isActive = false
		handlerIdsByEvent[event] =  handlerIdsByEvent[event]?.filter { $0 != id }
		handlers[id] = nil
	}
	
	@objc
	open func off(id: UUID) {
		if let handler = handler(id) {
			handler.isActive = false
			handlerIdsByEvent[handler.event] =  handlerIdsByEvent[handler.event]?.filter { $0 != id }
		}
		
		handlers[id] = nil
	}

	@objc
	@discardableResult
	open func on(_ event: String, once: Bool = false, handler: @escaping RediSyncEventHandlerCallback<Any>) -> UUID {
		let eventHandler = RediSyncEventHandler(event, once: once, handler: handler)
		
		handlers[eventHandler.id] = eventHandler
		handlerIdsByEvent[event] = handlerIdsByEvent[event] ?? []
		handlerIdsByEvent[event]!.append(eventHandler.id)
		
		return eventHandler.id
	}
	
	@discardableResult
	open func on<T>(_ event: String, once: Bool = false, handler: @escaping RediSyncEventHandlerCallback<T>) -> UUID {
		let eventHandler = RediSyncEventHandler(event, once: once, handler: handler)
		
		handlers[eventHandler.id] = eventHandler
		handlerIdsByEvent[event] = handlerIdsByEvent[event] ?? []
		handlerIdsByEvent[event]!.append(eventHandler.id)

		return eventHandler.id
	}

	@objc
	@discardableResult
	open func once(_ event: String, handler: @escaping RediSyncEventHandlerCallback<Any>) -> UUID {
		return on(event, once: true, handler: handler)
	}
	
	@discardableResult
	open func once<T>(_ event: String, handler: @escaping RediSyncEventHandlerCallback<T>) -> UUID {
		return on(event, once: true, handler: handler)
	}
	
	func waitForOneOf(_ events: String...) async {
		return await withCheckedContinuation { continuation in
			var continuationCalled = false
			var eventHandlerIds: [UUID] = []
			
			func done() {
				guard !continuationCalled else { return }
				
				continuationCalled = true
				
				for eventHandlerId in eventHandlerIds {
					self.off(id: eventHandlerId)
				}

				continuation.resume()
			}
			
			for event in events {
				let eventId = once(event) { _ in
					done()
				}
				
				eventHandlerIds.append(eventId)
			}
		}
	}
	
	private func handler(_ handlerId: UUID) -> RediSyncEventHandler<Any>? {
		return handlers[handlerId] as? RediSyncEventHandler<Any>
	}
}
