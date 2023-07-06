//
//  RediSyncEventHandler.swift
//  
//
//  Created by Mike Richards on 6/26/23.
//

import Foundation

class RediSyncEventHandler<T>: NSObject, RediSyncEventHandlerWrapper
{
	public let event: String
	public let handler: RediSyncEventHandlerCallback<T>
	public let id = UUID()
	
	public var isActive = true

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
	private var handlers: [UUID: RediSyncEventHandlerWrapper] = [:]
	private var handlerIdsByEvent: [String: [UUID]] = [:]
	
	open func emit<T>(_ event: String, _ args: T?) {
		let handlerIds = handlerIdsByEvent[event] ?? []
		
		Task {
			for handlerId in handlerIds {
				let handler = handlers[handlerId] as? RediSyncEventHandler<T>
				handler?.emit(args)
			}
		}
	}
	
	open func emit(_ event: String) {
		let handlerIds = handlerIdsByEvent[event] ?? []
		
		Task {
			for handlerId in handlerIds {
				let handler = handlers[handlerId] as? RediSyncEventHandler<Any>
				handler?.emit(nil)
			}
		}
	}
	
	@objc
	open func off(_ event: String) {
		let handlerIds = handlerIdsByEvent[event] ?? []
		
		for handlerId in handlerIds {
			handlers[handlerId]?.isActive = false
			handlers[handlerId] = nil
		}
		
		handlerIdsByEvent[event] = nil
	}
	
	@objc
	open func off(_ event: String, id: UUID) {
		handlers[id]?.isActive = false
		handlerIdsByEvent[event] =  handlerIdsByEvent[event]?.filter { $0 != id }
		handlers[id] = nil
	}
	
	@objc
	open func off(id: UUID) {
		if var handler = handlers[id] {
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
}

fileprivate protocol RediSyncEventHandlerWrapper {
	var event: String { get }
	var isActive: Bool { get set }
}
