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
	private let handlersLockQueue = DispatchQueue(label: "redisync.eventemitter.handlers.lock.queue")

	private var handlers: [UUID: RediSyncEventHandlerWrapper] = [:]
	private var handlerIdsByEvent: [String: Set<UUID>] = [:]
	
	open func emit<T>(_ event: String, _ args: T?) {
		let handlers = handlersFor(event: event)

		for handler in handlers {
			if let handler = handler as? RediSyncEventHandler<T> {
				Task {
					handler.emit(args)
				}
			}
		}
	}
	
	open func emit(_ event: String) {
		let handlers = handlersFor(event: event)
		
		for handler in handlers {
			if let handler = handler as? RediSyncEventHandler<Any> {
				Task {
					handler.emit(nil)
				}
			}
			else {
				print("*** ERROR: Handler cannot be assigned as RediSyncEventHandler<Any> for event '\(event)' ***")
			}
		}
	}
	
	@objc
	open func off(_ event: String) {
		removeHandlersFor(event: event)
	}
	
	@objc
	open func off(id: UUID) {
		removeHandler(id: id)
	}

	@objc
	@discardableResult
	open func on(_ event: String, once: Bool = false, handler: @escaping RediSyncEventHandlerCallback<Any>) -> UUID {
		let eventHandler = RediSyncEventHandler(event, once: once, handler: handler)
		
		setHandler(handler: eventHandler)

		return eventHandler.id
	}
	
	@discardableResult
	open func on<T>(_ event: String, once: Bool = false, handler: @escaping RediSyncEventHandlerCallback<T>) -> UUID {
		let eventHandler = RediSyncEventHandler(event, once: once, handler: handler)
		
		setHandler(handler: eventHandler)
		
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
	
	private func handler(id: UUID) -> RediSyncEventHandlerWrapper? {
		return handlersLockQueue.sync { handlers[id] }
	}
	
	private func handlersFor(event: String) -> [RediSyncEventHandlerWrapper] {
		return handlersLockQueue.sync {
			guard let handlerIds = handlerIdsByEvent[event] else { return [] }
			
			var handlers: [RediSyncEventHandlerWrapper] = []
			
			for handlerId in handlerIds {
				if let handler = self.handlers[handlerId] {
					handlers.append(handler)
				}
			}
			
			return handlers
		}
	}
	
	private func removeHandler(id: UUID) {
		handlersLockQueue.sync {
			guard var handler = handlers[id] else { return }
			
			handler.isActive = false
			handlerIdsByEvent[handler.event]?.remove(id)
			handlers[id] = nil
		}
	}
	
	private func removeHandlersFor(event: String) {
		handlersLockQueue.sync {
			guard var handlerIds = handlerIdsByEvent[event] else { return }
			
			for handlerId in handlerIds {
				if var handler = self.handlers[handlerId] {
					handler.isActive = false
					self.handlers[handlerId] = nil
				}
			}
			
			handlerIds.removeAll()
		}
	}
	
	private func setHandler(handler: RediSyncEventHandlerWrapper) {
		handlersLockQueue.sync {
			self.handlers[handler.id] = handler
			
			self.handlerIdsByEvent[handler.event] = self.handlerIdsByEvent[handler.event] ?? Set()
			self.handlerIdsByEvent[handler.event]!.insert(handler.id)
		}
	}
}

fileprivate protocol RediSyncEventHandlerWrapper {
	var event: String { get }
	var id: UUID { get }
	var isActive: Bool { get set }
}
