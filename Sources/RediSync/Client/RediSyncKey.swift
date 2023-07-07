import Foundation
import os

@available(macOS 13.0, *)
@objcMembers
public class RediSyncKey<T>: RediSyncEventEmitter
{
	public private(set) var key: String
	public private(set) var value: T?
	
	public var isWatching: Bool {
		get { return id != nil }
		set {
			guard newValue != isWatching else { return }
			
			if isWatching {
				Task { await startWatching() }
			}
			else {
				Task { await stopWatching() }
			}
		}
	}
	
	private let logger: Logger
	private let valueRetriever: RediSyncKeyValueRetriever<T>

	private var id: String?
	private var lastSocketEventId: String?
	private var onReconnectEventId: UUID?
	private var onWatchEventIds: [UUID]?
	private weak var sockets: RediSyncSocketManager?
	
	private init(key: String, sockets: RediSyncSocketManager?, valueRetriever: @escaping RediSyncKeyValueRetriever<T>) {
		self.key = key
		self.sockets = sockets
		self.valueRetriever = valueRetriever
		logger = Logger(subsystem: "RediSync", category: "RediSyncKey('\(key)')")

		super.init()
	}
	
	deinit {
//		Task { await stopWatching() }
	}
	
	@discardableResult
	func startWatching() async -> Self {
		guard !isWatching else { return self }
		
		guard let sockets = sockets else {
			id = nil
			return self
		}
		
		id = await sockets.watch(key: key)
		
		if let id = id {
			if let oldOnWatchEventIds = onWatchEventIds {
				sockets.offSocketEvent(oldOnWatchEventIds)
			}
			
			if let oldOnReconnectEventId = onReconnectEventId {
				sockets.off(id: oldOnReconnectEventId)
			}
			
			onWatchEventIds = sockets.onSocketEvent("watch:::\(id)") { [weak self] data in
				guard let self = self else { return }
				
				Task { await self.onKeyEvent(data: data) }
			}
			
			onReconnectEventId = sockets.on("connected") { [weak self] _ in
				guard let self = self else { return }

				Task { await self.onReconnect() }
			}
		}
		
		return self
	}
	
	func stopWatching() async {
		guard let id = id, let sockets = sockets else { return }
		
		if await sockets.stopWatching(watcherId: id) == true {
			self.id = nil
			
			if let oldOnWatchEventIds = onWatchEventIds {
				sockets.offSocketEvent(oldOnWatchEventIds)
			}
			
			if let oldOnReconnectEventId = onReconnectEventId {
				sockets.off(id: oldOnReconnectEventId)
			}
		}
	}
	
	private func onKeyEvent(data: [String: Any]) async {
		logger.debug("onKeyEvent(\(data, privacy: .public))")
		
		guard let action = data["action"] as? String, let id = data["id"] as? String, id != lastSocketEventId else { return }

		lastSocketEventId = id
		
		switch action {
		case "changed":
			await retrieveValue()
		
		case "deleted":
			value = nil
			emit("deleted")
		
		case "renamed":
			let wasWatching = isWatching
			
			await stopWatching()
			
			if let newKey = data["newKey"] as? String {
				key = newKey
			}
			
			if wasWatching {
				await startWatching()
			}
		
		default:
			break
		}
	}
	
	private func onReconnect() async {
		let wasWatching = isWatching
		
		id = nil
		
		if wasWatching {
			await startWatching()
		}
	}
	
	@discardableResult
	private func retrieveValue() async -> Self {
		logger.debug("retrieveValue()")
		
		value = await valueRetriever()
		
		emit("changed", value)
		
		return self
	}
	
	static func forKey(_ key: String, sockets: RediSyncSocketManager?, valueRetriever: @escaping RediSyncKeyValueRetriever<T>) async -> RediSyncKey<T> {
		return await RediSyncKey(key: key, sockets: sockets, valueRetriever: valueRetriever).retrieveValue()
	}
}

enum RediSyncKeyType {
	case int
	case hash
	case list
	case string
}

@available(macOS 13.0, *)
//typealias RediSyncKeyValueRetriever<T> = (RediSyncClient) async -> (T?)
typealias RediSyncKeyValueRetriever<T> = () async -> T?
