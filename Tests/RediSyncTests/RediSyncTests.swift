import XCTest
@testable import RediSync

@available(macOS 13.0, *)
final class RediSyncTests: XCTestCase {
    func testExample() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(RediSync().text, "Hello, World!")
    }
	
//	func hmm() throws {
//		let a = "YES"
//		
//		XCTAssertEqual(a, "YES")
//	}
//	
//	func connect() async throws {
//		let client = RediSyncClient(appKey: "+7glquavqwnTLMObXgqrQQYcvYHQH/zrmA8CQq0W")
//		
//		let didConnect = await client.connect()
//		
//		XCTAssertEqual(didConnect, true)
//	}
}
