import XCTest
@testable import RediSync

final class RediSyncTests: XCTestCase {
    func testExample() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(RediSync().text, "Hello, World!")
    }
}
