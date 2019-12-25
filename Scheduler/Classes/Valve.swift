//
//  Valve.swift
//  Scheduler
//
//  Created by Valo on 2019/12/24.
//

import Foundation

fileprivate var QueueNum = 0

public struct Valve {
    public class Merge<Key: Hashable, Value: Equatable> {
        public var interval: TimeInterval = 0.1
        public var maxMergeCount = 100
        public var mergeAction: (([Value], [Key: Value]) -> Void)?
        private var objects: [Value] = []
        private var keyObjects: [Key: Value] = [:]
        private let lock = DispatchSemaphore(value: 1)
        private lazy var queueNum: Int = { QueueNum += 1; return QueueNum }()
        private lazy var queue = DispatchQueue(label: "com.valo.valve.merge.\(self.queueNum)", qos: .utility, attributes: [], autoreleaseFrequency: .inherit, target: nil)

        public func add(_ object: Value, for key: Key) {
            lock.wait()
            if keyObjects.count == 0 {
                let deadline = DispatchWallTime.now()
                queue.asyncAfter(wallDeadline: deadline) {
                    self.runMerge()
                }
            }
            if let oldobj = keyObjects[key], let idx = objects.firstIndex(of: oldobj) {
                objects.remove(at: idx)
            }
            objects.append(object)
            keyObjects[key] = object
            if objects.count >= maxMergeCount {
                queue.async {
                    self.runMerge()
                }
            }
            lock.signal()
        }

        private func runMerge() {
            guard let block = mergeAction else { return }
            lock.wait()
            let objs = objects
            let keyobjs = keyObjects
            block(objs, keyobjs)
            objects.removeAll()
            keyObjects.removeAll()
            lock.signal()
        }
    }

    class Limit<Value: Equatable> {
        public var interval: TimeInterval = 0.1
        public var maxMergeCount = 100
        public var limitAction: ((Value) -> Void)?
        private var objects: [Value] = []
        private var running = false

        private var lock = DispatchSemaphore(value: 1)
        private lazy var queueNum: Int = { QueueNum += 1; return QueueNum }()
        private lazy var queue = DispatchQueue(label: "com.valo.valve.limit.\(self.queueNum)", qos: .utility, attributes: [], autoreleaseFrequency: .inherit, target: nil)
        private lazy var timer: DispatchSourceTimer = {
            let _timer = DispatchSource.makeTimerSource()
            let deadline = DispatchWallTime.distantFuture
            let repeating = Double(self.interval) * Double(NSEC_PER_SEC)
            _timer.schedule(wallDeadline: deadline, repeating: repeating)
            _timer.setEventHandler {
                self.poll()
            }
            return _timer
        }()

        deinit {
            timer.cancel()
        }

        func add(_ object: Value) {
            lock.wait()
            if objects.count == 0 && !running {
                running = true
                timer.resume()
            }
            objects.append(object)
            lock.signal()
        }

        private func poll() {
            lock.wait()
            if let object = objects.first {
                objects.remove(at: 0)
                if objects.count == 0 && running {
                    timer.suspend()
                    running = false
                }
                if let block = limitAction {
                    block(object)
                }
            }
            lock.signal()
        }
    }

    public class Last {
        private class Item: NSObject {
            var interval: TimeInterval = 0.2
            var action: (() -> Void)?

            func execute(_ action: () -> Void) {
                let selector = #selector(_execute)
                Item.cancelPreviousPerformRequests(withTarget: self, selector: selector, object: nil)
                perform(selector, with: nil, afterDelay: interval)
            }

            @objc func _execute() {
                if let block = action {
                    block()
                }
            }
        }

        private static var items: [String: Item] = [:]

        private class func item(for identifier: String) -> Item {
            if let item = items[identifier] {
                return item
            }
            let item = Item()
            items[identifier] = item
            return item
        }

        public class func set(interval: TimeInterval, for identifier: String) {
            let i = item(for: identifier)
            i.interval = interval
        }

        public class func execute(_ action: () -> Void, for identifier: String) {
            let i = item(for: identifier)
            i.execute(action)
        }
    }
}
