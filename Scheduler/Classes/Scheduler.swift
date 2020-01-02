//
//  Scheduler.swift
//  Scheduler
//
//  Created by Valo on 2019/12/24.
//

import Foundation

public enum Policy {
    case fifo, lifo
    // case priority
}

public enum State: Int {
    case running = 0, suspend, canceling, completed
}

public protocol Task: Equatable {
    associatedtype Key where Key: Hashable
    var id: Key { get }
    var state: State { get }
    var progress: Progress { get }
    var priority: Float { get set }
    func resume()
    func suspend()
    func cancel()
}

public extension Task {
    static func == (lhs: Self, rhs: Self) -> Bool {
        return lhs.id == rhs.id
    }
}

fileprivate struct Extra {
    var suspendTime: CFAbsoluteTime = 0
    var updateAt: CFAbsoluteTime = 0
    var lastProgress: Double = 0
}

fileprivate var QueueNum = 0
fileprivate var ValveNum = 0

public final class Scheduler<Element: Task>: NSObject {
    // MARK: var

    public var maxActivations: Int = 8
    public var timeout: TimeInterval = 60
    public var durationOfSuspension: TimeInterval = 30
    public var interval: TimeInterval = 0.1
    public var policy: Policy = .fifo

    public private(set) var runningTasks: [Element] = []

    private var suspendTasks: [Element] = []
    private var allTasks: [Element.Key: Element] = [:]
    private var allExtras: [Element.Key: Extra] = [:]
    private lazy var valve: Valve.Merge<Int, Int> = {
        let _valve = Valve.Merge<Int, Int>()
        _valve.mergeAction = { _, _ in
            self.pollQueue.async {
                self.poll()
            }
        }
        return _valve
    }()

    private lazy var queueNum: Int = { QueueNum += 1; return QueueNum }()
    private lazy var pollQueue = DispatchQueue(label: "com.valo.scheduler.poll.\(self.queueNum)", qos: .background, attributes: [], autoreleaseFrequency: .inherit, target: nil)
    private lazy var queue = DispatchQueue(label: "com.valo.scheduler.task.\(self.queueNum)", qos: .utility, attributes: [], autoreleaseFrequency: .inherit, target: nil)

    public var autoPoll: Bool = false {
        didSet {
            lock.wait()
            if autoPoll {
                timer.resume()
            } else {
                timer.suspend()
            }
            lock.signal()
        }
    }

    private var lock = DispatchSemaphore(value: 1)
    private lazy var timer: DispatchSourceTimer = {
        let _timer = DispatchSource.makeTimerSource()
        let deadline = DispatchWallTime.distantFuture
        let repeating = Double(self.interval) * Double(NSEC_PER_SEC)
        _timer.schedule(wallDeadline: deadline, repeating: repeating)
        _timer.setEventHandler {
            self.pollQueue.async {
                self.poll()
            }
        }
        return _timer
    }()

    deinit {
        timer.cancel()
    }

    // MARK: - public

    public init(_ policy: Policy) {
        self.policy = policy
    }

    public func manualPoll() {
        ValveNum += 1
        valve.add(ValveNum, for: ValveNum)
    }

    public func add(_ task: Element) {
        add([task])
    }

    public func add(_ tasks: [Element]) {
        lock.wait()
        var removeTasks: [Element] = []
        for task in tasks {
            if let oldtask = allTasks[task.id] {
                removeTasks.append(oldtask)
                if oldtask.state.rawValue < State.canceling.rawValue {
                    queue.async { oldtask.cancel() }
                }
            }
        }
        runningTasks.removeAll { removeTasks.contains($0) }
        removeTasks.forEach { allTasks.removeValue(forKey: $0.id); allExtras.removeValue(forKey: $0.id) }

        tasks.forEach { allTasks[$0.id] = $0 }
        if policy == .lifo {
            runningTasks.insert(contentsOf: tasks, at: 0)
        } else {
            runningTasks.append(contentsOf: tasks)
        }
        lock.signal()
    }

    public func suspend(_ taskid: Element.Key) {
        suspend([taskid])
    }

    public func suspend(_ taskids: [Element.Key]) {
        lock.wait()
        let changes = runningTasks.filter { taskids.contains($0.id) }
        for task in changes {
            if task.state == .running {
                queue.async { task.suspend() }
            }
        }
        runningTasks.removeAll { changes.contains($0) }
        suspendTasks.append(contentsOf: changes)
        lock.signal()
    }

    public func resume(_ taskid: Element.Key) {
        resume([taskid])
    }

    public func resume(_ taskids: [Element.Key]) {
        lock.wait()
        let changes = suspendTasks.filter { taskids.contains($0.id) }
        suspendTasks.removeAll { changes.contains($0) }
        runningTasks.append(contentsOf: changes)
        lock.signal()
    }

    public func cancel(_ taskid: Element.Key) {
        cancel([taskid])
    }

    public func cancel(_ taskids: [Element.Key]) {
        lock.wait()
        let changes = allTasks.filter { taskids.contains($0.key) }.values
        for task in changes {
            if task.state.rawValue < State.canceling.rawValue {
                queue.async { task.cancel() }
            }
        }
        suspendTasks.removeAll { changes.contains($0) }
        runningTasks.removeAll { changes.contains($0) }
        taskids.forEach { allTasks.removeValue(forKey: $0); allExtras.removeValue(forKey: $0) }
        lock.signal()
    }

    public func cancelAll() {
        lock.wait()
        for task in runningTasks {
            if task.state.rawValue < State.canceling.rawValue {
                queue.async { task.cancel() }
            }
        }
        runningTasks.removeAll()
        suspendTasks.removeAll()
        allTasks.removeAll()
        allExtras.removeAll()
        lock.signal()
    }

    public func prioritize(_ taskid: Element.Key) {
        prioritize([taskid])
    }

    public func prioritize(_ taskids: [Element.Key]) {
        let changes = runningTasks.filter { taskids.contains($0.id) }
        runningTasks.removeAll { taskids.contains($0.id) }
        runningTasks.insert(contentsOf: changes, at: 0)
    }

    public subscript(key: Element.Key) -> Element? {
        get {
            return allTasks[key]
        }
        set(newValue) {
            allTasks[key] = newValue
        }
    }

    // MARK: - private

    private func poll() {
        guard runningTasks.count > 0 else { return }
        lock.wait()
        var completedTasks: [Element] = []
        var running = 0, pause = 0, resume = 0
        let now = CFAbsoluteTimeGetCurrent()
        for task in runningTasks {
            var extra = allExtras[task.id]
            if extra == nil {
                extra = Extra()
                allExtras[task.id] = extra
            }

            switch task.state {
            case .running:
                let progress = task.progress.fractionCompleted
                let delay = progress < extra!.lastProgress && now - extra!.updateAt > timeout
                if running >= maxActivations || delay {
                    if delay { extra!.suspendTime = now }
                    pause += 1
                    queue.async { task.suspend() }
                } else {
                    extra!.updateAt = now
                    running += 1
                }
            case .suspend:
                let wakeup = extra!.suspendTime == 0 || (now - extra!.suspendTime > durationOfSuspension)
                if running < maxActivations && wakeup {
                    queue.async { task.resume() }
                    running += 1
                    resume += 1
                    extra!.updateAt = now
                    extra!.suspendTime = 0
                    extra!.lastProgress = task.progress.fractionCompleted
                }
            default:
                completedTasks.append(task)
            }
        }
        runningTasks.removeAll { completedTasks.contains($0) }
        completedTasks.forEach { allTasks.removeValue(forKey: $0.id); allExtras.removeValue(forKey: $0.id) }
        if !autoPoll {
            delayPoll()
        }
        lock.signal()
    }

    private func delayPoll() {
        let selector = #selector(_delayPoll)
        NSObject.cancelPreviousPerformRequests(withTarget: self, selector: selector, object: nil)
        perform(selector)
    }

    @objc private func _delayPoll() {
        manualPoll()
    }
}
