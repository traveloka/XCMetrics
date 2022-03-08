// Copyright (c) 2020 Spotify AB.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import Foundation
import CryptoSwift
import Queues

/// Job that parses the uploaded xcactivitylog and inserts it into a Repository
class ProcessMetricsJob: Job {

    typealias Payload = UploadMetricsRequest

    let logFileRepository: LogFileRepository

    let metricsRepository: MetricsRepository

    /// If true, the user data (userId, machineName) will be hashed and the
    /// User data will be redacted from the log
    let redactUserData: Bool

    /// Queue in which the logs will be processed
    let queue = DispatchQueue(label: "process.metrics.queue", qos: .userInitiated)

    /// Set concurrent processing of logs up to the number of cores
    let semaphore = DispatchSemaphore(value: ProcessInfo.processInfo.processorCount)

    init(logFileRepository: LogFileRepository, metricsRepository: MetricsRepository, redactUserData: Bool) {
        self.logFileRepository = logFileRepository
        self.metricsRepository = metricsRepository
        self.redactUserData = redactUserData
    }

    func dequeue(_ context: QueueContext, _ payload: UploadMetricsRequest) -> EventLoopFuture<Void> {
        logWithTimestamp(context.logger, msg: "[ProcessMetricsJob] message dequeued")

        let eventLoop = context.application.eventLoopGroup.next()
        let promise = eventLoop.makePromise(of: Void.self)

        /// parsing is a blocking call, we execute it in a Dispatch Queue to not block the eventloop
        queue.async {
            self.semaphore.wait()

            defer {
                self.semaphore.signal()
            }
            let logFile: LogFile
            var buildMetrics: BuildMetrics
            do {
                logWithTimestamp(context.logger, msg: "[ProcessMetricsJob] fetching log from \(payload.logURL)")
                logFile = try self.logFileRepository.get(logURL: payload.logURL)
                logWithTimestamp(context.logger, msg: "[ProcessMetricsJob] log fetched to \(logFile.localURL)")
                buildMetrics = try MetricsProcessor.process(metricsRequest: payload,
                                                            logFile: logFile,
                                                            redactUserData: self.redactUserData)
            } catch {
                context.logger.error("[ProcessMetricsJob] error processing log from \(payload.logURL): \(error)")
                promise.fail(error)
                return
            }

            guard let filteredMetrics =  self.filterLog(buildMetrics) else {
                logWithTimestamp(context.logger, msg: "[ProcessMetricsJob] Skipping build failures...")

                promise.succeed(())

                return promise.futureResult
            }

            buildMetrics = filteredMetrics

            logWithTimestamp(context.logger, msg: "[ProcessMetricsJob] log parsed \(payload.logURL)")
            _ = self.metricsRepository.insertBuildMetrics(buildMetrics, using: eventLoop)
                .flatMapAlways { (result) -> EventLoopFuture<Void> in
                    var wasProcessed: Bool = false
                    switch result {
                    case .failure(let error):
                        context.logger.error("[ProcessMetricsJob] error inserting log from \(payload.logURL): \(error)")
                        wasProcessed = false
                        promise.fail(error)
                    case .success:
                        wasProcessed = true
                        promise.succeed(())
                    }
                    return self.removeLocalLog(logFile,
                                               wasProcessed: wasProcessed,
                                               using: eventLoop, context)
                }
                .map { _ -> Void in
                    context.logger.info("[ProcessMetricsJob] finished processing \(payload.logURL)")
                    return ()
                }
        }
        return promise.futureResult
    }

    /**
     * Process and filter log. Since we only will use to benchmark the build time,
     * this method will throw all unused data for POC, such as build errors, warnings, steps, targets, etc.
     *
     * @return nil if the log is shouldn't be stored or BuildMetrics with filtered log.
     */
    private func filterLog(buildMetrics: BuildMetrics) -> BuildMetrics? {
        let trackedModules: [String] = [
            "Flight",
            "GTCore",
            "GTHomepage",
            "Shuttle",
            "Bus",
            "Rail",
            "Train",
            "Rental",
            "Experience",
            "Groceries",
            "Health",
            "Booking"
        ]

        let isProjectShouldBeTracked = trackedModules.contains { element in
            return buildMetrics.build.projectName.contains(element)
        }

        if (buildMetrics.build.buildStatus == "failed" || buildMetrics.build.buildStatus == "finished with errors")
            && !isProjectShouldBeTracked  {
            return nil
        }

        // Filter log
        buildMetrics.targets = [Target]()
        buildMetrics.steps = [Step]()
        buildMetrics.warnings = nil
        buildMetrics.errors = nil
        buildMetrics.notes = nil
    }

    private func removeLocalLog(_ log: LogFile,
                                wasProcessed: Bool,
                                using eventLoop: EventLoop,
                                _ context: QueueContext) -> EventLoopFuture<Void> {
        return eventLoop.submit { () -> Void in
            do {
                try self.logFileRepository.delete(log: log, wasProcessed: wasProcessed)
            } catch {
                context.logger.error("[ProcessMetricsJob] Error removing log from \(log.localURL): \(error)")
            }
        }
    }
}
