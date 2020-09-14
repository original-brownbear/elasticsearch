/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class TransportThreadWatchdog {
    // Only check every 2s to not flood the logs on a blocked thread.
    // We mostly care about long blocks and not random slowness anyway and in tests would randomly catch slow operations that block for
    // less than 2s eventually.
    private static final TimeValue CHECK_INTERVAL = TimeValue.timeValueSeconds(2);

    private final long warnThreshold;
    private final ThreadPool threadPool;
    private final ConcurrentHashMap<Thread, Long> registry = new ConcurrentHashMap<>();

    private final Logger logger;

    private volatile boolean stopped;

    public TransportThreadWatchdog(ThreadPool threadPool, Settings settings, Logger logger) {
        this.threadPool = threadPool;
        warnThreshold = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(settings).nanos() + TimeValue.timeValueMillis(100L).nanos();
        threadPool.schedule(this::logLongRunningExecutions, CHECK_INTERVAL, ThreadPool.Names.GENERIC);
        this.logger = logger;
    }

    public boolean register() {
        Long previousValue = registry.put(Thread.currentThread(), threadPool.relativeTimeInNanos());
        return previousValue == null;
    }

    public void unregister() {
        Long previousValue = registry.remove(Thread.currentThread());
        assert previousValue != null;
        maybeLogElapsedTime(previousValue);
    }

    private void maybeLogElapsedTime(long startTime) {
        long elapsedTime = threadPool.relativeTimeInNanos() - startTime;
        if (elapsedTime > warnThreshold) {
            logger.warn(
                    new ParameterizedMessage("Slow execution on network thread [{} milliseconds]",
                            TimeUnit.NANOSECONDS.toMillis(elapsedTime)),
                    new RuntimeException("Slow exception on network thread"));
        }
    }

    private void logLongRunningExecutions() {
        for (Map.Entry<Thread, Long> entry : registry.entrySet()) {
            final Long blockedSinceInNanos = entry.getValue();
            final long elapsedTimeInNanos = threadPool.relativeTimeInNanos() - blockedSinceInNanos;
            if (elapsedTimeInNanos > warnThreshold) {
                final Thread thread = entry.getKey();
                final String stackTrace =
                        Arrays.stream(thread.getStackTrace()).map(Object::toString).collect(Collectors.joining("\n"));
                final Thread.State threadState = thread.getState();
                if (blockedSinceInNanos == registry.get(thread)) {
                    logger.warn("Potentially blocked execution on network thread [{}] [{}] [{} milliseconds]: \n{}",
                            thread.getName(), threadState, TimeUnit.NANOSECONDS.toMillis(elapsedTimeInNanos), stackTrace);
                }
            }
        }
        if (stopped == false) {
            threadPool.scheduleUnlessShuttingDown(CHECK_INTERVAL, ThreadPool.Names.GENERIC, this::logLongRunningExecutions);
        }
    }

    public void stop() {
        stopped = true;
    }
}
