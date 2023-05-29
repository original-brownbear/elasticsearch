/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.common.util.concurrent;

import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(
    value = 1,
    jvmArgs = {
        "-agentpath:/home/brownbear/asycp/async-profiler-2.8-linux-x64/build/libasyncProfiler.so=start,event=cpu,file=profile2.html",
        "-Xmx1G",
        "-Xms1G" }
)
@OperationsPerInvocation(2621440)
public class KeyedLockBenchmark {

    static final int SIZE =
        org.elasticsearch.benchmark.common.util.concurrent.KeyedLockBenchmark.class.getAnnotation(
        OperationsPerInvocation.class
    ).value();

    private KeyedLock<Long> keyedLock;

    @Setup
    public void init() {
        keyedLock = new KeyedLock<>();
    }

    @Benchmark
    public void read() {
        for (int i = 0; i < SIZE; i++) {
            try (var ignored = keyedLock.acquire(1L)) {
                try (var ignored2 = keyedLock.acquire(2L)) {
                    try (var ignored3 = keyedLock.acquire(3L)) {
                    }
                }
            }
        }
    }
}
