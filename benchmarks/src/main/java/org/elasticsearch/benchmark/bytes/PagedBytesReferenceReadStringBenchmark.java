/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.bytes;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.PagedBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class PagedBytesReferenceReadStringBenchmark {

    @Param(value = { "1000000" })
    private int stringCount;

    private BytesReference pagedBytes;

    private StreamInput streamInput;

    @Setup
    public void initResults() throws IOException {
        final BytesStreamOutput tmp = new BytesStreamOutput();
        for (int i = 0; i < stringCount; i++) {
            tmp.writeString(i + "test" + i);
        }
        pagedBytes = tmp.bytes();
        if (pagedBytes instanceof PagedBytesReference == false) {
            throw new AssertionError("expected PagedBytesReference but saw [" + pagedBytes.getClass() + "]");
        }
        this.streamInput = pagedBytes.streamInput();
    }

    @Benchmark
    public long readString() throws IOException {
        long res = 0L;
        streamInput.reset();
        for (int i = 0; i < stringCount; i++) {
            res = res ^ streamInput.readString().length();
        }
        return res;
    }
}
