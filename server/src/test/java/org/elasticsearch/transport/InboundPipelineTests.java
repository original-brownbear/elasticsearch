/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class InboundPipelineTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));

    public void testDecodeExceptionIsPropagated() throws IOException {
        BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> {};
        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(recycler);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);

        try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(recycler)) {
            String actionName = "actionName";
            final TransportVersion invalidVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MINIMUM_COMPATIBLE);
            final String value = randomAlphaOfLength(1000);
            final boolean isRequest = randomBoolean();
            final long requestId = randomNonNegativeLong();

            OutboundMessage message;
            if (isRequest) {
                message = new OutboundMessage.Request(
                    threadContext,
                    new TestRequest(value),
                    invalidVersion,
                    actionName,
                    requestId,
                    false,
                    null
                );
            } else {
                message = new OutboundMessage.Response(threadContext, new TestResponse(value), invalidVersion, requestId, false, null);
            }

            final BytesReference reference = message.serialize(streamOutput);
            try (ReleasableBytesReference releasable = ReleasableBytesReference.wrap(reference)) {
                expectThrows(IllegalStateException.class, () -> pipeline.handleBytes(new FakeTcpChannel(), releasable));
            }

            // Pipeline cannot be reused after uncaught exception
            final IllegalStateException ise = expectThrows(
                IllegalStateException.class,
                () -> pipeline.handleBytes(new FakeTcpChannel(), ReleasableBytesReference.wrap(BytesArray.EMPTY))
            );
            assertEquals("Pipeline state corrupted by uncaught exception", ise.getMessage());
        }
    }

}
