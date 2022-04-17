/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.FakeTcpChannel;
import org.elasticsearch.transport.Header;
import org.elasticsearch.transport.InboundAggregator;
import org.elasticsearch.transport.InboundDecoder;
import org.elasticsearch.transport.InboundMessage;
import org.elasticsearch.transport.OutboundMessage;
import org.elasticsearch.transport.StatsTracker;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TestRequest;
import org.elasticsearch.transport.TestResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class InboundPipelineTests extends ESTestCase {

    private static final int BYTE_THRESHOLD = 128 * 1024;
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));

    public void testPipelineHandling() throws IOException {
        final List<Tuple<MessageData, Exception>> expected = new ArrayList<>();
        final List<Tuple<MessageData, Exception>> actual = new ArrayList<>();
        final List<ReleasableBytesReference> toRelease = new ArrayList<>();
        final BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> {
            try {
                final Header header = m.getHeader();
                final MessageData actualData;
                final Version version = header.getVersion();
                final boolean isRequest = header.isRequest();
                final long requestId = header.getRequestId();
                final Compression.Scheme compressionScheme = header.getCompressionScheme();
                if (header.isCompressed()) {
                    assertNotNull(compressionScheme);
                } else {
                    assertNull(compressionScheme);
                }
                if (m.isShortCircuit()) {
                    actualData = new MessageData(version, requestId, isRequest, compressionScheme, header.getActionName(), null);
                } else if (isRequest) {
                    final TestRequest request = new TestRequest(m.openOrGetStreamInput());
                    actualData = new MessageData(version, requestId, isRequest, compressionScheme, header.getActionName(), request.value);
                } else {
                    final TestResponse response = new TestResponse(m.openOrGetStreamInput());
                    actualData = new MessageData(version, requestId, isRequest, compressionScheme, null, response.value);
                }
                actual.add(new Tuple<>(actualData, m.getException()));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, recycler);
        final String breakThisAction = "break_this_action";
        final String actionName = "actionName";
        final Predicate<String> canTripBreaker = breakThisAction::equals;
        final TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        circuitBreaker.startBreaking();
        final InboundAggregator aggregator = new InboundAggregator(() -> circuitBreaker, canTripBreaker);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);
        final FakeTcpChannel channel = new FakeTcpChannel();

        final int iterations = randomIntBetween(100, 500);
        long totalMessages = 0;
        long bytesReceived = 0;

        for (int i = 0; i < iterations; ++i) {
            actual.clear();
            expected.clear();
            toRelease.clear();
            try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(recycler)) {
                while (streamOutput.size() < BYTE_THRESHOLD) {
                    final Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
                    final String value = randomRealisticUnicodeOfCodepointLength(randomIntBetween(200, 400));
                    final boolean isRequest = randomBoolean();
                    Compression.Scheme compressionScheme = getCompressionScheme(version);
                    final long requestId = totalMessages++;

                    final MessageData messageData;
                    Exception expectedExceptionClass = null;

                    OutboundMessage message;
                    if (isRequest) {
                        if (rarely()) {
                            messageData = new MessageData(version, requestId, true, compressionScheme, breakThisAction, null);
                            message = new OutboundMessage.Request(
                                threadContext,
                                new TestRequest(value),
                                version,
                                breakThisAction,
                                requestId,
                                false,
                                compressionScheme
                            );
                            expectedExceptionClass = new CircuitBreakingException("", CircuitBreaker.Durability.PERMANENT);
                        } else {
                            messageData = new MessageData(version, requestId, true, compressionScheme, actionName, value);
                            message = new OutboundMessage.Request(
                                threadContext,
                                new TestRequest(value),
                                version,
                                actionName,
                                requestId,
                                false,
                                compressionScheme
                            );
                        }
                    } else {
                        messageData = new MessageData(version, requestId, false, compressionScheme, null, value);
                        message = new OutboundMessage.Response(
                            threadContext,
                            new TestResponse(value),
                            version,
                            requestId,
                            false,
                            compressionScheme
                        );
                    }

                    expected.add(new Tuple<>(messageData, expectedExceptionClass));
                    try (RecyclerBytesStreamOutput temporaryOutput = new RecyclerBytesStreamOutput(recycler)) {
                        Streams.copy(message.serialize(temporaryOutput).streamInput(), streamOutput, false);
                    }
                }

                final BytesReference networkBytes = streamOutput.bytes();
                int currentOffset = 0;
                while (currentOffset != networkBytes.length()) {
                    final int remainingBytes = networkBytes.length() - currentOffset;
                    final int bytesToRead = Math.min(randomIntBetween(1, 32 * 1024), remainingBytes);
                    final BytesReference slice = networkBytes.slice(currentOffset, bytesToRead);
                    try (ReleasableBytesReference reference = new ReleasableBytesReference(slice, () -> {})) {
                        toRelease.add(reference);
                        bytesReceived += reference.length();
                        pipeline.handleBytes(channel, Netty4Utils.toByteBuf(reference));
                        currentOffset += bytesToRead;
                    }
                }

                final int messages = expected.size();
                for (int j = 0; j < messages; ++j) {
                    final Tuple<MessageData, Exception> expectedTuple = expected.get(j);
                    final Tuple<MessageData, Exception> actualTuple = actual.get(j);
                    final MessageData expectedMessageData = expectedTuple.v1();
                    final MessageData actualMessageData = actualTuple.v1();
                    assertEquals(expectedMessageData.requestId, actualMessageData.requestId);
                    assertEquals(expectedMessageData.isRequest, actualMessageData.isRequest);
                    assertEquals(expectedMessageData.compressionScheme, actualMessageData.compressionScheme);
                    assertEquals(expectedMessageData.actionName, actualMessageData.actionName);
                    assertEquals(expectedMessageData.value, actualMessageData.value);
                    if (expectedTuple.v2() != null) {
                        assertNotNull(actualTuple.v2());
                        assertThat(actualTuple.v2(), instanceOf(expectedTuple.v2().getClass()));
                    }
                }

                for (ReleasableBytesReference released : toRelease) {
                    assertFalse(released.hasReferences());
                }
            }

            assertEquals(bytesReceived, statsTracker.getBytesRead());
            assertEquals(totalMessages, statsTracker.getMessagesReceived());
        }
    }

    private static Compression.Scheme getCompressionScheme(Version version) {
        if (randomBoolean()) {
            return null;
        } else {
            if (version.before(Compression.Scheme.LZ4_VERSION)) {
                return Compression.Scheme.DEFLATE;
            } else {
                return randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4);
            }
        }
    }

    public void testDecodeExceptionIsPropagated() throws IOException {
        BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> {};
        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, recycler);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);

        try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(recycler)) {
            String actionName = "actionName";
            final Version invalidVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
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

            expectThrows(
                IllegalStateException.class,
                () -> pipeline.handleBytes(new FakeTcpChannel(), Netty4Utils.toByteBuf(message.serialize(streamOutput)))
            );

            // Pipeline cannot be reused after uncaught exception
            final IllegalStateException ise = expectThrows(
                IllegalStateException.class,
                () -> pipeline.handleBytes(new FakeTcpChannel(), Unpooled.EMPTY_BUFFER)
            );
            assertEquals("Pipeline state corrupted by uncaught exception", ise.getMessage());
        }
    }

    public void testEnsureBodyIsNotPrematurelyReleased() throws IOException {
        BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> {};
        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, recycler);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);

        try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(recycler)) {
            String actionName = "actionName";
            final Version version = Version.CURRENT;
            final String value = randomAlphaOfLength(1000);
            final boolean isRequest = randomBoolean();
            final long requestId = randomNonNegativeLong();

            OutboundMessage message;
            if (isRequest) {
                message = new OutboundMessage.Request(threadContext, new TestRequest(value), version, actionName, requestId, false, null);
            } else {
                message = new OutboundMessage.Response(threadContext, new TestResponse(value), version, requestId, false, null);
            }

            final BytesReference reference = message.serialize(streamOutput);
            final int fixedHeaderSize = TcpHeader.headerSize(Version.CURRENT);
            final int variableHeaderSize = reference.getInt(fixedHeaderSize - 4);
            final int totalHeaderSize = fixedHeaderSize + variableHeaderSize;
            for (int i = 0; i < totalHeaderSize - 1; ++i) {
                try (ReleasableBytesReference slice = ReleasableBytesReference.wrap(reference.slice(i, 1))) {
                    pipeline.handleBytes(new FakeTcpChannel(), Netty4Utils.toByteBuf(slice));
                }
            }

            final int from = totalHeaderSize - 1;
            final BytesReference partHeaderPartBody = reference.slice(from, reference.length() - from - 1);
            ByteBuf buf = Netty4Utils.toByteBuf(partHeaderPartBody);
            pipeline.handleBytes(new FakeTcpChannel(), buf);
            final ByteBuf singleByteSlice = Netty4Utils.toByteBuf(reference.slice(reference.length() - 1, 1));
            assertThat(singleByteSlice.refCnt(), equalTo(1));
            pipeline.handleBytes(new FakeTcpChannel(), singleByteSlice);
            assertThat(buf.refCnt(), equalTo(0));
            assertThat(singleByteSlice.refCnt(), equalTo(0));
        }
    }

    private static class MessageData {

        private final Version version;
        private final long requestId;
        private final boolean isRequest;
        private final Compression.Scheme compressionScheme;
        private final String value;
        private final String actionName;

        private MessageData(
            Version version,
            long requestId,
            boolean isRequest,
            Compression.Scheme compressionScheme,
            String actionName,
            String value
        ) {
            this.version = version;
            this.requestId = requestId;
            this.isRequest = isRequest;
            this.compressionScheme = compressionScheme;
            this.actionName = actionName;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageData that = (MessageData) o;
            return requestId == that.requestId
                && isRequest == that.isRequest
                && Objects.equals(compressionScheme, that.compressionScheme)
                && Objects.equals(version, that.version)
                && Objects.equals(value, that.value)
                && Objects.equals(actionName, that.actionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, requestId, isRequest, compressionScheme, value, actionName);
        }
    }
}
