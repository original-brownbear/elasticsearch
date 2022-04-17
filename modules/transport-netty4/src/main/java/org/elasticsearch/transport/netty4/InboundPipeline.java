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
import io.netty.handler.codec.ByteToMessageDecoder;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.Header;
import org.elasticsearch.transport.InboundAggregator;
import org.elasticsearch.transport.InboundDecoder;
import org.elasticsearch.transport.InboundMessage;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.StatsTracker;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class InboundPipeline implements Releasable {

    private static final ThreadLocal<ArrayList<Object>> fragmentList = ThreadLocal.withInitial(ArrayList::new);
    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final LongSupplier relativeTimeInMillis;
    private final StatsTracker statsTracker;
    private final InboundDecoder decoder;
    private final InboundAggregator aggregator;
    private final BiConsumer<TcpChannel, InboundMessage> messageHandler;
    private Exception uncaughtException;
    private ByteBuf pending = null;
    private boolean isClosed = false;

    public InboundPipeline(
        Version version,
        StatsTracker statsTracker,
        Recycler<BytesRef> recycler,
        LongSupplier relativeTimeInMillis,
        Supplier<CircuitBreaker> circuitBreaker,
        Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction,
        BiConsumer<TcpChannel, InboundMessage> messageHandler,
        boolean ignoreDeserializationErrors
    ) {
        this(
            statsTracker,
            relativeTimeInMillis,
            new InboundDecoder(version, recycler),
            new InboundAggregator(circuitBreaker, registryFunction, ignoreDeserializationErrors),
            messageHandler
        );
    }

    public InboundPipeline(
        StatsTracker statsTracker,
        LongSupplier relativeTimeInMillis,
        InboundDecoder decoder,
        InboundAggregator aggregator,
        BiConsumer<TcpChannel, InboundMessage> messageHandler
    ) {
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.statsTracker = statsTracker;
        this.decoder = decoder;
        this.aggregator = aggregator;
        this.messageHandler = messageHandler;
    }

    @Override
    public void close() {
        isClosed = true;
        Releasables.closeExpectNoException(decoder, aggregator, () -> {
            final ByteBuf p = pending;
            if (p != null) {
                p.release();
            }
            pending = null;
        });
    }

    public void handleBytes(TcpChannel channel, ByteBuf byteBuf) throws IOException {
        if (uncaughtException != null) {
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            doHandleBytes(channel, byteBuf);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    private void doHandleBytes(TcpChannel channel, ByteBuf reference) throws IOException {
        channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
        statsTracker.markBytesRead(reference.readableBytes());
        pending = ByteToMessageDecoder.COMPOSITE_CUMULATOR.cumulate(
            reference.alloc(),
            pending == null ? Unpooled.EMPTY_BUFFER : pending,
            reference
        );

        final ArrayList<Object> fragments = fragmentList.get();
        boolean continueHandling = true;

        while (continueHandling && isClosed == false) {
            boolean continueDecoding = true;
            while (continueDecoding && pending != null) {
                final ByteBuf slice = pending.slice();
                final int bytesDecoded = decoder.decode(
                    new ReleasableBytesReference(Netty4Utils.toBytesReference(slice), new ByteBufRefCounted(slice)),
                    fragments::add
                );
                if (bytesDecoded != 0) {
                    releasePendingBytes(bytesDecoded);
                    if (fragments.isEmpty() == false && endOfMessage(fragments.get(fragments.size() - 1))) {
                        continueDecoding = false;
                    }
                } else {
                    continueDecoding = false;
                }
            }

            if (fragments.isEmpty()) {
                continueHandling = false;
            } else {
                try {
                    forwardFragments(channel, fragments);
                } finally {
                    for (Object fragment : fragments) {
                        if (fragment instanceof ReleasableBytesReference) {
                            ((ReleasableBytesReference) fragment).close();
                        }
                    }
                    fragments.clear();
                }
            }
        }
    }

    private void forwardFragments(TcpChannel channel, ArrayList<Object> fragments) throws IOException {
        for (Object fragment : fragments) {
            if (fragment instanceof Header) {
                assert aggregator.isAggregating() == false;
                aggregator.headerReceived((Header) fragment);
            } else if (fragment instanceof Compression.Scheme) {
                assert aggregator.isAggregating();
                aggregator.updateCompressionScheme((Compression.Scheme) fragment);
            } else if (fragment == InboundDecoder.PING) {
                assert aggregator.isAggregating() == false;
                messageHandler.accept(channel, PING_MESSAGE);
            } else if (fragment == InboundDecoder.END_CONTENT) {
                assert aggregator.isAggregating();
                try (InboundMessage aggregated = aggregator.finishAggregation()) {
                    statsTracker.markMessageReceived();
                    messageHandler.accept(channel, aggregated);
                }
            } else {
                assert aggregator.isAggregating();
                assert fragment instanceof ReleasableBytesReference;
                aggregator.aggregate((ReleasableBytesReference) fragment);
            }
        }
    }

    private static boolean endOfMessage(Object fragment) {
        return fragment == InboundDecoder.PING || fragment == InboundDecoder.END_CONTENT || fragment instanceof Exception;
    }

    private void releasePendingBytes(int bytesConsumed) {
        final int readable = pending.readableBytes();
        if (readable == bytesConsumed) {
            pending.release();
            pending = null;
        } else {
            pending = pending.skipBytes(bytesConsumed);
        }
    }

    private record ByteBufRefCounted(ByteBuf buffer) implements RefCounted {

        @Override
        public void incRef() {
            buffer.retain();
        }

        @Override
        public boolean tryIncRef() {
            if (hasReferences() == false) {
                return false;
            }
            try {
                buffer.retain();
            } catch (RuntimeException e) {
                assert hasReferences() == false;
                return false;
            }
            return true;
        }

        @Override
        public boolean decRef() {
            return buffer.release();
        }

        @Override
        public boolean hasReferences() {
            return buffer.refCnt() > 0;
        }
    }

}
