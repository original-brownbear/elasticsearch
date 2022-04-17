/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

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
import java.util.ArrayDeque;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class InboundPipeline implements Releasable {

    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final LongSupplier relativeTimeInMillis;
    private final StatsTracker statsTracker;
    private final InboundDecoder decoder;
    private final InboundAggregator aggregator;
    private final BiConsumer<TcpChannel, InboundMessage> messageHandler;
    private Exception uncaughtException;
    private final ArrayDeque<ByteBuf> pending = new ArrayDeque<>(2);
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
            ByteBuf buf;
            while ((buf = pending.poll()) != null) {
                buf.release();
            }
        });
    }

    public void handleBytes(TcpChannel channel, ByteBuf reference) throws IOException {
        if (uncaughtException != null) {
            reference.release();
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        boolean success = false;
        try {
            doHandleBytes(channel, reference);
            success = true;
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        } finally {
            if (success == false) {
                reference.release();
            }
        }
    }

    private void doHandleBytes(TcpChannel channel, ByteBuf reference) throws IOException {
        channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
        statsTracker.markBytesRead(reference.readableBytes());

        if (pending.isEmpty() == false) {
            // we already have pending bytes, so we queue these bytes after them and then try to decode from the combined message
            pending.add(reference);
            doHandleBytesWithPending(channel);
            return;
        }

        while (isClosed == false && reference.readableBytes() > 0) {
            final int bytesDecoded;
            bytesDecoded = decode(channel, asBytesReference(reference));
            if (bytesDecoded != 0) {
                if (reference.readableBytes() == bytesDecoded) {
                    reference.release();
                    return;
                } else {
                    reference = reference.skipBytes(bytesDecoded);
                }
            } else {
                break;
            }
        }
        // if handling the messages didn't cause the channel to get closed and we did not fully consume the buffer retain it
        if (isClosed == false && reference.readableBytes() > 0) {
            pending.add(reference);
        }
    }

    private static ReleasableBytesReference asBytesReference(ByteBuf buffer) {
        return new ReleasableBytesReference(Netty4Utils.toBytesReference(buffer), new ByteBufRefCounted(buffer));
    }

    private int decode(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        return decoder.decode(channel, reference, this::forwardFragment);
    }

    private void doHandleBytesWithPending(TcpChannel channel) throws IOException {
        do {
            final int bytesDecoded;
            if (pending.size() == 1) {
                bytesDecoded = decode(channel, asBytesReference(pending.peekFirst()));
            } else {
                try (ReleasableBytesReference toDecode = getPendingBytes()) {
                    bytesDecoded = decode(channel, toDecode);
                }
            }
            if (bytesDecoded != 0 && isClosed == false) {
                releasePendingBytes(bytesDecoded);
            } else {
                assert isClosed == false || pending.isEmpty() : "pending chunks should be empty if closed but saw [" + pending + "]";
                return;
            }
        } while (pending.isEmpty() == false);
    }

    private void forwardFragment(TcpChannel channel, Object fragment) throws IOException {
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

    private ReleasableBytesReference getPendingBytes() {
        assert pending.size() > 1 : "must use this method with multiple pending references but used with " + pending;
        final ByteBuf[] byteBufs = new ByteBuf[pending.size()];
        int index = 0;
        for (ByteBuf pendingReference : pending) {
            byteBufs[index] = pendingReference.retain();
            ++index;
        }
        final ByteBuf composite = new CompositeByteBuf(byteBufs[0].alloc(), false, byteBufs.length, byteBufs);
        return new ReleasableBytesReference(Netty4Utils.toBytesReference(composite), () -> {
            for (ByteBuf buf : byteBufs) {
                buf.release();
            }
        });
    }

    private void releasePendingBytes(int bytesConsumed) {
        int bytesToRelease = bytesConsumed;
        while (bytesToRelease != 0) {
            ByteBuf reference = pending.pollFirst();
            assert reference != null;
            if (bytesToRelease < reference.readableBytes()) {
                pending.addFirst(reference.skipBytes(bytesToRelease));
                return;
            } else {
                bytesToRelease -= reference.readableBytes();
                reference.release();
            }
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
