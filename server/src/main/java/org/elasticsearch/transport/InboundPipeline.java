/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

public class InboundPipeline implements Releasable {
    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final LongSupplier relativeTimeInMillis;
    private final StatsTracker statsTracker;
    private final InboundDecoder decoder;
    private final InboundAggregator aggregator;
    private final BiConsumer<TcpChannel, InboundMessage> messageHandler;
    private Exception uncaughtException;
    private boolean isClosed = false;

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
        Releasables.closeExpectNoException(decoder, aggregator);
    }

    public void handleBytes(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        if (uncaughtException != null) {
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            doHandleBytes(channel, reference);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    public void doHandleBytes(TcpChannel channel, final ReleasableBytesReference reference) throws IOException {
        channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
        statsTracker.markBytesRead(reference.length());
        ReleasableBytesReference currentSlice = reference;
        while (isClosed == false) {
            final int bytesDecoded = decoder.decode(currentSlice, fragment -> forwardFragment(channel, fragment));
            if (bytesDecoded == 0) {
                break;
            }
            currentSlice = currentSlice.unretainedSlice(bytesDecoded, currentSlice.length() - bytesDecoded);
        }
    }

    private void forwardFragment(TcpChannel channel, Object fragment) throws IOException {
        if (fragment instanceof Header) {
            headerReceived((Header) fragment);
        } else if (fragment instanceof Compression.Scheme) {
            assert aggregator.isAggregating();
            aggregator.updateCompressionScheme((Compression.Scheme) fragment);
        } else if (fragment == InboundDecoder.PING) {
            assert aggregator.isAggregating() == false;
            messageHandler.accept(channel, PING_MESSAGE);
        } else if (fragment == InboundDecoder.END_CONTENT) {
            assert aggregator.isAggregating();
            InboundMessage aggregated = aggregator.finishAggregation();
            statsTracker.markMessageReceived();
            messageHandler.accept(channel, aggregated);
        } else {
            assert aggregator.isAggregating();
            assert fragment instanceof ReleasableBytesReference;
            aggregator.aggregate((ReleasableBytesReference) fragment);
        }
    }

    protected void headerReceived(Header header) {
        assert aggregator.isAggregating() == false;
        aggregator.headerReceived(header);
    }

}
