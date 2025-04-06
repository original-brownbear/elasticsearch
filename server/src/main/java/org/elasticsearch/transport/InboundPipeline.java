/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

public class InboundPipeline implements Releasable {

    public static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final LongSupplier relativeTimeInMillis;
    private final StatsTracker statsTracker;
    private final InboundDecoder decoder;
    private final InboundAggregator aggregator;
    public final BiConsumer<TcpChannel, InboundMessage> messageHandler;
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
            reference.close();
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
            statsTracker.markBytesRead(reference.length());
            if (isClosed) {
                reference.close();
                return;
            }
            doHandleBytes(reference, channel);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    private void doHandleBytes(ReleasableBytesReference bytesReference, TcpChannel channel) throws IOException {
        CheckedConsumer<Object, IOException> decodeConsumer = f -> forwardFragment(channel, f);
        do {
            int bytesDecoded = decoder.decode(bytesReference, decodeConsumer);
            if (bytesDecoded != 0) {
                bytesReference = bytesReference.slice(bytesDecoded, bytesReference.length() - bytesDecoded);
            } else {
                bytesReference.close();
                break;
            }
        } while (true);
    }

    private void forwardFragment(TcpChannel channel, Object fragment) throws IOException {
        if (fragment instanceof Header) {
            headerReceived((Header) fragment);
        } else if (fragment instanceof Compression.Scheme) {
            assert aggregator.isAggregating();
            aggregator.updateCompressionScheme((Compression.Scheme) fragment);
        } else if (fragment == InboundDecoder.END_CONTENT) {
            assert aggregator.isAggregating();
            InboundMessage aggregated = aggregator.finishAggregation();
            try {
                statsTracker.markMessageReceived();
                messageHandler.accept(channel, aggregated);
            } finally {
                aggregated.decRef();
            }
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
