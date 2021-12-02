/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpRequest;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Tuple;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;

    private final Netty4HttpServerTransport serverTransport;
    private final int maxEventsHeld;
    private final PriorityQueue<Tuple<Netty4HttpResponse, ChannelPromise>> outboundHoldingQueue = new PriorityQueue<>(
        1,
        Comparator.comparing(Tuple::v1)
    );

    /*
     * The current read and write sequence numbers. Read sequence numbers are attached to requests in the order they are read from the
     * channel, and then transferred to responses. A response is not written to the channel context until its sequence number matches the
     * current write sequence, implying that all preceding messages have been written.
     */
    private int readSequence;
    private int writeSequence;

    /**
     * Construct a new pipelining handler; this handler should be used downstream of HTTP decoding/aggregation.
     *
     * @param logger        for logging unexpected errors
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public Netty4HttpPipeliningHandler(Logger logger, final int maxEventsHeld, Netty4HttpServerTransport transport) {
        this.logger = logger;
        this.maxEventsHeld = maxEventsHeld;
        this.serverTransport = transport;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof FullHttpRequest;
        final FullHttpRequest httpRequest = (FullHttpRequest) msg;
        if (httpRequest.decoderResult().isFailure()) {
            final Throwable cause = httpRequest.decoderResult().cause();
            final Exception nonError;
            if (cause instanceof Error) {
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                nonError = new Exception(cause);
            } else {
                nonError = (Exception) cause;
            }
            handleRequest(ctx, new Netty4HttpRequest(httpRequest, nonError, readSequence++));
        } else {
            handleRequest(ctx, new Netty4HttpRequest(httpRequest, readSequence++));
        }
    }

    protected void handleRequest(ChannelHandlerContext ctx, Netty4HttpRequest httpRequest) {
        final Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        boolean success = false;
        try {
            serverTransport.incomingRequest(httpRequest, channel);
            success = true;
        } finally {
            if (success == false) {
                httpRequest.release();
            }
        }
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        assert msg instanceof Netty4HttpResponse : "Invalid message type: " + msg.getClass();
        Netty4HttpResponse response = (Netty4HttpResponse) msg;
        boolean success = false;
        try {
            List<Tuple<Netty4HttpResponse, ChannelPromise>> readyResponses = write(response, promise);
            for (Tuple<Netty4HttpResponse, ChannelPromise> readyResponse : readyResponses) {
                ctx.write(readyResponse.v1(), readyResponse.v2());
            }
            success = true;
        } catch (IllegalStateException e) {
            ctx.channel().close();
        } finally {
            if (success == false) {
                promise.setFailure(new ClosedChannelException());
            }
        }
    }

    private List<Tuple<Netty4HttpResponse, ChannelPromise>> write(Netty4HttpResponse response, ChannelPromise listener) {
        if (outboundHoldingQueue.size() < maxEventsHeld) {
            ArrayList<Tuple<Netty4HttpResponse, ChannelPromise>> readyResponses = new ArrayList<>();
            outboundHoldingQueue.add(new Tuple<>(response, listener));
            while (outboundHoldingQueue.isEmpty() == false) {
                /*
                 * Since the response with the lowest sequence number is the top of the priority queue, we know if its sequence
                 * number does not match the current write sequence number then we have not processed all preceding responses yet.
                 */
                final Tuple<Netty4HttpResponse, ChannelPromise> top = outboundHoldingQueue.peek();

                if (top.v1().getSequence() != writeSequence) {
                    break;
                }
                outboundHoldingQueue.poll();
                readyResponses.add(top);
                writeSequence++;
            }

            return readyResponses;
        } else {
            int eventCount = outboundHoldingQueue.size() + 1;
            throw new IllegalStateException("Too many pipelined events [" + eventCount + "]. Max events allowed [" + maxEventsHeld + "].");
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        List<Tuple<Netty4HttpResponse, ChannelPromise>> inflightResponses = removeAllInflightResponses();

        if (inflightResponses.isEmpty() == false) {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            for (Tuple<Netty4HttpResponse, ChannelPromise> inflightResponse : inflightResponses) {
                try {
                    inflightResponse.v2().setFailure(closedChannelException);
                } catch (RuntimeException e) {
                    logger.error("unexpected error while releasing pipelined http responses", e);
                }
            }
        }
        ctx.close(promise);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        if (cause instanceof Error) {
            serverTransport.onException(channel, new Exception(cause));
        } else {
            serverTransport.onException(channel, (Exception) cause);
        }
    }

    private List<Tuple<Netty4HttpResponse, ChannelPromise>> removeAllInflightResponses() {
        ArrayList<Tuple<Netty4HttpResponse, ChannelPromise>> responses = new ArrayList<>(outboundHoldingQueue);
        outboundHoldingQueue.clear();
        return responses;
    }
}
