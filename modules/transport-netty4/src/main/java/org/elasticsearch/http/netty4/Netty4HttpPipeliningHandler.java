/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.concurrent.PromiseCombiner;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.http.HttpPipelinedMessage;
import org.elasticsearch.transport.netty4.NettyAllocator;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private static final String DO_NOT_SPLIT = "es.unsafe.do_not_split_http_responses";

    private static final boolean DO_NOT_SPLIT_HTTP_RESPONSES;
    private static final int SPLIT_THRESHOLD;

    static {
        DO_NOT_SPLIT_HTTP_RESPONSES = Booleans.parseBoolean(System.getProperty(DO_NOT_SPLIT), false);
        // Netty will add some header bytes if it compresses this message. So we downsize slightly.
        SPLIT_THRESHOLD = (int) (NettyAllocator.suggestedMaxAllocationSize() * 0.99);
    }

    private final Logger logger;

    private final Netty4HttpServerTransport serverTransport;
    private final int maxEventsHeld;
    private final PriorityQueue<Tuple<HttpPipelinedMessage, ChannelPromise>> outboundHoldingQueue = new PriorityQueue<>(
        1,
        Comparator.comparing(Tuple::v1)
    );

    private Tuple<Netty4ChunkedHttpResponse, ChannelPromise> currentChunkedWrite;

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
        assert msg instanceof HttpPipelinedMessage : "Invalid message type: " + msg.getClass();
        HttpPipelinedMessage response = (HttpPipelinedMessage) msg;
        boolean success = false;
        try {
            doWrite(ctx, response, promise);
            success = true;
        } catch (IllegalStateException e) {
            ctx.channel().close();
        } finally {
            if (success == false) {
                promise.setFailure(new ClosedChannelException());
            }
        }
    }

    private static void encodeAndWrite(ChannelHandlerContext ctx, Netty4HttpResponse msg, ChannelPromise listener) {
        if (DO_NOT_SPLIT_HTTP_RESPONSES || msg.content().readableBytes() <= SPLIT_THRESHOLD) {
            ctx.write(msg, listener);
        } else {
            HttpResponse response = new DefaultHttpResponse(msg.protocolVersion(), msg.status(), msg.headers());
            final PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
            combiner.add(ctx.write(response));
            ByteBuf content = msg.content();
            while (content.readableBytes() > SPLIT_THRESHOLD) {
                combiner.add(ctx.write(new DefaultHttpContent(content.readRetainedSlice(SPLIT_THRESHOLD))));
            }
            combiner.add(ctx.write(new DefaultLastHttpContent(content.readRetainedSlice(content.readableBytes()))));
            combiner.finish(listener);
        }
    }

    private void doWrite(ChannelHandlerContext ctx, HttpPipelinedMessage response, ChannelPromise listener) {
        if (outboundHoldingQueue.size() < maxEventsHeld) {
            outboundHoldingQueue.add(new Tuple<>(response, listener));
            if (currentChunkedWrite != null) {
                return;
            }
            while (outboundHoldingQueue.isEmpty() == false) {
                if (flushOutboundHoldingQueue(ctx)) {
                    return;
                }
            }
        } else {
            int eventCount = outboundHoldingQueue.size() + 1;
            throw new IllegalStateException("Too many pipelined events [" + eventCount + "]. Max events allowed [" + maxEventsHeld + "].");
        }
    }

    private boolean flushOutboundHoldingQueue(ChannelHandlerContext ctx) {
        /*
         * Since the response with the lowest sequence number is the top of the priority queue, we know if its sequence
         * number does not match the current write sequence number then we have not processed all preceding responses yet.
         */
        final Tuple<HttpPipelinedMessage, ChannelPromise> top = outboundHoldingQueue.peek();

        final HttpPipelinedMessage message = top.v1();
        if (message.getSequence() != writeSequence) {
            return true;
        }
        outboundHoldingQueue.poll();
        final ChannelPromise l = top.v2();
        writeSequence++;
        if (message instanceof Netty4HttpResponse) {
            encodeAndWrite(ctx, (Netty4HttpResponse) message, l);
            return false;
        } else {
            assert message instanceof Netty4ChunkedHttpResponse;
            final Netty4ChunkedHttpResponse msg = (Netty4ChunkedHttpResponse) message;
            currentChunkedWrite = Tuple.tuple(msg, l);
            ctx.write(new DefaultHttpResponse(msg.protocolVersion(), msg.status(), msg.headers())).addListener(future -> {
                assert ctx.executor().inEventLoop();
                if (future.isSuccess() == false) {
                    l.tryFailure(future.cause());
                }
            });
            return true;
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (ctx.channel().isWritable()) {
            doFlush(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        if (doFlush(ctx) == false) {
            ctx.flush();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        doFlush(ctx);
        super.channelInactive(ctx);
    }

    private boolean doFlush(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        final Channel channel = ctx.channel();
        if (channel.isActive() == false) {
            failQueuedWrites();
            return false;
        }
        boolean needsFlush = true;
        while (channel.isWritable()) {
            if (currentChunkedWrite == null) {
                break;
            }
            final ByteBuf writeBuffer = ctx.alloc().buffer();
            final boolean done;
            try {
                done = currentChunkedWrite.v1().serializeChunk(writeBuffer);
                writeBuffer.capacity(writeBuffer.readableBytes());
            } catch (IOException e) {
                failQueuedWrites();
                return true;
            }
            final ChannelFuture writeFuture;
            final ChannelPromise finalListener = currentChunkedWrite.v2();
            if (done) {
                writeFuture = ctx.write(new DefaultLastHttpContent(writeBuffer));
            } else {
                writeFuture = ctx.write(new DefaultHttpContent(writeBuffer));
            }
            needsFlush = true;
            if (done) {
                writeFuture.addListener(future -> {
                    assert ctx.executor().inEventLoop();
                    if (future.isSuccess()) {
                        finalListener.trySuccess();
                    } else {
                        finalListener.tryFailure(future.cause());
                    }
                });
                currentChunkedWrite = null;
                while (outboundHoldingQueue.isEmpty() == false) {
                    if (flushOutboundHoldingQueue(ctx)) {
                        break;
                    }
                }
            } else {
                writeFuture.addListener(future -> {
                    assert ctx.executor().inEventLoop();
                    if (future.isSuccess() == false) {
                        finalListener.tryFailure(future.cause());
                    }
                });
            }
            if (channel.isWritable() == false) {
                // try flushing to make channel writable again, loop will only continue if channel becomes writable again
                ctx.flush();
                needsFlush = false;
            }
        }
        if (needsFlush) {
            ctx.flush();
        }
        if (channel.isActive() == false) {
            failQueuedWrites();
        }
        return true;
    }

    private void failQueuedWrites() {
        final List<Tuple<HttpPipelinedMessage, ChannelPromise>> inFlight = removeAllInflightResponses();
        if (currentChunkedWrite == null && inFlight.isEmpty()) {
            return;
        }
        ClosedChannelException closedChannelException = new ClosedChannelException();
        if (currentChunkedWrite != null) {
            currentChunkedWrite.v2().setFailure(closedChannelException);
            currentChunkedWrite = null;
        }
        for (Tuple<HttpPipelinedMessage, ChannelPromise> inflightResponse : inFlight) {
            try {
                inflightResponse.v2().setFailure(closedChannelException);
            } catch (RuntimeException e) {
                logger.error("unexpected error while releasing pipelined http responses", e);
            }
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        failQueuedWrites();
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

    private List<Tuple<HttpPipelinedMessage, ChannelPromise>> removeAllInflightResponses() {
        ArrayList<Tuple<HttpPipelinedMessage, ChannelPromise>> responses = new ArrayList<>(outboundHoldingQueue);
        outboundHoldingQueue.clear();
        return responses;
    }
}
