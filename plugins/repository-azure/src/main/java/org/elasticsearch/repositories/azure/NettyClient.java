/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure;

import com.microsoft.rest.v2.http.ConcurrentMultiDequeMap;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpClientConfiguration;
import com.microsoft.rest.v2.http.HttpClientFactory;
import com.microsoft.rest.v2.http.HttpHeader;
import com.microsoft.rest.v2.http.HttpHeaders;
import com.microsoft.rest.v2.http.HttpRequest;
import com.microsoft.rest.v2.http.HttpResponse;
import com.microsoft.rest.v2.http.SharedChannelPoolOptions;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FailedFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.SucceededFuture;
import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.Single;
import io.reactivex.SingleEmitter;
import io.reactivex.annotations.Nullable;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;
import org.elasticsearch.transport.TcpTransport;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * An HttpClient that is implemented using Netty.
 */
public final class NettyClient extends HttpClient {
    private final NettyAdapter adapter;
    private final HttpClientConfiguration configuration;

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    /**
     * Creates NettyClient.
     * @param configuration the HTTP client configuration.
     * @param adapter the adapter to Netty
     */
    private NettyClient(HttpClientConfiguration configuration, NettyAdapter adapter) {
        this.adapter = adapter;
        this.configuration = configuration != null ? configuration : new HttpClientConfiguration(null);
    }

    @Override
    public Single<HttpResponse> sendRequestAsync(final HttpRequest request) {
        return adapter.sendRequestInternalAsync(request, configuration);
    }

    private static final class NettyAdapter {
        private static final String EPOLL_GROUP_CLASS_NAME = "io.netty.channel.epoll.EpollEventLoopGroup";
        private static final String EPOLL_SOCKET_CLASS_NAME = "io.netty.channel.epoll.EpollSocketChannel";

        private static final String KQUEUE_GROUP_CLASS_NAME = "io.netty.channel.kqueue.KQueueEventLoopGroup";
        private static final String KQUEUE_SOCKET_CLASS_NAME = "io.netty.channel.kqueue.KQueueSocketChannel";

        private final MultithreadEventLoopGroup eventLoopGroup;
        private final SharedChannelPool channelPool;

        public Future<?> shutdownGracefully() {
            channelPool.close();
            return eventLoopGroup.shutdownGracefully();
        }

        private static final class TransportConfig {
            final MultithreadEventLoopGroup eventLoopGroup;
            final Class<? extends SocketChannel> channelClass;

            private TransportConfig(MultithreadEventLoopGroup eventLoopGroup,
                Class<? extends SocketChannel> channelClass) {
                this.eventLoopGroup = eventLoopGroup;
                this.channelClass = channelClass;
            }
        }

        private static MultithreadEventLoopGroup loadEventLoopGroup(String className, int size) throws ReflectiveOperationException {
            Class<?> cls = Class.forName(className);
            ThreadFactory factory = new DefaultThreadFactory(cls, true);
            MultithreadEventLoopGroup result = (MultithreadEventLoopGroup) cls
                .getConstructor(Integer.TYPE, ThreadFactory.class).newInstance(size, factory);
            return result;
        }

        @SuppressWarnings("unchecked")
        private static NettyAdapter.TransportConfig loadTransport(int groupSize) {
            NettyAdapter.TransportConfig result = null;
            try {
                final String osName = System.getProperty("os.name");
                if (osName.contains("Linux")) {
                    result = new NettyAdapter.TransportConfig(loadEventLoopGroup(EPOLL_GROUP_CLASS_NAME, groupSize),
                        (Class<? extends SocketChannel>) Class.forName(EPOLL_SOCKET_CLASS_NAME));
                } else if (osName.contains("Mac")) {
                    result = new NettyAdapter.TransportConfig(loadEventLoopGroup(KQUEUE_GROUP_CLASS_NAME, groupSize),
                        (Class<? extends SocketChannel>) Class.forName(KQUEUE_SOCKET_CLASS_NAME));
                }
            } catch (Exception e) {
                String message = e.getMessage();
                if (message == null) {
                    Throwable cause = e.getCause();
                    if (cause != null) {
                        message = cause.getMessage();
                    }
                }
                LoggerFactory.getLogger(NettyAdapter.class)
                    .debug("Exception when obtaining native EventLoopGroup and SocketChannel: " + message);
            }

            if (result == null) {
                result = new NettyAdapter.TransportConfig(
                    new NioEventLoopGroup(groupSize, daemonThreadFactory(TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX)),
                    NioSocketChannel.class);
            }

            return result;
        }

        private static SharedChannelPool createChannelPool(Bootstrap bootstrap, NettyAdapter.TransportConfig config,
            SharedChannelPoolOptions options, SslContext sslContext) {
            bootstrap.group(config.eventLoopGroup);
            bootstrap.channel(config.channelClass);
            bootstrap.option(ChannelOption.AUTO_READ, false);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) TimeUnit.MINUTES.toMillis(3L));
            return new SharedChannelPool(bootstrap, config.eventLoopGroup, new AbstractChannelPoolHandler() {
                @Override
                public synchronized void channelCreated(Channel ch) throws URISyntaxException {
                    SocketAccess.doPrivilegedVoidException(() -> {
                        // Why is it necessary to have "synchronized" to prevent NRE in pipeline().get(Class<T>)?
                        // Is channelCreated not run on the eventLoop assigned to the channel?
                        ch.pipeline().addLast("HttpClientCodec", new HttpClientCodec());
                        ch.pipeline().addLast("HttpClientInboundHandler", new HttpClientInboundHandler());
                    });
                }
            }, options, sslContext);
        }

        private NettyAdapter() {
            NettyAdapter.TransportConfig config = loadTransport(0);
            this.eventLoopGroup = config.eventLoopGroup;
            this.channelPool = createChannelPool(new Bootstrap(), config,
                new SharedChannelPoolOptions()
                    .withPoolSize(eventLoopGroup.executorCount() * 16).withIdleChannelKeepAliveDurationInSec(60), null);
        }

        private Single<HttpResponse> sendRequestInternalAsync(final HttpRequest request, final HttpClientConfiguration configuration) {
            addHeaders(request);

            // Creates cold observable from an emitter
            return Single.create((SingleEmitter<HttpResponse> responseEmitter) -> {
                AcquisitionListener listener = new AcquisitionListener(channelPool, request, responseEmitter);
                responseEmitter.setDisposable(listener);
                channelPool.acquire(request.url().toURI(), configuration.proxy()).addListener(listener);
            });
        }
    }

    private static void addHeaders(final HttpRequest request) {
        request.withHeader(HttpHeaderNames.HOST.toString(), request.url().getHost())
            .withHeader(HttpHeaderNames.CONNECTION.toString(),
                HttpHeaderValues.KEEP_ALIVE.toString());
    }

    private static final class AcquisitionListener
        implements GenericFutureListener<Future<? super Channel>>, Disposable {

        private final SharedChannelPool channelPool;
        private final HttpRequest request;
        private final SingleEmitter<HttpResponse> responseEmitter;

        // state is tracked to ensure that any races between write, read,
        // disposal, cancel, and request are properly handled via a serialized state machine.
        private final AtomicInteger state = new AtomicInteger(ACQUIRING_NOT_DISPOSED);

        private static final int MAX_SEND_BUF_SIZE = 1024 * 64;

        private static final int ACQUIRING_NOT_DISPOSED = 0;
        private static final int ACQUIRING_DISPOSED = 1;
        private static final int ACQUIRED_CONTENT_NOT_SUBSCRIBED = 2;
        private static final int ACQUIRED_CONTENT_SUBSCRIBED = 3;
        private static final int ACQUIRED_DISPOSED_CONTENT_SUBSCRIBED = 4;
        private static final int ACQUIRED_DISPOSED_CONTENT_NOT_SUBSCRIBED = 5;
        private static final int CHANNEL_RELEASED = 6;

        // synchronized by `state`
        private Channel channel;

        //synchronized by `state`
        private ResponseContentFlowable content;

        private volatile boolean finishedWritingRequestBody;
        private volatile AcquisitionListener.RequestSubscriber requestSubscriber;

        AcquisitionListener(
            SharedChannelPool channelPool,
            HttpRequest request,
            SingleEmitter<HttpResponse> responseEmitter) {
            this.channelPool = channelPool;
            this.request = request;
            this.responseEmitter = responseEmitter;
        }

        @Override
        public void operationComplete(Future<? super Channel> cf) {
            if (!cf.isSuccess()) {
                emitError(cf.cause());
                return;
            }
            channel = (Channel) cf.getNow();
            while (true) {
                int s = state.get();
                if (s == ACQUIRING_DISPOSED) {
                    if (transition(ACQUIRING_DISPOSED, CHANNEL_RELEASED)) {
                        LOGGER.debug("Channel disposed on acquisition");
                        channelPool.closeAndRelease(channel);
                        return;
                    }
                } else if (s == ACQUIRING_NOT_DISPOSED) {
                    if (transition(ACQUIRING_NOT_DISPOSED, ACQUIRED_CONTENT_NOT_SUBSCRIBED)) {
                        break;
                    }
                } else {
                    return;
                }
            }

            final HttpClientInboundHandler inboundHandler =
                channel.pipeline().get(HttpClientInboundHandler.class);
            inboundHandler.setFields(responseEmitter, this);
            //TODO do we need a memory barrier here to ensure vis of responseEmitter in other threads?

            try {

                final DefaultHttpRequest raw = createDefaultHttpRequest(request);

                writeRequest(raw);

                if (request.body() == null) {
                    writeBodyEnd();
                } else {
                    requestSubscriber = new AcquisitionListener.RequestSubscriber(inboundHandler);
                    String contentLengthHeader = request.headers().value("content-length");
                    try {
                        long contentLength = Long.parseLong(contentLengthHeader);
                        request.body()
                            .flatMap(
                                bb -> bb.remaining() > MAX_SEND_BUF_SIZE ? FlowableUtil.split(bb, MAX_SEND_BUF_SIZE) : Flowable.just(bb))
                            .compose(FlowableUtil.ensureLength(contentLength))
                            .subscribe(requestSubscriber);
                    } catch (NumberFormatException e) {
                        String message = "Content-Length was expected to be a valid long but was \"" + contentLengthHeader + '"';
                        throw new IllegalArgumentException(message, e);
                    }
                }
            } catch (Exception e) {
                emitError(e);
            }
        }

        private final class RequestSubscriber implements FlowableSubscriber<ByteBuffer>, GenericFutureListener<Future<Void>> {
            Subscription subscription;

            // we need a done flag because an onNext emission can throw and emit an Error
            // event though the onNext cancels the subscription that is best endeavours for the
            // upstream so we need to be defensive about terminal events that follow
            private boolean done;

            private final HttpClientInboundHandler inboundHandler;

            RequestSubscriber(HttpClientInboundHandler inboundHandler) {
                this.inboundHandler = inboundHandler;
            }

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                inboundHandler.requestContentSubscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(ByteBuffer buf) {
                if (done) {
                    return;
                }

                try {
                    // Always dispatching writes on the event loop prevents data corruption on macOS.
                    // Since channel.write always dispatches to the event loop itself if needed internally,
                    // it seems fine to do it here.
                    channel.eventLoop().execute(() -> {
                        try {
                            channel.write(Unpooled.wrappedBuffer(buf)).addListener(this);
                            if (channel.isWritable()) {
                                subscription.request(1);
                            } else {
                                channel.flush();
                            }
                        } catch (Exception e) {
                            subscription.cancel();
                            onError(e);
                        }
                    });
                } catch (Exception e) {
                    subscription.cancel();
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                // TODO should we wrap the throwable so that the client
                // knows that the error occurred from the request body?
                if (done) {
                    RxJavaPlugins.onError(t);
                    return;
                }
                done = true;
                emitError(t);
            }

            @Override
            public void onComplete() {
                if (done) {
                    return;
                }
                done = true;
                try {
                    writeBodyEnd();
                } catch (Exception e) {
                    emitError(e);
                }
            }

            @Override
            public void operationComplete(Future<Void> future) {
                if (!future.isSuccess()) {
                    subscription.cancel();
                    done = true;
                    emitError(future.cause());
                }
            }

            void channelWritable(boolean writable) {
                if (writable) {
                    subscription.request(1);
                }
            }

        }

        private void writeRequest(final DefaultHttpRequest raw) {
            channel.eventLoop().execute(() ->
                channel //
                    .write(raw) //
                    .addListener((Future<Void> future) -> {
                        if (!future.isSuccess()) {
                            emitError(future.cause());
                        }
                    })
            );
        }

        private void writeBodyEnd() {
            channel.eventLoop().execute(() -> channel //
                .writeAndFlush(DefaultLastHttpContent.EMPTY_LAST_CONTENT) //
                .addListener((Future<Void> future) -> {
                    if (future.isSuccess()) {
                        finishedWritingRequestBody = true;
                        // reads the response status code and headers and may also read some of the
                        // response body which will be buffered in ResponseContentFlowable
                        channel.read();
                    } else {
                        emitError(future.cause());
                    }
                }));
        }

        private boolean transition(int from, int to) {
            return state.compareAndSet(from, to);
        }

        void emitError(Throwable throwable) {
            while (true) {
                if (throwable == null) {
                    throwable = new IOException("Unknown error in Netty client");
                }
                if (channel != null) {
                    LOGGER.warn("Error emitted on channel {}. Message: {}", channel.id(), throwable.getMessage());
                } else {
                    LOGGER.warn("Error emitted before channel is created. Message: {}", throwable.getMessage());
                }
                LOGGER.debug("Stack trace: ", new Exception());
                channelPool.dump();
                int s = state.get();
                if (s == ACQUIRING_NOT_DISPOSED) {
                    if (transition(ACQUIRING_NOT_DISPOSED, ACQUIRING_DISPOSED)) {
                        LOGGER.debug("Channel disposed before response is subscribed");
                        responseEmitter.onError(throwable);
                        break;
                    }
                } else if (s == ACQUIRED_CONTENT_SUBSCRIBED) {
                    if (transition(ACQUIRED_CONTENT_SUBSCRIBED, CHANNEL_RELEASED)) {
                        LOGGER.debug("Channel disposed after content is subscribed");
                        content.onError(throwable);
                        closeAndReleaseChannel();
                        break;
                    }
                } else if (s == ACQUIRED_CONTENT_NOT_SUBSCRIBED) {
                    if (transition(ACQUIRED_CONTENT_NOT_SUBSCRIBED, CHANNEL_RELEASED)) {
                        LOGGER.debug("Channel disposed before content is subscribed");
                        responseEmitter.onError(throwable);
                        closeAndReleaseChannel();
                        break;
                    }
                } else if (s == ACQUIRED_DISPOSED_CONTENT_SUBSCRIBED) {
                    if (transition(ACQUIRED_DISPOSED_CONTENT_SUBSCRIBED, CHANNEL_RELEASED)) {
                        LOGGER.debug("Channel disposed after content is subscribed with response emitter disposed");
                        content.onError(throwable);
                        closeAndReleaseChannel();
                        break;
                    }
                } else if (s == ACQUIRED_DISPOSED_CONTENT_NOT_SUBSCRIBED) {
                    if (transition(ACQUIRED_DISPOSED_CONTENT_NOT_SUBSCRIBED, CHANNEL_RELEASED)) {
                        LOGGER.debug("Channel disposed before content is subscribed with response emitter disposed");
                        closeAndReleaseChannel();
                        throw Exceptions.propagate(throwable);
                    }
                } else {
                    LOGGER.debug("Channel disposed at state {}", s);
                    closeAndReleaseChannel();
                    break;
                }
            }
        }

        /**
         * Returns false if and only if content subscription should be immediately
         * cancelled.
         * @param content the content that was subscribed to
         * @return false if and only if content subscription should be immediately
         * cancelled
         */
        boolean contentSubscribed(ResponseContentFlowable content) {
            while (true) {
                int s = state.get();
                if (s == ACQUIRED_CONTENT_NOT_SUBSCRIBED) {
                    if (transition(ACQUIRED_CONTENT_NOT_SUBSCRIBED, ACQUIRED_CONTENT_SUBSCRIBED)) {
                        this.content = content;
                        return true;
                    }
                } else if (s == ACQUIRED_DISPOSED_CONTENT_NOT_SUBSCRIBED) {
                    if (transition(ACQUIRED_DISPOSED_CONTENT_NOT_SUBSCRIBED, ACQUIRED_DISPOSED_CONTENT_SUBSCRIBED)) {
                        this.content = content;
                        return true;
                    }
                } else {
                    return false;
                }
            }
        }

        private void releaseChannel(boolean cancelled) {
            if (!cancelled && finishedWritingRequestBody) {
                Future<?> release = channelPool.release(channel);
                if (!release.isSuccess()) {
                    emitError(release.cause());
                }
            } else {
                LOGGER.debug("Channel disposed on cancellation or request body reading interrupted");
                closeAndReleaseChannel();
            }
        }

        /**
         * Is called when content flowable terminates or is cancelled.
         **/
        void contentDone(boolean cancelled) {
            while (true) {
                int s = state.get();
                if (s == ACQUIRED_CONTENT_SUBSCRIBED) {
                    if (transition(ACQUIRED_CONTENT_SUBSCRIBED, CHANNEL_RELEASED)) {
                        releaseChannel(cancelled);
                        return;
                    }
                } else if (s == ACQUIRED_DISPOSED_CONTENT_SUBSCRIBED) {
                    if (transition(ACQUIRED_DISPOSED_CONTENT_SUBSCRIBED, CHANNEL_RELEASED)) {
                        releaseChannel(cancelled);
                        return;
                    }
                } else {
                    return;
                }
            }
        }

        @Override
        public void dispose() {
            while (true) {
                int s = state.get();
                if (s == ACQUIRING_NOT_DISPOSED) {
                    if (transition(ACQUIRING_NOT_DISPOSED, ACQUIRING_DISPOSED)) {
                        // haven't got the channel to be able to release it yet
                        // but check in operationComplete will release it
                        return;
                    }
                } else if (s == ACQUIRING_DISPOSED) {
                    if (transition(ACQUIRING_DISPOSED, CHANNEL_RELEASED)) {
                        // error emitted during channel acquisition, channel may
                        // or may not be available
                        LOGGER.debug("Channel disposed on ACQUIRING_DISPOSED");
                        closeAndReleaseChannel();
                        return;
                    }
                } else if (s == ACQUIRED_CONTENT_NOT_SUBSCRIBED) {
                    if (transition(ACQUIRED_CONTENT_NOT_SUBSCRIBED, ACQUIRED_DISPOSED_CONTENT_NOT_SUBSCRIBED)) {
                        // response emitted but before content is subscribed.
                        // likely a Flowable<ByteBuffer> response that's not yet
                        // been subscribed
                        return;
                    }
                } else if (s == ACQUIRED_CONTENT_SUBSCRIBED) {
                    if (transition(ACQUIRED_CONTENT_SUBSCRIBED, ACQUIRED_DISPOSED_CONTENT_SUBSCRIBED)) {
                        // response emitted and content is already subscribed
                        return;
                    }
                } else {
                    return;
                }
            }
        }

        private void closeAndReleaseChannel() {
            if (channel != null) {
                channelPool.closeAndRelease(channel);
            }
        }

        @Override
        public boolean isDisposed() {
            return state.get() >= 4;
        }

        public void channelWritable(boolean writable) {
            if (requestSubscriber != null) {
                requestSubscriber.channelWritable(writable);
            }
        }

    }

    private static DefaultHttpRequest createDefaultHttpRequest(HttpRequest request) {
        final DefaultHttpRequest raw = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
            HttpMethod.valueOf(request.httpMethod().toString()), request.url().toString());

        for (HttpHeader header : request.headers()) {
            raw.headers().add(header.name(), header.value());
        }
        return raw;
    }

    /**
     * Emits HTTP response content from Netty.
     */
    private static final class ResponseContentFlowable extends Flowable<ByteBuf> implements Subscription {

        // single producer, single consumer queue
        private final SimplePlainQueue<HttpContent> queue = new SpscLinkedArrayQueue<>(16);
        private final Subscription channelSubscription;
        private final AtomicBoolean chunkRequested = new AtomicBoolean(true);
        private final AtomicLong requested = new AtomicLong();

        // work-in-progress counter
        private final AtomicInteger wip = new AtomicInteger(1); // set to 1 to disable drain till we are ready

        // ensures one subscriber only
        private final AtomicBoolean once = new AtomicBoolean();

        private Subscriber<? super ByteBuf> subscriber;

        // can be non-volatile because event methods onReceivedContent,
        // chunkComplete, onError are serialized and is only written and
        // read in those event methods
        private boolean done;

        private volatile boolean cancelled;

        // must be volatile otherwise parts of Throwable might not be visible to drain
        // loop (or suffer from word tearing)
        private volatile Throwable err;
        private final AcquisitionListener acquisitionListener;

        ResponseContentFlowable(AcquisitionListener acquisitionListener, Subscription channelSubscription) {
            this.acquisitionListener = acquisitionListener;
            this.channelSubscription = channelSubscription;
        }

        @Override
        protected void subscribeActual(Subscriber<? super ByteBuf> s) {
            if (once.compareAndSet(false, true)) {
                subscriber = s;
                s.onSubscribe(this);

                acquisitionListener.contentSubscribed(this);

                // now that subscriber has been set enable the drain loop
                wip.lazySet(0);

                // we call drain because requests could have happened asynchronously before
                // wip was set to 0 (which enables the drain loop)
                drain();
            } else {
                s.onSubscribe(SubscriptionHelper.CANCELLED);
                s.onError(new IllegalStateException("Multiple subscriptions not allowed for response content"));
            }
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            channelSubscription.cancel();
            drain();
        }

        //
        // EVENTS - serialized
        //

        void onReceivedContent(HttpContent data) {
            if (done) {
                RxJavaPlugins.onError(new IllegalStateException("data arrived after LastHttpContent"));
                return;
            }
            if (data instanceof LastHttpContent) {
                done = true;
            }
            if (cancelled) {
                data.release();
            } else {
                queue.offer(data);
                drain();
            }
        }

        void chunkCompleted() {
            if (done) {
                return;
            }
            chunkRequested.set(false);
            drain();
        }

        void onError(Throwable cause) {
            if (done) {
                RxJavaPlugins.onError(cause);
            }
            done = true;
            err = cause;
            drain();
        }

        void channelInactive() {
            if (!done) {
                done = true;
                err = new IOException("channel inactive");
                drain();
            }
        }

        //
        // PRIVATE METHODS
        //

        private void requestChunkOfByteBufsFromUpstream() {
            channelSubscription.request(1);
        }

        private void drain() {
            // Below is a non-blocking technique to ensure serialization (in-order
            // processing) of the block inside the if statement and also to ensure
            // no race conditions exist where items on the queue would be missed.
            //
            // wip = `work in progress` and follows a naming convention in RxJava
            //
            // `missed` is a clever little trick to ensure that we only do as many
            // loops as actually required. If `drain` is called say 10 times while
            // the `drain` loop is active then we notice that there are possibly
            // extra items on the queue that arrived just after we found none left
            // (and before the method exits). We don't need to loop around ten times
            // but only once because all items will be picked up from the queue in
            // one additional polling loop.
            if (wip.getAndIncrement() == 0) {
                // need to check cancelled even if there are no requests
                if (cancelled) {
                    releaseQueue();
                    acquisitionListener.contentDone(true);
                    return;
                }
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        // Note that an error can shortcut the emission of content that is currently on
                        // the queue. This is probably desirable generally because it prevents work that being done
                        // downstream that might be thrown away anyway due to the error.
                        Throwable error = err;
                        if (error != null) {
                            releaseQueue();
                            channelSubscription.cancel();
                            subscriber.onError(error);
                            acquisitionListener.contentDone(true);
                            return;
                        }
                        HttpContent o = queue.poll();
                        if (o != null) {
                            e++;
                            if (emitContent(o)) {
                                return;
                            }
                        } else {
                            // queue is empty so lets see if we need to request another chunk
                            // note that we can only request one chunk at a time because the
                            // method channel.read() ignores calls if a read is pending
                            if (chunkRequested.compareAndSet(false, true)) {
                                requestChunkOfByteBufsFromUpstream();
                            }
                            break;
                        }
                        if (cancelled) {
                            releaseQueue();
                            acquisitionListener.contentDone(true);
                            return;
                        }
                    }
                    if (e > 0) {
                        // it's tempting to use the result of this method to avoid
                        // another volatile read of requested but to avoid race conditions
                        // it's essential that requested is read again AFTER wip is changed.
                        BackpressureHelper.produced(requested, e);
                    }
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        // should only be called from the drain loop
        // returns true if complete
        private boolean emitContent(HttpContent data) {
            subscriber.onNext(data.content());
            if (data instanceof LastHttpContent) {
                // release queue defensively (event serialization and the done flag
                // should mean there are no more items on the queue)
                releaseQueue();
                subscriber.onComplete();
                acquisitionListener.contentDone(false);
                return true;
            } else {
                return false;
            }
        }

        // Should only be called from the drain loop. We want to poll
        // the whole queue and release the contents one by one so we
        // need to honor the single consumer aspect of the Spsc queue
        // to ensure proper visibility of the queued items.
        private void releaseQueue() {
            HttpContent c;
            while ((c = queue.poll()) != null) {
                c.release();
            }
        }
    }

    private static final class ChannelSubscription implements Subscription {

        private final AtomicReference<Channel> channel;
        private final AcquisitionListener acquisitionListener;

        ChannelSubscription(AtomicReference<Channel> channel, AcquisitionListener acquisitionListener) {
            this.channel = channel;
            this.acquisitionListener = acquisitionListener;
        }

        @Override
        public void request(long n) {
            assert n == 1 : "requests must be one at a time!";
            Channel c = channel.get();
            if (c != null) {
                c.read();
            }
        }

        @Override
        public void cancel() {
            acquisitionListener.contentDone(true);
        }
    }

    private static final class HttpClientInboundHandler extends ChannelInboundHandlerAdapter {
        private SingleEmitter<HttpResponse> responseEmitter;

        // TODO does this need to be volatile
        private volatile ResponseContentFlowable contentEmitter;
        private AcquisitionListener acquisitionListener;
        //TODO may not need to be volatile, depends on eventLoop involvement
        private volatile Subscription requestContentSubscription;

        private final AtomicReference<Channel> channel = new AtomicReference<>();

        HttpClientInboundHandler() {
        }

        void setFields(SingleEmitter<HttpResponse> responseEmitter, AcquisitionListener acquisitionListener) {
            // this will be called before request has been initiated
            this.responseEmitter = responseEmitter;
            this.acquisitionListener = acquisitionListener;
            this.contentEmitter = new ResponseContentFlowable(
                acquisitionListener, new ChannelSubscription(channel, acquisitionListener));
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            // TODO can ctx.channel() return a different object at some point in the lifecycle?
            channel.set(ctx.channel());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            acquisitionListener.emitError(cause);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws URISyntaxException {
            SocketAccess.doPrivilegedVoidException(() -> {
                if (contentEmitter != null) {
                    // It doesn't seem like this should be possible since we set this volatile field
                    // before we begin writing request content, but it can happen under high load
                    contentEmitter.chunkCompleted();
                }
            });
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            acquisitionListener.channelWritable(ctx.channel().isWritable());
            super.channelWritabilityChanged(ctx);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof io.netty.handler.codec.http.HttpResponse) {
                io.netty.handler.codec.http.HttpResponse response = (io.netty.handler.codec.http.HttpResponse) msg;

                if (response.decoderResult().isFailure()) {
                    exceptionCaught(ctx, response.decoderResult().cause());
                    return;
                }

                responseEmitter.onSuccess(new NettyResponse(response, contentEmitter));
            } else if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;

                contentEmitter.onReceivedContent(content);
                if (msg instanceof LastHttpContent) {
                    acquisitionListener.contentDone(false);
                }
            } else {
                exceptionCaught(ctx, new IllegalStateException("Unexpected message type: " + msg.getClass().getName()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (contentEmitter != null) {
                contentEmitter.channelInactive();
            }
            super.channelInactive(ctx);
        }

    }

    /**
     * The factory for creating a NettyClient.
     */
    public static class Factory implements HttpClientFactory {
        private final NettyAdapter adapter;

        /**
         * Create a Netty client factory with default settings.
         */
        public Factory() {
            this.adapter = new NettyAdapter();
        }

        @Override
        public HttpClient create(final HttpClientConfiguration configuration) {
            return new NettyClient(configuration, adapter);
        }

        @Override
        public void close() {
            adapter.shutdownGracefully().awaitUninterruptibly();
        }
    }

    /**
     * A Netty channel pool implementation shared between multiple requests.
     *
     * Requests with the same host, port, and scheme share the same internal
     * pool. All the internal pools for all the requests have a fixed size limit.
     * This channel pool should be shared between multiple Netty adapters.
     */
    private static final class SharedChannelPool implements ChannelPool {
        private static final AttributeKey<URI> CHANNEL_URI = AttributeKey.newInstance("channel-uri");
        private static final AttributeKey<ZonedDateTime> CHANNEL_AVAILABLE_SINCE = AttributeKey.newInstance("channel-available-since");
        private static final AttributeKey<ZonedDateTime> CHANNEL_LEASED_SINCE = AttributeKey.newInstance("channel-leased-since");
        private static final AttributeKey<ZonedDateTime> CHANNEL_CREATED_SINCE = AttributeKey.newInstance("channel-created-since");
        private static final AttributeKey<ZonedDateTime> CHANNEL_CLOSED_SINCE = AttributeKey.newInstance("channel-closed-since");
        private final Bootstrap bootstrap;
        private final EventLoopGroup eventLoopGroup;
        private final ChannelPoolHandler handler;
        private final int poolSize;
        private final AtomicInteger channelCount = new AtomicInteger(0);
        private final SharedChannelPoolOptions poolOptions;
        private final ConcurrentMultiDequeMap<URI, SharedChannelPool.ChannelRequest> requests;
        private final ConcurrentMultiDequeMap<URI, Channel> available;
        private final ConcurrentMultiDequeMap<URI, Channel> leased;
        private final SslContext sslContext;
        private volatile boolean closed;
        private final Logger logger = LoggerFactory.getLogger(SharedChannelPool.class);
        final AtomicInteger wip = new AtomicInteger(0);

        private boolean isChannelHealthy(Channel channel) {
            try {
                if (!channel.isActive()) {
                    return false;
                } else if (channel.pipeline().get("HttpResponseDecoder") == null && channel.pipeline().get("HttpClientCodec") == null) {
                    return false;
                } else {
                    ZonedDateTime channelAvailableSince = channel.attr(CHANNEL_AVAILABLE_SINCE).get();
                    if (channelAvailableSince == null) {
                        channelAvailableSince = channel.attr(CHANNEL_LEASED_SINCE).get();
                    }
                    final long channelIdleDurationInSec =
                        ChronoUnit.SECONDS.between(channelAvailableSince, ZonedDateTime.now(ZoneOffset.UTC));
                    return channelIdleDurationInSec < this.poolOptions.idleChannelKeepAliveDurationInSec();
                }
            } catch (Throwable t) {
                return false;
            }
        }

        /**
         * Creates an instance of the shared channel pool.
         * @param bootstrap the bootstrap to create channels
         * @param handler the handler to apply to the channels on creation, acquisition and release
         * @param options optional settings for the pool
         * @param sslContext the SSL Context for the connections
         */
        SharedChannelPool(Bootstrap bootstrap, EventLoopGroup eventLoopGroup, ChannelPoolHandler handler, SharedChannelPoolOptions options,
                          SslContext sslContext) {
            this.poolOptions = options.clone();
            this.bootstrap = bootstrap.clone();
            this.eventLoopGroup = eventLoopGroup;
            this.handler = handler;
            this.poolSize = options.poolSize();
            this.requests = new ConcurrentMultiDequeMap<>();
            this.available = new ConcurrentMultiDequeMap<>();
            this.leased = new ConcurrentMultiDequeMap<>();
            try {
                if (sslContext == null) {
                    this.sslContext = SslContextBuilder.forClient().build();
                } else {
                    this.sslContext = sslContext;
                }
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }
        }

        private void drain(URI preferredUri) {
            if (!wip.compareAndSet(0, 1)) {
                return;
            }
            while (!closed && wip.updateAndGet(x -> requests.size()) != 0) {
                if (channelCount.get() >= poolSize && available.size() == 0) {
                    wip.set(0);
                    break;
                }
                // requests must be non-empty based on the above condition
                SharedChannelPool.ChannelRequest request;
                if (preferredUri != null && requests.containsKey(preferredUri)) {
                    request = requests.poll(preferredUri);
                } else {
                    request = requests.poll();
                }

                boolean foundHealthyChannelInPool = false;
                // Try to retrieve a healthy channel from pool
                if (available.containsKey(request.channelURI)) {
                    Channel channel = available.pop(request.channelURI); // try most recently used
                    if (isChannelHealthy(channel)) {
                        logger.debug("Channel picked up from pool: {}", channel.id());
                        leased.put(request.channelURI, channel);
                        foundHealthyChannelInPool = true;
                        channel.attr(CHANNEL_LEASED_SINCE).set(ZonedDateTime.now(ZoneOffset.UTC));
                        request.promise.setSuccess(channel);
                        try {
                            handler.channelAcquired(channel);
                        } catch (Exception e) {
                            throw Exceptions.propagate(e);
                        }
                    } else {
                        logger.debug("Channel disposed from pool due to timeout or half closure: {}", channel.id());
                        closeChannel(channel);
                        channelCount.decrementAndGet();
                        // Delete all channels created before this
                        while (available.containsKey(request.channelURI)) {
                            Channel broken = available.pop(request.channelURI);
                            logger.debug("Channel disposed from pool due to timeout or half closure: {}", broken.id());
                            closeChannel(broken);
                            channelCount.decrementAndGet();
                        }
                    }
                }
                if (!foundHealthyChannelInPool) {
                    // Not found a healthy channel in pool. Create a new channel - remove an available one if size overflows
                    if (channelCount.get() >= poolSize) {
                        Channel nextAvailable = available.poll(); // Dispose least recently used
                        logger.debug("Channel disposed due to overflow: {}", nextAvailable.id());
                        closeChannel(nextAvailable);
                        channelCount.decrementAndGet();
                    }
                    int port;
                    if (request.destinationURI.getPort() < 0) {
                        port = "https".equals(request.destinationURI.getScheme()) ? 443 : 80;
                    } else {
                        port = request.destinationURI.getPort();
                    }
                    channelCount.incrementAndGet();
                    this.bootstrap.clone().handler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            assert ch.eventLoop().inEventLoop();
                            if (request.proxy != null) {
                                ch.pipeline().addFirst("HttpProxyHandler", new HttpProxyHandler(request.proxy.address()));
                            }
                            handler.channelCreated(ch);
                        }
                    }).connect(request.destinationURI.getHost(), port).addListener((ChannelFuture f) -> {
                        if (f.isSuccess()) {
                            Channel channel = f.channel();
                            channel.attr(CHANNEL_URI).set(request.channelURI);

                            // Apply SSL handler for https connections
                            if ("https".equalsIgnoreCase(request.destinationURI.getScheme())) {
                                channel.pipeline().addBefore("HttpClientCodec", "SslHandler",
                                    this.sslContext.newHandler(channel.alloc(), request.destinationURI.getHost(), port));
                            }
                            leased.put(request.channelURI, channel);
                            channel.attr(CHANNEL_CREATED_SINCE).set(ZonedDateTime.now(ZoneOffset.UTC));
                            channel.attr(CHANNEL_LEASED_SINCE).set(ZonedDateTime.now(ZoneOffset.UTC));
                            logger.debug("Channel created: {}", channel.id());
                            handler.channelAcquired(channel);
                            request.promise.setSuccess(channel);
                        } else {
                            request.promise.setFailure(f.cause());
                            channelCount.decrementAndGet();
                        }
                    });
                }
            }
        }

        /**
         * Acquire a channel for a URI.
         * @param uri the URI the channel acquired should be connected to
         * @return the future to a connected channel
         */
        public Future<Channel> acquire(URI uri, @Nullable Proxy proxy) {
            return this.acquire(uri, proxy, this.bootstrap.config().group().next().newPromise());
        }

        /**
         * Acquire a channel for a URI.
         * @param uri the URI the channel acquired should be connected to
         * @param promise the writable future to a connected channel
         * @return the future to a connected channel
         */
        public Future<Channel> acquire(URI uri, @Nullable Proxy proxy, final Promise<Channel> promise) {
            if (closed) {
                throw new RejectedExecutionException("SharedChannelPool is closed");
            }

            SharedChannelPool.ChannelRequest channelRequest = new SharedChannelPool.ChannelRequest();
            channelRequest.promise = promise;
            channelRequest.proxy = proxy;
            int port;
            if (uri.getPort() < 0) {
                port = "https".equals(uri.getScheme()) ? 443 : 80;
            } else {
                port = uri.getPort();
            }
            try {
                channelRequest.destinationURI = new URI(String.format(Locale.US, "%s://%s:%d", uri.getScheme(), uri.getHost(), port));

                if (proxy == null) {
                    channelRequest.channelURI = channelRequest.destinationURI;
                } else {
                    InetSocketAddress address = (InetSocketAddress) proxy.address();
                    channelRequest.channelURI =
                        new URI(String.format(Locale.US, "%s://%s:%d", uri.getScheme(), address.getHostString(), address.getPort()));
                }

                requests.put(channelRequest.channelURI, channelRequest);
                drain(null);
            } catch (URISyntaxException e) {
                promise.setFailure(e);
            }
            return channelRequest.promise;
        }

        @Override
        public Future<Channel> acquire() {
            throw new UnsupportedOperationException("Please pass host & port to shared channel pool.");
        }

        @Override
        public Future<Channel> acquire(Promise<Channel> promise) {
            throw new UnsupportedOperationException("Please pass host & port to shared channel pool.");
        }

        private Future<Void> closeChannel(final Channel channel) {
            if (!channel.isOpen()) {
                return new SucceededFuture<>(eventLoopGroup.next(), null);
            }
            channel.attr(CHANNEL_CLOSED_SINCE).set(ZonedDateTime.now(ZoneOffset.UTC));
            logger.debug("Channel initiated to close: " + channel.id());
            // Closing a channel doesn't change the channel count
            try {
                return channel.close().addListener(f -> {
                    if (!f.isSuccess()) {
                        logger.warn("Possible channel leak: failed to close " + channel.id(), f.cause());
                    }
                });
            } catch (Exception e) {
                logger.warn("Possible channel leak: failed to close " + channel.id(), e);
                return new FailedFuture<>(eventLoopGroup.next(), e);
            }
        }

        /**
         * Closes the channel and releases it back to the pool.
         * @param channel the channel to close and release.
         * @return a Future representing the operation.
         */
        public Future<Void> closeAndRelease(final Channel channel) {
            try {
                Future<Void> closeFuture = closeChannel(channel).addListener(future -> {
                    URI channelUri = channel.attr(CHANNEL_URI).get();
                    if (leased.remove(channelUri, channel) || available.remove(channelUri, channel)) {
                        channelCount.decrementAndGet();
                        logger.debug("Channel closed and released out of pool: " + channel.id());
                    }
                    drain(channelUri);
                });
                return closeFuture;
            } catch (Exception e) {
                return bootstrap.config().group().next().newFailedFuture(e);
            }
        }

        @Override
        public Future<Void> release(final Channel channel) {
            try {
                handler.channelReleased(channel);
                URI channelUri = channel.attr(CHANNEL_URI).get();
                leased.remove(channelUri, channel);
                if (isChannelHealthy(channel)) {
                    available.put(channelUri, channel);
                    channel.attr(CHANNEL_AVAILABLE_SINCE).set(ZonedDateTime.now(ZoneOffset.UTC));
                    logger.debug("Channel released to pool: " + channel.id());
                } else {
                    channelCount.decrementAndGet();
                    logger.debug("Channel broken on release, dispose: " + channel.id());
                }
                drain(channelUri);
            } catch (Exception e) {
                return bootstrap.config().group().next().newFailedFuture(e);
            }
            return bootstrap.config().group().next().newSucceededFuture(null);
        }

        @Override
        public Future<Void> release(final Channel channel, final Promise<Void> promise) {
            return release(channel).addListener(f -> {
                if (f.isSuccess()) {
                    promise.setSuccess(null);
                } else {
                    promise.setFailure(f.cause());
                }
            });
        }

        @Override
        public void close() {
            closed = true;
            while (requests.size() != 0) {
                requests.poll().promise.setFailure(new CancellationException("Channel pool was closed"));
            }
        }

        private static class ChannelRequest {
            private URI destinationURI;
            private URI channelURI;
            private Proxy proxy;
            private Promise<Channel> promise;
        }

        /**
         * Used to print a current overview of the channels in this pool.
         */
        public void dump() {
            logger.info("Channel\tState\tFor\tAge\tURL");
            List<Channel> closed = new ArrayList<>();
            for (Channel channel : leased.values()) {
                if (channel.hasAttr(CHANNEL_CLOSED_SINCE)) {
                    closed.add(channel);
                }
            }
            for (Channel channel : available.values()) {
                if (channel.hasAttr(CHANNEL_CLOSED_SINCE)) {
                    closed.add(channel);
                }
            }
            logger.info(
                "Active channels: " + channelCount.get() + " Leaked or being initialized channels: " +
                    (channelCount.get() - leased.size() - available.size()));
        }
    }


    /**
     * A HttpResponse that is implemented using Netty.
     */
    private static final class NettyResponse extends HttpResponse {
        private final io.netty.handler.codec.http.HttpResponse rxnRes;
        private final Flowable<ByteBuf> contentStream;

        NettyResponse(io.netty.handler.codec.http.HttpResponse rxnRes, Flowable<ByteBuf> emitter) {
            this.rxnRes = rxnRes;
            this.contentStream = emitter;
        }

        @Override
        public int statusCode() {
            return rxnRes.status().code();
        }

        @Override
        public String headerValue(String headerName) {
            return rxnRes.headers().get(headerName);
        }

        @Override
        public HttpHeaders headers() {
            HttpHeaders headers = new HttpHeaders();
            for (Map.Entry<String, String> header : rxnRes.headers()) {
                headers.set(header.getKey(), header.getValue());
            }
            return headers;
        }

        private Single<ByteBuf> collectContent() {
            ByteBuf allContent = null;
            String contentLengthString = headerValue("Content-Length");
            if (contentLengthString != null) {
                try {
                    int contentLength = Integer.parseInt(contentLengthString);
                    allContent = Unpooled.buffer(contentLength);
                } catch (NumberFormatException ignored) {
                }
            }

            if (allContent == null) {
                allContent = Unpooled.buffer();
            }

            return contentStream.collectInto(allContent, (allContent1, chunk) -> {
                //use try-finally to ensure chunk gets released
                try {
                    allContent1.writeBytes(chunk);
                }
                finally {
                    chunk.release();
                }
            });
        }

        @Override
        public Single<byte[]> bodyAsByteArray() {
            return collectContent().map(byteBuf -> {
                byte[] result;
                if (byteBuf.readableBytes() == byteBuf.array().length) {
                    result = byteBuf.array();
                } else {
                    byte[] dst = new byte[byteBuf.readableBytes()];
                    byteBuf.readBytes(dst);
                    result = dst;
                }

                // This byteBuf is not pooled but Netty uses ref counting to track allocation metrics
                byteBuf.release();
                return result;
            });
        }

        @Override
        public Flowable<ByteBuffer> body() {
            return contentStream.map(byteBuf -> {
                ByteBuffer dst = ByteBuffer.allocate(byteBuf.readableBytes());
                byteBuf.readBytes(dst);
                byteBuf.release();
                dst.flip();
                return dst;
            });
        }

        @Override
        public Single<String> bodyAsString() {
            return collectContent().map(byteBuf -> {
                String result = byteBuf.toString(StandardCharsets.UTF_8);
                byteBuf.release();
                return result;
            });
        }

        @Override
        public void close() {
            contentStream.subscribe(new FlowableSubscriber<>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.cancel();
                }

                @Override
                public void onNext(ByteBuf byteBuf) {
                    // no-op
                }

                @Override
                public void onError(Throwable ignored) {
                    // May receive a "multiple subscription not allowed" error here, but we don't care
                }

                @Override
                public void onComplete() {
                    // no-op
                }
            });
        }
    }

}
