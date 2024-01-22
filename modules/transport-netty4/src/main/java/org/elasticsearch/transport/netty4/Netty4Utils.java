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
import io.netty.buffer.Unpooled;
import io.netty.util.NettyRuntime;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.AbstractBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Booleans;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

public class Netty4Utils {

    private static final AtomicBoolean isAvailableProcessorsSet = new AtomicBoolean();

    /**
     * Set the number of available processors that Netty uses for sizing various resources (e.g., thread pools).
     *
     * @param availableProcessors the number of available processors
     * @throws IllegalStateException if available processors was set previously and the specified value does not match the already-set value
     */
    public static void setAvailableProcessors(final int availableProcessors) {
        // we set this to false in tests to avoid tests that randomly set processors from stepping on each other
        final boolean set = Booleans.parseBoolean(System.getProperty("es.set.netty.runtime.available.processors", "true"));
        if (set == false) {
            return;
        }

        /*
         * This can be invoked twice, once from Netty4Transport and another time from Netty4HttpServerTransport; however,
         * Netty4Runtime#availableProcessors forbids settings the number of processors twice so we prevent double invocation here.
         */
        if (isAvailableProcessorsSet.compareAndSet(false, true)) {
            NettyRuntime.setAvailableProcessors(availableProcessors);
        } else if (availableProcessors != NettyRuntime.availableProcessors()) {
            /*
             * We have previously set the available processors yet either we are trying to set it to a different value now or there is a bug
             * in Netty and our previous value did not take, bail.
             */
            final String message = String.format(
                Locale.ROOT,
                "available processors value [%d] did not match current value [%d]",
                availableProcessors,
                NettyRuntime.availableProcessors()
            );
            throw new IllegalStateException(message);
        }
    }

    /**
     * Turns the given BytesReference into a ByteBuf. Note: the returned ByteBuf will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ByteBuf goes out of scope.
     */
    public static ByteBuf toByteBuf(final BytesReference reference) {
        if (reference.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        if (reference instanceof ByteBufBytesReference byteBufBytesReference) {
            return byteBufBytesReference.toByteBuf();
        } else if (reference.hasArray()) {
            return Unpooled.wrappedBuffer(reference.array(), reference.arrayOffset(), reference.length());
        } else {
            final BytesRefIterator iterator = reference.iterator();
            // usually we have one, two, or three components from the header, the message, and a buffer
            final List<ByteBuf> buffers = new ArrayList<>(3);
            try {
                BytesRef slice;
                while ((slice = iterator.next()) != null) {
                    buffers.add(Unpooled.wrappedBuffer(slice.bytes, slice.offset, slice.length));
                }

                if (buffers.size() == 1) {
                    return buffers.get(0);
                } else {
                    CompositeByteBuf composite = Unpooled.compositeBuffer(buffers.size());
                    composite.addComponents(true, buffers);
                    return composite;
                }
            } catch (IOException ex) {
                throw new AssertionError("no IO happens here", ex);
            }
        }
    }

    public static Recycler<BytesRef> createRecycler(Settings settings) {
        // If this method is called by super ctor the processors will not be set. Accessing NettyAllocator initializes netty's internals
        // setting the processors. We must do it ourselves first just in case.
        setAvailableProcessors(EsExecutors.allocatedProcessors(settings));
        return NettyAllocator.getRecycler();
    }

    /**
     * Wraps the given ChannelBuffer with a BytesReference
     */
    public static BytesReference toBytesReference(final ByteBuf buffer) {
        final int readableBytes = buffer.readableBytes();
        if (readableBytes == 0) {
            return BytesArray.EMPTY;
        }
        if (buffer.hasArray()) {
            return new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), buffer.readableBytes());
        }
        return new ByteBufBytesReference(buffer, readableBytes);
    }

    private static class ByteBufBytesReference extends AbstractBytesReference {

        private final ByteBuf buffer;
        private final int offset;

        ByteBufBytesReference(ByteBuf buffer, int length) {
            super(length);
            this.buffer = buffer;
            this.offset = buffer.readerIndex();
            assert length <= buffer.readableBytes() : "length[" + length + "] > " + buffer.readableBytes();
        }

        @Override
        public byte get(int index) {
            return buffer.getByte(offset + index);
        }

        @Override
        public int getInt(int index) {
            return buffer.getInt(offset + index);
        }

        @Override
        public int indexOf(byte marker, int from) {
            final int start = offset + from;
            return buffer.forEachByte(start, length - start, value -> value != marker);
        }

        @Override
        public BytesReference slice(int from, int length) {
            return new ByteBufBytesReference(buffer.slice(offset + from, length), length);
        }

        @Override
        public void writeTo(OutputStream os) throws IOException {
            buffer.getBytes(offset, os, length);
        }

        ByteBuf toByteBuf() {
            return Unpooled.unreleasableBuffer(buffer);
        }

        @Override
        public String utf8ToString() {
            return buffer.toString(offset, length, StandardCharsets.UTF_8);
        }

        @Override
        public BytesRef toBytesRef() {
            if (buffer.hasArray()) {
                return new BytesRef(buffer.array(), buffer.arrayOffset() + offset, length);
            }
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            return new BytesRef(copy);
        }

        @Override
        public BytesRefIterator iterator() {
            final Iterator<ByteBuffer> buffers = Iterators.forArray(buffer.nioBuffers());
            return () -> {
                if (buffers.hasNext() == false) {
                    return null;
                }
                var buf = buffers.next();
                return new BytesRef(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
            };
        }

        @Override
        public boolean hasArray() {
            return buffer.hasArray();
        }

        @Override
        public byte[] array() {
            return buffer.array();
        }

        @Override
        public int arrayOffset() {
            return buffer.arrayOffset();
        }

        @Override
        public long ramBytesUsed() {
            return buffer.capacity();
        }
    }
}
