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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
        if (reference instanceof ByteBufBytesReference byteBufBytesReference) {
            return byteBufBytesReference.buffer;
        }
        if (reference instanceof CompositeBytesReference compositeBytesReference) {
            final var refs = compositeBytesReference.references();
            final var bufs = new ByteBuf[refs.length];
            for (int i = 0; i < refs.length; i++) {
                bufs[i] = toByteBuf(refs[i]);
            }
            return Unpooled.compositeBuffer(bufs.length).addComponents(true, bufs);
        }
        if (reference.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        if (reference.hasArray()) {
            return Unpooled.wrappedBuffer(reference.array(), reference.arrayOffset(), reference.length());
        }
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

    /**
     * Wraps the given ChannelBuffer with a BytesReference
     */
    public static ReleasableBytes toBytesReference(final ByteBuf buffer) {
        return new ByteBufBytesReference(buffer);
    }

    public static Recycler<BytesRef> createRecycler(Settings settings) {
        // If this method is called by super ctor the processors will not be set. Accessing NettyAllocator initializes netty's internals
        // setting the processors. We must do it ourselves first just in case.
        setAvailableProcessors(EsExecutors.allocatedProcessors(settings));
        return NettyAllocator.getRecycler();
    }

    public static final class ByteBufBytesReference implements ReleasableBytes {
        private final ByteBuf buffer;

        ByteBufBytesReference(ByteBuf buffer) {
            this.buffer = buffer;
        }

        @Override
        public byte get(int index) {
            return buffer.getByte(index);
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
            return buffer.arrayOffset() + buffer.readerIndex();
        }

        @Override
        public int getInt(int index) {
            return buffer.getInt(index);
        }

        @Override
        public int getIntLE(int index) {
            return buffer.getIntLE(index);
        }

        @Override
        public long getLongLE(int index) {
            return buffer.getLongLE(index);
        }

        @Override
        public double getDoubleLE(int index) {
            return buffer.getDoubleLE(index);
        }

        @Override
        public int indexOf(byte marker, int from) {
            return buffer.indexOf(from, buffer.writerIndex(), marker);
        }

        @Override
        public int length() {
            return buffer.readableBytes();
        }

        @Override
        public BytesReference slice(int from, int length) {
            return new ByteBufBytesReference(buffer.slice(from, length));
        }

        @Override
        public long ramBytesUsed() {
            return buffer.capacity();
        }

        @Override
        public StreamInput streamInput() throws IOException {
            return new ByteBufStreamInput(buffer.slice());
        }

        @Override
        public void writeTo(OutputStream os) throws IOException {
            buffer.getBytes(buffer.readerIndex(), os, buffer.readableBytes());
        }

        @Override
        public String utf8ToString() {
            return buffer.toString(StandardCharsets.UTF_8);
        }

        @Override
        public BytesRef toBytesRef() {
            final int readableBytes = buffer.readableBytes();
            if (readableBytes == 0) {
                return new BytesRef();
            } else if (buffer.hasArray()) {
                return new BytesRef(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), readableBytes);
            } else {
                final byte[] bytes = new byte[readableBytes];
                buffer.getBytes(buffer.readerIndex(), bytes);
                return new BytesRef(bytes);
            }
        }

        @Override
        public BytesRefIterator iterator() {
            int readableBytes = buffer.readableBytes();
            if (readableBytes == 0) {
                return BytesRefIterator.EMPTY;
            } else if (buffer.hasArray() == false) {
                final ByteBuffer[] byteBuffers = buffer.nioBuffers();
                return BytesReference.fromByteBuffers(byteBuffers).iterator();
            }
            return new BytesRefIterator() {
                BytesRef ref = length() == 0
                    ? null
                    : new BytesRef(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), readableBytes);

                @Override
                public BytesRef next() {
                    BytesRef r = ref;
                    ref = null; // only return it once...
                    return r;
                }
            };
        }

        @Override
        public int compareTo(BytesReference o) {
            return buffer.compareTo(Netty4Utils.toByteBuf(o));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            BytesRef bytes = toBytesRef();
            return builder.value(bytes.bytes, bytes.offset, bytes.length);
        }

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

        @Override
        public void close() {
            buffer.release();
        }

        @Override
        public ReleasableBytes retainedSlice(int from, int length) {
            return new ByteBufBytesReference(buffer.retainedSlice(from, length));
        }

        @Override
        public ReleasableBytes retain() {
            incRef();
            return this;
        }

        private static final class ByteBufStreamInput extends StreamInput {
            private final ByteBuf byteBuf;

            ByteBufStreamInput(ByteBuf byteBuf) {
                this.byteBuf = byteBuf;
            }

            @Override
            public void mark(int readlimit) {
                byteBuf.markReaderIndex();
            }

            @Override
            public void reset() {
                byteBuf.resetReaderIndex();
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public byte readByte() {
                return byteBuf.readByte();
            }

            @Override
            public short readShort() {
                return byteBuf.readShort();
            }

            @Override
            public int readInt() {
                return byteBuf.readInt();
            }

            @Override
            public long readLong() {
                return byteBuf.readLong();
            }

            @Override
            public int read(byte[] b, int off, int len) {
                int readable = byteBuf.readableBytes();
                if (readable == 0) {
                    return -1;
                }
                int toRead = Math.min(len, readable);
                byteBuf.readBytes(b, off, toRead);
                return toRead;
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) {
                byteBuf.readBytes(b, offset, len);
            }

            @Override
            public void close() {

            }

            @Override
            public int available() {
                return byteBuf.readableBytes();
            }

            @Override
            protected void ensureCanReadBytes(int length) throws EOFException {
                final int available = available();
                if (length > available) {
                    throwEOF(length, available);
                }
            }

            @Override
            public int read() {
                if (byteBuf.isReadable() == false) {
                    return -1;
                }
                return byteBuf.readByte();
            }

            @Override
            public ReleasableBytes readReleasableBytesReference() throws IOException {
                final int len = readArraySize();
                return new ByteBufBytesReference(byteBuf.readRetainedSlice(len));
            }

            @Override
            public boolean supportReadAllToReleasableBytesReference() {
                return true;
            }

            @Override
            public ReleasableBytes readAllToReleasableBytesReference() {
                return new ByteBufBytesReference(byteBuf.readRetainedSlice(byteBuf.readableBytes()));
            }
        }
    }
}
