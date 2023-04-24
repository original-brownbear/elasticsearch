/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;

public final class Netty4BytesStream extends BytesStream {
    private final ByteBuf buffer;

    public Netty4BytesStream() {
        this.buffer = NettyAllocator.getAllocator().compositeHeapBuffer(Integer.MAX_VALUE);
    }

    @Override
    public BytesReference bytes() {
        return Netty4Utils.toBytesReference(buffer.duplicate());
    }

    @Override
    public void close() {
        // sometimes OutboundHandler does a double close, it's safe to guard against that as below since we don't escape the buffers
        // ref count
        if (buffer.refCnt() > 0) {
            buffer.release();
        }
    }

    @Override
    public void seek(long position) {
        if (position > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " cannot hold more than 2GB of data");
        }
        int pos = Math.toIntExact(position);
        buffer.ensureWritable(pos);
        buffer.writerIndex(pos);
    }

    @Override
    public long position() {
        return buffer.writerIndex();
    }

    @Override
    public void writeByte(byte b) {
        buffer.writeByte(b);
    }

    @Override
    public void writeInt(int i) {
        buffer.writeInt(i);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
                buffer.writeBytes(b, offset, length);
    }

    @Override
    public void flush() {

    }
}
