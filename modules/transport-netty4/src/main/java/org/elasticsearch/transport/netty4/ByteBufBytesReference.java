/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;

import io.netty.buffer.ByteBufInputStream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.AbstractBytesReference;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public final class ByteBufBytesReference extends AbstractBytesReference {

    private final ByteBuf byteBuf;

    public ByteBufBytesReference(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }

    @Override
    public byte get(int index) {
        return byteBuf.getByte(index);
    }

    @Override
    public int indexOf(byte marker, int from) {
        return byteBuf.indexOf(from, length(), marker);
    }

    @Override
    public int length() {
        return byteBuf.readableBytes();
    }

    @Override
    public int hashCode() {
        // NOOP override to satisfy Checkstyle's EqualsHashCode
        return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof final ByteBufBytesReference that) {
            return byteBuf.equals(that.byteBuf);
        }
        return super.equals(other);
    }

    @Override
    public BytesReference slice(int from, int length) {
        if (from == 0 && length() == length) {
            return this;
        }
        Objects.checkFromIndexSize(from, length, length());
        return new ByteBufBytesReference(byteBuf.slice(from, length));
    }

    @Override
    public boolean hasArray() {
        return byteBuf.hasArray();
    }

    @Override
    public byte[] array() {
        return byteBuf.array();
    }

    @Override
    public int arrayOffset() {
        return byteBuf.arrayOffset();
    }

    @Override
    public BytesRef toBytesRef() {
        if (byteBuf.hasArray()) {
            return new BytesRef(array(), arrayOffset(), length());
        }
        final byte[] bytes = new byte[length()];
        byteBuf.getBytes(0, bytes);
        return new BytesRef(bytes);
    }

    @Override
    public long ramBytesUsed() {
        return length();
    }

    @Override
    public StreamInput streamInput() {
        if (hasArray()) {
            return StreamInput.wrap(array(), arrayOffset(), length());
        }
        return new InputStreamStreamInput(new ByteBufInputStream(byteBuf));
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        byteBuf.duplicate().readBytes(os, length());
    }

    @Override
    public int getInt(int index) {
        return byteBuf.getInt(index);
    }

    @Override
    public long getLongLE(int index) {
        return byteBuf.getLongLE(index);
    }

    @Override
    public int getIntLE(int index) {
        return byteBuf.getIntLE(index);
    }

    @Override
    public double getDoubleLE(int index) {
        return byteBuf.getDoubleLE(index);
    }

    public ByteBuf byteBuf() {
        return byteBuf;
    }
}
