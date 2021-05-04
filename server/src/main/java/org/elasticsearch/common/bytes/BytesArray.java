/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class BytesArray extends AbstractBytesReference {

    public static final BytesArray EMPTY = new BytesArray(BytesRef.EMPTY_BYTES, 0, 0, true);
    private final byte[] bytes;
    private final int offset;
    private final int length;
    private final boolean unpooled;

    public BytesArray(String bytes) {
        this(new BytesRef(bytes));
    }

    public static BytesArray wrapUnpooled(BytesRef bytesRef) {
        return new BytesArray(bytesRef.bytes, bytesRef.offset, bytesRef.length, true);
    }

    public static BytesArray wrap(BytesRef bytesRef) {
        return new BytesArray(bytesRef);
    }

    private BytesArray(BytesRef bytesRef) {
        this(bytesRef.bytes, bytesRef.offset, bytesRef.length, true);
    }

    public static BytesArray copy(BytesRef bytesRef) {
        final byte[] bytes = new byte[bytesRef.length];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, 0, bytesRef.length);
        return new BytesArray(bytes, true);
    }

    public BytesArray(byte[] bytes) {
        this(bytes, false);
    }

    public BytesArray(byte[] bytes, boolean unpooled) {
        this(bytes, 0, bytes.length, unpooled);
    }

    public BytesArray(byte[] bytes, int offset, int length) {
        this(bytes, offset, length, false);
    }

    public BytesArray(byte[] bytes, int offset, int length, boolean unpooled) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.unpooled = unpooled;
    }

    @Override
    public byte get(int index) {
        return bytes[offset + index];
    }

    @Override
    public int length() {
        return length;
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
        if (other instanceof BytesArray) {
            final BytesArray that = (BytesArray) other;
            return Arrays.equals(bytes, offset, offset + length, that.bytes, that.offset, that.offset + that.length);
        }
        return super.equals(other);
    }

    @Override
    public BytesReference slice(int from, int length) {
        if (from == 0 && this.length == length) {
            return this;
        }
        Objects.checkFromIndexSize(from, length, this.length);
        return new BytesArray(bytes, offset + from, length, unpooled);
    }

    @Override
    public boolean hasArray() {
        return true;
    }

    @Override
    public byte[] array() {
        return bytes;
    }

    @Override
    public int arrayOffset() {
        return offset;
    }

    @Override
    public BytesRef toBytesRef() {
        return new BytesRef(bytes, offset, length);
    }

    @Override
    public long ramBytesUsed() {
        return bytes.length;
    }

    @Override
    public StreamInput streamInput() {
        if (unpooled) {
            return new ByteBufferStreamInput(ByteBuffer.wrap(bytes, offset, length)) {
                @Override
                public BytesReference readBytesReference(int length) {
                    final int bufferPos = buffer.position();
                    final BytesReference res = new BytesArray(bytes, offset + bufferPos, length, true);
                    buffer.position(bufferPos + length);
                    return res;
                }
            };
        }
        return StreamInput.wrap(bytes, offset, length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(bytes, offset, length);
    }

    @Override
    public boolean unpooled() {
        return unpooled;
    }
}
