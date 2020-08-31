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
package org.elasticsearch.common.io.stream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class ByteBufferStreamInput extends StreamInput {

    private final ByteBuffer buffer;

    public ByteBufferStreamInput(ByteBuffer buffer) {
        this.buffer = buffer.mark();
    }

    @Override
    public int read() throws IOException {
        if (!buffer.hasRemaining()) {
            return -1;
        }
        return buffer.get() & 0xFF;
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return buffer.get();
        } catch (BufferUnderflowException ex) {
            throw wrapAsEOF(ex);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final int remaining = buffer.remaining();
        if (remaining < 1) {
            return -1;
        }

        len = Math.min(len, remaining);
        buffer.get(b, off, len);
        return len;
    }

    @Override
    public long skip(long n) {
        n = Math.min(n, buffer.remaining());
        buffer.position((int) (buffer.position() + n));
        return n;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        try {
            buffer.get(b, offset, len);
        } catch (BufferUnderflowException ex) {
            throw wrapAsEOF(ex);
        }
    }

    @Override
    public short readShort() throws IOException {
        try {
            return buffer.getShort();
        } catch (BufferUnderflowException ex) {
            throw wrapAsEOF(ex);
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return buffer.getInt();
        } catch (BufferUnderflowException ex) {
            throw wrapAsEOF(ex);
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            return buffer.getLong();
        } catch (BufferUnderflowException ex) {
            throw wrapAsEOF(ex);
        }
    }

    private static EOFException wrapAsEOF(BufferUnderflowException ex) {
        EOFException eofException = new EOFException();
        eofException.initCause(ex);
        return eofException;
    }

    @Override
    public void reset() throws IOException {
        buffer.reset();
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        if (buffer.remaining() < length) {
            throw new EOFException("tried to read: " + length + " bytes but only " + buffer.remaining() + " remaining");
        }
    }

    @Override
    public void mark(int readlimit) {
        buffer.mark();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void close() throws IOException {
    }
}
