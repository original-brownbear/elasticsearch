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
package org.elasticsearch.common.bytes;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * Mark-supporting {@link StreamInput} over all the byte array.
 */
public final class ByteArrayStreamInput extends StreamInput {
    private final byte[] bytes;
    private final int limit;
    private int offset;
    private int mark;

    public ByteArrayStreamInput(byte[] bytes, int offset, int limit) {
        this.bytes = bytes;
        this.mark = offset;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public byte readByte() throws IOException {
        if (offset >= limit) {
            throw new EOFException();
        }
        return bytes[offset++];
    }

    @Override
    public int readInt() throws EOFException {
        if (limit - offset < Integer.BYTES) {
            throw new EOFException();
        }
        offset += Integer.BYTES;
        return BytesArray.intFromBytes(offset - Integer.BYTES, bytes);
    }

    @Override
    public long readLong() throws IOException {
        if (limit - offset < Long.BYTES) {
            throw new EOFException();
        }
        offset += Long.BYTES;
        return BytesArray.longFromBytes(offset - Long.BYTES, bytes);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
        final int read = read(b, offset, len);
        if (read != len) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (len == 0) {
            return 0;
        }
        if (offset >= limit) {
            return -1;
        }
        final int read = Math.min(len, limit - offset);
        System.arraycopy(bytes, offset, b, off, read);
        assert read <= len;
        if (read > 0) {
            offset += read;
        }
        return read;
    }

    @Override
    public void close() {
    }

    @Override
    public int read() throws IOException {
        if (offset >= limit) {
            return -1;
        }
        return Byte.toUnsignedInt(readByte());
    }

    @Override
    public int available() {
        return limit - offset;
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        int bytesAvailable = limit - offset;
        if (bytesAvailable < length) {
            throw new EOFException("tried to read: " + length + " bytes but only " + bytesAvailable + " remaining");
        }
    }

    @Override
    public void reset() {
        offset = mark;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readLimit) {
        // readLimit is irrelevant since we always have random access to all the bytes in the reference
        this.mark = offset;
    }

    @Override
    public long skip(long n) {
        final int skip = (int) Math.min(Integer.MAX_VALUE, n);
        final int numBytesSkipped = Math.min(skip, limit - offset);
        offset += numBytesSkipped;
        return numBytesSkipped;
    }

    @Override
    public String readString() throws IOException {
        final int charCount = readArraySize();
        final CharsRef charsRef;
        if (charCount > SMALL_STRING_LIMIT) {
            if (largeSpare == null) {
                largeSpare = new CharsRef(ArrayUtil.oversize(charCount, Character.BYTES));
            } else if (largeSpare.chars.length < charCount) {
                // we don't use ArrayUtils.grow since there is no need to copy the array
                largeSpare.chars = new char[ArrayUtil.oversize(charCount, Character.BYTES)];
            }
            charsRef = largeSpare;
        } else {
            charsRef = smallSpare.get();
        }
        charsRef.length = charCount;
        int charsOffset = 0;

        final char[] charBuffer = charsRef.chars;
        try {
            for (; charsOffset < charCount; ) {
                final int c = bytes[offset++] & 0xff;
                switch (c >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        charBuffer[charsOffset++] = (char) c;
                        break;
                    case 12:
                    case 13:
                        charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | bytes[offset++] & 0x3F);
                        break;
                    case 14:
                        charBuffer[charsOffset++] = (char) (
                                (c & 0x0F) << 12 | (bytes[offset++] & 0x3F) << 6 | (bytes[offset++] & 0x3F));
                        break;
                    default:
                        throwOnBrokenChar(c);
                }
            }
        } catch (ArrayIndexOutOfBoundsException ignored) {
            // we expect to never run into this for a byte array except for when dealing with a corrupted message
            throw new EOFException();
        }
        if (offset > limit) {
            throw new EOFException();
        }
        return charsRef.toString();
    }
}
