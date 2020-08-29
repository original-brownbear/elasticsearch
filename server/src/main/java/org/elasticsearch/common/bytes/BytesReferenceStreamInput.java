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

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.common.io.stream.BufferedStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * A StreamInput that reads off a {@link BytesRefIterator}. This is used to provide
 * generic stream access to {@link BytesReference} instances without materializing the
 * underlying bytes reference.
 */
final class BytesReferenceStreamInput extends StreamInput {
    private final BytesReference reference;

    private BytesRefIterator iterator;
    private int sliceIndex;
    private BytesRef slice;
    private int sliceStartOffset; // the current position of the stream

    private int mark = 0;

    BytesReferenceStreamInput(BytesReference reference) throws IOException {
        this.reference = reference;
        this.iterator = reference.iterator();
        this.slice = iterator.next();
        this.sliceStartOffset = 0;
        this.sliceIndex = 0;
    }

    @Override
    public byte readByte() throws IOException {
        if (offset() >= reference.length()) {
            throw new EOFException();
        }
        maybeNextSlice();
        return slice.bytes[slice.offset + (sliceIndex++)];
    }

    private int offset() {
        return sliceStartOffset + sliceIndex;
    }

    private void maybeNextSlice() throws IOException {
        while (sliceIndex == slice.length) {
            sliceStartOffset += sliceIndex;
            slice = iterator.next();
            sliceIndex = 0;
            if (slice == null) {
                throw new EOFException();
            }
        }
    }

    @Override
    public void readBytes(byte[] b, int bOffset, int len) throws IOException {
        final int length = reference.length();
        final int offset = offset();
        if (offset + len > length) {
            throw new IndexOutOfBoundsException(
                    "Cannot read " + len + " bytes from stream with length " + length + " at offset " + offset);
        }
        read(b, bOffset, len);
    }

    @Override
    public int read() throws IOException {
        if (offset() >= reference.length()) {
            return -1;
        }
        return Byte.toUnsignedInt(readByte());
    }

    @Override
    public int read(final byte[] b, final int bOffset, final int len) throws IOException {
        final int length = reference.length();
        final int offset = offset();
        if (offset >= length) {
            return -1;
        }
        final int numBytesToCopy =  Math.min(len, length - offset);
        int remaining = numBytesToCopy; // copy the full length or the remaining part
        int destOffset = bOffset;
        while (remaining > 0) {
            maybeNextSlice();
            final int currentLen = Math.min(remaining, slice.length - sliceIndex);
            assert currentLen > 0 : "length has to be > 0 to make progress but was: " + currentLen;
            System.arraycopy(slice.bytes, slice.offset + sliceIndex, b, destOffset, currentLen);
            destOffset += currentLen;
            remaining -= currentLen;
            sliceIndex += currentLen;
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesToCopy;
    }

    @Override
    public short readShort() throws IOException {
        maybeNextSlice();
        if (maySkipBoundsChecks(2)) {
            final int position = slice.offset + sliceIndex;
            sliceIndex += 2;
            final byte[] buf = slice.bytes;
            return (short) (((buf[position] & 0xFF) << 8) | (buf[position + 1] & 0xFF));
        }
        return super.readShort();
    }

    @Override
    public int readInt() throws IOException {
        maybeNextSlice();
        if (maySkipBoundsChecks(4)) {
            final int position = slice.offset + sliceIndex;
            sliceIndex += 4;
            return BufferedStreamInput.intFromBytes(position, slice.bytes);
        }
        return super.readInt();
    }

    @Override
    public int readVInt() throws IOException {
        maybeNextSlice();
        if (maySkipBoundsChecks(5)) {
            final byte[] buf = slice.bytes;
            byte b = buf[slice.offset + (sliceIndex++)];
            int i = b & 0x7F;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7F) << 7;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7F) << 14;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7F) << 21;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            if ((b & 0x80) != 0) {
                throw new IOException("Invalid vInt ((" + Integer.toHexString(b) + " & 0x7f) << 28) | " + Integer.toHexString(i));
            }
            return i | ((b & 0x7F) << 28);
        }
        return readVIntSlow(this);
    }

    @Override
    public long readLong() throws IOException {
        maybeNextSlice();
        if (maySkipBoundsChecks(8)) {
            final int position = slice.offset + sliceIndex;
            sliceIndex += 8;
            return BufferedStreamInput.longFromBytes(position, slice.bytes);
        }
        return super.readLong();
    }

    @Override
    public long readVLong() throws IOException {
        maybeNextSlice();
        if (maySkipBoundsChecks(10)) {
            final byte[] buf = slice.bytes;
            byte b = buf[slice.offset + (sliceIndex++)];
            long i = b & 0x7FL;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7FL) << 7;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7FL) << 14;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7FL) << 21;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7FL) << 28;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7FL) << 35;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7FL) << 42;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= (b & 0x7FL) << 49;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            i |= ((b & 0x7FL) << 56);
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[slice.offset + (sliceIndex++)];
            if (b != 0 && b != 1) {
                throw new IOException("Invalid vlong (" + Integer.toHexString(b) + " << 63) | " + Long.toHexString(i));
            }
            i |= ((long) b) << 63;
            return i;
        }
        return readVLongSlow(this);
    }

    @Override
    public long readZLong() throws IOException {
        maybeNextSlice();
        if (maySkipBoundsChecks(10)) {
            final byte[] buf = slice.bytes;
            long accumulator = 0L;
            int i = 0;
            long currentByte;
            while (((currentByte = buf[slice.offset + (sliceIndex++)]) & 0x80L) != 0) {
                accumulator |= (currentByte & 0x7F) << i;
                i += 7;
                if (i > 63) {
                    throw new IOException("variable-length stream is too long");
                }
            }
            return BitUtil.zigZagDecode(accumulator | (currentByte << i));
        }
        return readZLongSlow(this);
    }

    @Override
    public String readString() throws IOException {
        final int charCount = readArraySize();
        final CharsRef charsRef = charsRef(charCount);
        if (maySkipBoundsChecks(charCount * 3)) {
            sliceIndex = BufferedStreamInput.readCharsUnsafe(slice.bytes, charsRef.chars, slice.offset + sliceIndex, charsRef.length)
                    - slice.offset;
        } else {
            super.readStringSlow(charsRef);
        }
        return charsRef.toString();
    }

    private boolean maySkipBoundsChecks(int len) {
        return reference.length() - sliceStartOffset == slice.length || slice.length - sliceIndex >= len;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public int available() {
        return reference.length() - offset();
    }

    @Override
    protected void ensureCanReadBytes(int bytesToRead) throws EOFException {
        int bytesAvailable = reference.length() - offset();
        if (bytesAvailable < bytesToRead) {
            throw new EOFException("tried to read: " + bytesToRead + " bytes but only " + bytesAvailable + " remaining");
        }
    }

    @Override
    public long skip(long n) throws IOException {
        final int skip = (int) Math.min(Integer.MAX_VALUE, n);
        final int numBytesSkipped =  Math.min(skip, reference.length() - offset());
        int remaining = numBytesSkipped;
        while (remaining > 0) {
            maybeNextSlice();
            int currentLen = Math.min(remaining, slice.length - sliceIndex);
            remaining -= currentLen;
            sliceIndex += currentLen;
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesSkipped;
    }

    @Override
    public void reset() throws IOException {
        iterator = reference.iterator();
        slice = iterator.next();
        sliceStartOffset = 0;
        sliceIndex = 0;
        skip(mark);
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readLimit) {
        // readLimit is optional it only guarantees that the stream remembers data upto this limit but it can remember more
        // which we do in our case
        this.mark = offset();
    }
}
