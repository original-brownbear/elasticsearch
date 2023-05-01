/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A StreamInput that reads off a {@link BytesRefIterator}. This is used to provide
 * generic stream access to {@link BytesReference} instances without materializing the
 * underlying bytes.
 */
class BytesReferenceStreamInput extends StreamInput {

    private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);

    protected final BytesReference bytesReference;
    private BytesRefIterator iterator;
    private ByteBuffer slice;
    private int totalOffset; // the offset on the stream at which the current slice starts
    private int mark = 0;

    BytesReferenceStreamInput(BytesReference bytesReference) throws IOException {
        this.bytesReference = bytesReference;
        this.iterator = bytesReference.iterator();
        this.slice = convertToByteBuffer(iterator.next());
        this.totalOffset = 0;
    }

    static ByteBuffer convertToByteBuffer(BytesRef bytesRef) {
        if (bytesRef == null) {
            return EMPTY;
        }
        // slice here forces the buffer to have a sliced view, keeping track of the original offset
        return ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length).slice();
    }

    @Override
    public byte readByte() throws IOException {
        maybeNextSlice();
        return slice.get();
    }

    @Override
    public short readShort() throws IOException {
        if (slice.remaining() >= 2) {
            return slice.getShort();
        } else {
            // slow path
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (slice.remaining() >= 4) {
            return slice.getInt();
        } else {
            // slow path
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (slice.remaining() >= 8) {
            return slice.getLong();
        } else {
            // slow path
            return super.readLong();
        }
    }

    @Override
    public int readVInt() throws IOException {
        if (slice.remaining() >= 5) {
            byte b = slice.get();
            if (b >= 0) {
                return b;
            }
            int i = b & 0x7F;
            b = slice.get();
            i |= (b & 0x7F) << 7;
            if (b >= 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7F) << 14;
            if (b >= 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7F) << 21;
            if (b >= 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x0F) << 28;
            if ((b & 0xF0) == 0) {
                return i;
            }
            throwOnBrokenVInt(b, i);
        }
        return super.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        if (slice.remaining() >= 10) {
            byte b = slice.get();
            long i = b & 0x7FL;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7FL) << 7;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7FL) << 14;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7FL) << 21;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7FL) << 28;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7FL) << 35;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7FL) << 42;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            i |= (b & 0x7FL) << 49;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            i |= ((b & 0x7FL) << 56);
            if ((b & 0x80) == 0) {
                return i;
            }
            b = slice.get();
            if (b != 0 && b != 1) {
                throwOnBrokenVLong(b, i);
            }
            i |= ((long) b) << 63;
            return i;
        } else {
            return super.readVLong();
        }
    }

    protected int offset() {
        return totalOffset + slice.position();
    }

    private void maybeNextSlice() throws IOException {
        if (slice.hasRemaining() == false) {
            // moveToNextSlice is intentionally extracted to another method since it's the assumed cold-path
            moveToNextSlice();
        }
    }

    private void moveToNextSlice() throws IOException {
        totalOffset += slice.limit();
        BytesRef bytesRef = iterator.next();
        while (bytesRef != null && bytesRef.length == 0) {
            // rare corner case of a bytes reference that has a 0-length component
            bytesRef = iterator.next();
        }
        if (bytesRef == null) {
            throw new EOFException();
        }
        slice = convertToByteBuffer(bytesRef);
        assert slice.position() == 0;
    }

    @Override
    public void readBytes(byte[] b, int bOffset, int len) throws IOException {
        Objects.checkFromIndexSize(offset(), len, bytesReference.length());
        final int bytesRead = read(b, bOffset, len);
        assert bytesRead == len : bytesRead + " vs " + len;
    }

    @Override
    public int read() throws IOException {
        if (offset() >= bytesReference.length()) {
            return -1;
        }
        return Byte.toUnsignedInt(readByte());
    }

    @Override
    public String readString() throws IOException {
        final int charCount = readVInt();
        int sliceRemaining = slice.remaining();
        if (sliceRemaining >= charCount * 3 || sliceRemaining == available()) {
            byte[] arr = slice.array();
            final int arrOffset = slice.arrayOffset();
            int offset = arrOffset + slice.position();
            if (allAscii(arr, offset, charCount)) {
                slice.position(offset + charCount - arrOffset);
                return new String(arr, offset, charCount, StandardCharsets.US_ASCII);
            }
            final char[] charBuffer = charCount > SMALL_STRING_LIMIT ? ensureLargeSpare(charCount) : smallSpare.get();
            slice.position(fastCharsLoop(arr, offset, charCount, charBuffer) - arrOffset);
            return new String(charBuffer, 0, charCount);
        } else {
            final char[] charBuffer = charCount > SMALL_STRING_LIMIT ? ensureLargeSpare(charCount) : smallSpare.get();
            slowCharsLoop(charCount, charBuffer);
            return new String(charBuffer, 0, charCount);
        }
    }

    private void slowCharsLoop(int charCount, char[] charBuffer) throws IOException {
        for (int i = 0; i < charCount; i++) {
            final int c = readByte() & 0xff;
            switch (c >> 4) {
                case 0, 1, 2, 3, 4, 5, 6, 7 -> charBuffer[i] = (char) c;
                case 12, 13 -> charBuffer[i] = ((char) ((c & 0x1F) << 6 | readByte() & 0x3F));
                case 14 -> charBuffer[i] = ((char) ((c & 0x0F) << 12 | (readByte() & 0x3F) << 6 | (readByte() & 0x3F)));
                default -> throwOnBrokenChar(c);
            }
        }
    }

    private static boolean allAscii(byte[] bytes, int offset, int len) {
        for (int i = 0; i < len; i++) {
            if (((bytes[offset + i] & 0xff) & 0x80) != 0) {
                return false;
            }
        }
        return true;
    }

    private static int fastCharsLoop(byte[] arr, int offset, int charCount, char[] charBuffer) throws IOException {
        for (int i = 0; i < charCount; i++) {
            final int c = arr[offset++] & 0xff;
            switch (c >> 4) {
                case 0, 1, 2, 3, 4, 5, 6, 7 -> charBuffer[i] = (char) c;
                case 12, 13 -> charBuffer[i] = twoBytes(c, arr[offset++]);
                case 14 -> charBuffer[i] = threeBytes(c, arr[offset++], arr[offset++]);
                default -> throwOnBrokenChar(c);
            }
        }
        return offset;
    }

    private static char twoBytes(int first, int second) {
        return ((char) ((first & 0x1F) << 6 | second & 0x3F));
    }

    private static char threeBytes(int first, int second, int third) {
        return ((char) ((first & 0x0F) << 12 | (second & 0x3F) << 6 | (third & 0x3F)));
    }

    @Override
    public int read(final byte[] b, final int bOffset, final int len) throws IOException {
        final int length = bytesReference.length();
        final int offset = offset();
        if (offset >= length) {
            return -1;
        }
        final int numBytesToCopy = Math.min(len, length - offset);
        int remaining = numBytesToCopy; // copy the full length or the remaining part
        int destOffset = bOffset;
        while (remaining > 0) {
            maybeNextSlice();
            final int currentLen = Math.min(remaining, slice.remaining());
            assert currentLen > 0 : "length has to be > 0 to make progress but was: " + currentLen;
            slice.get(b, destOffset, currentLen);
            destOffset += currentLen;
            remaining -= currentLen;
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesToCopy;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public int available() {
        return bytesReference.length() - offset();
    }

    @Override
    protected void ensureCanReadBytes(int bytesToRead) throws EOFException {
        int bytesAvailable = bytesReference.length() - offset();
        if (bytesAvailable < bytesToRead) {
            throwEOF(bytesToRead, bytesAvailable);
        }
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0L) {
            return 0L;
        }
        assert offset() <= bytesReference.length() : offset() + " vs " + bytesReference.length();
        // definitely >= 0 and <= Integer.MAX_VALUE so casting is ok
        final int numBytesSkipped = (int) Math.min(n, bytesReference.length() - offset());
        int remaining = numBytesSkipped;
        while (remaining > 0) {
            maybeNextSlice();
            int currentLen = Math.min(remaining, slice.remaining());
            remaining -= currentLen;
            slice.position(slice.position() + currentLen);
            assert remaining >= 0 : "remaining: " + remaining;
        }
        return numBytesSkipped;
    }

    @Override
    public void reset() throws IOException {
        if (totalOffset <= mark) {
            slice.position(mark - totalOffset);
        } else {
            iterator = bytesReference.iterator();
            slice = convertToByteBuffer(iterator.next());
            totalOffset = 0;
            final long skipped = skip(mark);
            assert skipped == mark : skipped + " vs " + mark;
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readLimit) {
        // We ignore readLimit since the data is all in-memory and therefore we can reset the mark no matter how far we advance.
        this.mark = offset();
    }
}
