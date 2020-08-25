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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;

import java.io.EOFException;
import java.io.IOException;

public abstract class BufferedStreamInput extends StreamInput {

    protected byte[] currentBuffer;

    protected int pos;

    protected int limit;

    @Override
    public byte readByte() throws IOException {
        bufferAtLeast(1);
        return currentBuffer[pos++];
    }

    @Override
    public int read() throws IOException {
        if (tryBuffer(1) == 0) {
            return -1;
        }
        return currentBuffer[pos++] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int remaining = len;
        while (remaining > 0) {
            if (tryBuffer(remaining) == 0) {
                return len == remaining ? -1 : len - remaining;
            }
            final int toRead = Math.min(limit - pos, remaining);
            System.arraycopy(currentBuffer, pos, b, off, toRead);
            off += toRead;
            pos += toRead;
            remaining -= toRead;
        }
        return len;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
        int remaining = len;
        while (remaining > 0) {
            if (tryBuffer(remaining) == 0) {
                throw new IndexOutOfBoundsException();
            }
            final int toRead = Math.min(limit - pos, remaining);
            System.arraycopy(currentBuffer, pos, b, offset, toRead);
            offset += toRead;
            pos += toRead;
            remaining -= toRead;
        }
    }

    @Override
    public BytesReference readBytesReference() throws IOException {
        return super.readBytesReference();
    }

    @Override
    public short readShort() throws IOException {
        bufferAtLeast(2);
        pos += 2;
        return (short) (((currentBuffer[pos - 2] & 0xFF) << 8) | (currentBuffer[pos - 1] & 0xFF));
    }

    @Override
    public int readInt() throws IOException {
        bufferAtLeast(4);
        pos += 4;
        return ((currentBuffer[pos - 4] & 0xFF) << 24) | ((currentBuffer[pos - 3] & 0xFF) << 16)
                | ((currentBuffer[pos - 2] & 0xFF) << 8) | (currentBuffer[pos - 1] & 0xFF);
    }

    @Override
    public int readVInt() throws IOException {
        if (tryBuffer(5) < 5) {
            return super.readVInt();
        }
        byte b = currentBuffer[pos++];
        int i = b & 0x7F;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7F) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7F) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7F) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        if ((b & 0x80) != 0) {
            throw new IOException("Invalid vInt ((" + Integer.toHexString(b) + " & 0x7f) << 28) | " + Integer.toHexString(i));
        }
        return i | ((b & 0x7F) << 28);
    }

    @Override
    public long readLong() throws IOException {
        if (tryBuffer(8) < 8) {
            return super.readLong();
        }
        final int position = pos;
        pos += 8;
        return (((long) (((currentBuffer[position] & 0xFF) << 24) | ((currentBuffer[position + 1] & 0xFF) << 16) |
                ((currentBuffer[position + 2] & 0xFF) << 8) | (currentBuffer[position + 3] & 0xFF))) << 32)
                | ((((currentBuffer[position + 4] & 0xFF) << 24) | ((currentBuffer[position + 5] & 0xFF) << 16) |
                ((currentBuffer[position + 6] & 0xFF) << 8) | (currentBuffer[position + 7] & 0xFF)) & 0xFFFFFFFFL);
    }

    @Override
    public long readVLong() throws IOException {
        if (tryBuffer(10) < 10) {
            return super.readVLong();
        }
        byte b = currentBuffer[pos++];
        long i = b & 0x7FL;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7FL) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7FL) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7FL) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7FL) << 28;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7FL) << 35;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7FL) << 42;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= (b & 0x7FL) << 49;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        i |= ((b & 0x7FL) << 56);
        if ((b & 0x80) == 0) {
            return i;
        }
        b = currentBuffer[pos++];
        if (b != 0 && b != 1) {
            throw new IOException("Invalid vlong (" + Integer.toHexString(b) + " << 63) | " + Long.toHexString(i));
        }
        i |= ((long) b) << 63;
        return i;
    }

    @Override
    public long readZLong() throws IOException {
        if (tryBuffer(10) < 10) {
            return super.readZLong();
        }
        long accumulator = 0L;
        int i = 0;
        long currentByte;
        while (((currentByte = currentBuffer[pos++]) & 0x80L) != 0) {
            accumulator |= (currentByte & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IOException("variable-length stream is too long");
            }
        }
        return BitUtil.zigZagDecode(accumulator | (currentByte << i));
    }

    @Override
    public Text readText() throws IOException {
        return super.readText();
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
        int missingFromPartial = 0;
        final byte[] byteBuffer = currentBuffer;
        final char[] charBuffer = charsRef.chars;
        for (; charsOffset < charCount; ) {
            final int charsLeft = charCount - charsOffset;
            // Determine the minimum amount of bytes that are left in the string
            final int minRemainingBytes;
            if (missingFromPartial > 0) {
                // One byte for each remaining char except for the already partially read char
                minRemainingBytes = missingFromPartial + charsLeft - 1;
                missingFromPartial = 0;
            } else {
                // Each char has at least a single byte
                minRemainingBytes = charsLeft;
            }
            final int toRead;
            toRead = minRemainingBytes;
            if (tryBuffer(toRead) == 0) {
                throw new EOFException();
            }
            int offsetByteArray = pos;
            int sizeByteArray = limit;
            // As long as we at least have three bytes buffered we don't need to do any bounds checking when getting the next char since we
            // read 3 bytes per char/iteration at most
            for (; offsetByteArray < sizeByteArray - 2 && charsOffset < charCount; offsetByteArray++) {
                final int c = byteBuffer[offsetByteArray] & 0xff;
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
                        charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[++offsetByteArray] & 0x3F);
                        break;
                    case 14:
                        charBuffer[charsOffset++] = (char) (
                                (c & 0x0F) << 12 | (byteBuffer[++offsetByteArray] & 0x3F) << 6 | (byteBuffer[++offsetByteArray] & 0x3F));
                        break;
                    default:
                        throwOnBrokenChar(c);
                }
            }
            pos = offsetByteArray;
            // try to extract chars from remaining bytes with bounds checks for multi-byte chars
            final int bufferedBytesRemaining = sizeByteArray - offsetByteArray;
            for (int i = 0; i < bufferedBytesRemaining && charsOffset < charCount; i++) {
                final int c = byteBuffer[offsetByteArray] & 0xff;
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
                        offsetByteArray++;
                        break;
                    case 12:
                    case 13:
                        missingFromPartial = 2 - (bufferedBytesRemaining - i);
                        if (missingFromPartial == 0) {
                            offsetByteArray++;
                            charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[offsetByteArray++] & 0x3F);
                        }
                        ++i;
                        break;
                    case 14:
                        missingFromPartial = 3 - (bufferedBytesRemaining - i);
                        ++i;
                        break;
                    default:
                        throwOnBrokenChar(c);
                }
            }
            pos = offsetByteArray;
        }
        return charsRef.toString();
    }

    @Override
    public int available() {
        return limit - pos;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        bufferAtLeast(length);
    }

    @Override
    public long skip(long n) {
        final int skip = (int) Math.min(Integer.MAX_VALUE, n);
        final int numBytesSkipped =  Math.min(skip, limit - pos);
        pos += numBytesSkipped;
        return numBytesSkipped;
    }

    @Override
    public void close() {
        // nothing by default
    }

    protected void bufferAtLeast(int minBufferSize) throws EOFException {
        if (doBufferAtLeast(minBufferSize) == false) {
            throw new EOFException();
        }
    }

    protected abstract boolean doBufferAtLeast(int minBufferSize);

    protected int tryBuffer(int size) {
        doTryBuffer(size);
        return limit - pos;
    }

    protected abstract void doTryBuffer(int size);
}
