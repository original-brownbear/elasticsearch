/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class BufferedStreamInput extends StreamInput {

    @Nullable
    protected byte[] buf;

    protected int count;

    protected int pos;

    protected int markpos;

    protected int marklimit;

    public static BufferedStreamInput wrap(byte[] buf, InputStream input) throws IOException {
        final int length = Streams.readFully(input, buf, 0, buf.length);
        if (length < buf.length) {
            input.close();
            return wrap(buf, 0, length);
        }
        return new BufferedStreamInput(buf, 0, buf.length, -1) {

            @Nullable
            private InputStream in = input;

            @Override
            public int read() throws IOException {
                if (pos >= count) {
                    fill();
                    if (pos >= count)
                        return -1;
                }
                return getBufIfOpen()[pos++] & 0xff;
            }

            @Override
            public int read(byte b[], int off, int len) throws IOException {
                getBufIfOpen(); // Check for closed stream
                Objects.checkFromIndexSize(off, len, b.length);

                int n = 0;
                for (; ; ) {
                    int nread = read1(b, off + n, len - n);
                    if (nread <= 0)
                        return (n == 0) ? nread : n;
                    n += nread;
                    if (n >= len)
                        return n;
                    // if not closed but no bytes available, return
                    InputStream input = in;
                    if (input != null && input.available() <= 0)
                        return n;
                }
            }

            @Override
            public byte readByte() throws IOException {
                if (available() < 1) {
                    fill();
                    if (available() < 1)
                        throw new EOFException();
                }
                return getBufIfOpen()[pos++];
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                if (offset < 0) {
                    throw new IndexOutOfBoundsException();
                }
                final int read = Streams.readFully(this, b, offset, len);
                if (read != len) {
                    throw new EOFException();
                }
            }

            private byte[] getBufIfOpen() throws IOException {
                byte[] buffer = this.buf;
                if (buffer == null)
                    throw new IOException("Stream closed");
                return buffer;
            }

            /**
             * Read characters into a portion of an array, reading from the underlying
             * stream at most once if necessary.
             */
            private int read1(byte[] b, int off, int len) throws IOException {
                int avail = available();
                if (avail <= 0) {
                    if (in == null) {
                        return -1;
                    }
                    /* If the requested length is at least as large as the buffer, and
                       if there is no mark/reset activity, do not bother to copy the
                       bytes into the local buffer.  In this way buffered streams will
                       cascade harmlessly. */
                    if (len >= getBufIfOpen().length && markpos < 0) {
                        return in.read(b, off, len);
                    }
                    fill();
                    avail = count - pos;
                    if (avail <= 0) return -1;
                }
                int cnt = Math.min(avail, len);
                System.arraycopy(getBufIfOpen(), pos, b, off, cnt);
                pos += cnt;
                return cnt;
            }

            private void fill() throws IOException {
                if (in == null) {
                    // nothing to do don't have a stream to read from any longer
                    return;
                }
                byte[] buffer = getBufIfOpen();
                if (markpos < 0)
                    pos = 0;            /* no mark: throw away the buffer */
                else if (pos >= buffer.length) { /* no room left in buffer */
                    if (markpos > 0) {  /* can throw away early part of the buffer */
                        int sz = pos - markpos;
                        System.arraycopy(buffer, markpos, buffer, 0, sz);
                        pos = sz;
                        markpos = 0;
                    } else if (buffer.length >= marklimit) {
                        markpos = -1;   /* buffer got too big, invalidate mark */
                        pos = 0;        /* drop buffer contents */
                    } else {            /* grow buffer */
                        int nsz = ArrayUtil.oversize(pos * 2, 1);
                        if (nsz > marklimit)
                            nsz = marklimit;
                        byte[] nbuf = new byte[nsz];
                        System.arraycopy(buffer, 0, nbuf, 0, pos);
                        if (buf == null) {
                            throw new IOException("Stream closed");
                        } else {
                            buf = nbuf;
                        }
                        buffer = nbuf;
                    }
                }
                count = pos;
                final int toRead = buffer.length - pos;
                int n = Streams.readFully(in, buffer, pos, toRead);
                if (n > 0) {
                    count = n + pos;
                }
                if (n < toRead) {
                    in.close();
                    in = null;
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    IOUtils.close(in);
                } finally {
                    buf = null;
                    in = null;
                }
            }

            private InputStream getInIfOpen() throws IOException {
                InputStream input = in;
                if (input == null)
                    throw new IOException("Stream closed");
                return input;
            }

            @Override
            public void reset() throws IOException {
                getBufIfOpen(); // Cause exception if closed
                if (markpos < 0)
                    throw new IOException("Resetting to invalid mark");
                pos = markpos;
            }

            @Override
            protected void ensureCanReadBytes(int length) throws EOFException {
                if (in == null && available() < length) {
                    throw new EOFException("tried to read: " + length + " bytes but only " + available() + " remaining");
                }
            }

            @Override
            public long skip(long n) throws IOException {
                getBufIfOpen(); // Check for closed stream
                if (n <= 0) {
                    return 0;
                }
                long avail = count - pos;

                if (avail <= 0) {
                    // If no mark position set then don't keep in buffer
                    if (markpos < 0)
                        return getInIfOpen().skip(n);

                    // Fill in buffer to save bytes for reset
                    fill();
                    avail = count - pos;
                    if (avail <= 0)
                        return 0;
                }

                long skipped = Math.min(avail, n);
                pos += skipped;
                return skipped;
            }

            @Override
            public String readString() throws IOException {
                final int charCount = readArraySize();
                final CharsRef charsRef = charsRef(charCount);
                if (available() < charCount * 3) {
                    return super.readStringSlow(charsRef);
                }
                return readStringFast(charsRef);
            }
        };
    }

    public static BufferedStreamInput wrap(byte[] buf, int offset, int length) {
        return new BufferedStreamInput(buf, offset, length, offset) {

            @Override
            public int read() {
                if (pos >= count)
                    return -1;
                return buf[pos++] & 0xff;
            }

            @Override
            public int read(byte b[], int off, int len) {
                Objects.checkFromIndexSize(off, len, b.length);

                if (pos >= count) {
                    return -1;
                }

                int avail = count - pos;
                if (len > avail) {
                    len = avail;
                }
                if (len <= 0) {
                    return 0;
                }
                System.arraycopy(buf, pos, b, off, len);
                pos += len;
                return len;
            }

            @Override
            public byte readByte() throws IOException {
                if (available() < 1) {
                    throw new EOFException();
                }
                return buf[pos++];
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                Objects.checkFromIndexSize(offset, len, b.length);
                if (count - pos < len) {
                    throw new EOFException();
                }
                System.arraycopy(buf, pos, b, offset, len);
                pos += len;
            }

            @Override
            public void close() {
                // nothing to do
            }

            @Override
            public void reset() throws IOException {
                if (markpos < 0)
                    throw new IOException("Resetting to invalid mark");
                pos = markpos;
            }

            @Override
            protected void ensureCanReadBytes(int length) throws EOFException {
                if (available() < length) {
                    throw new EOFException("tried to read: " + length + " bytes but only " + available() + " remaining");
                }
            }

            @Override
            public long skip(long n) {
                if (n <= 0) {
                    return 0;
                }
                long skipped = Math.min(available(), n);
                pos += skipped;
                return skipped;
            }

            @Override
            public String readString() throws IOException {
                return readStringFast(charsRef(readArraySize()));
            }
        };
    }

    protected String readStringFast(CharsRef charsRef) throws IOException {
        pos = BufferedStreamInput.readCharsUnsafe(buf, charsRef.chars, pos, charsRef.length);
        return charsRef.toString();
    }

    public static int readCharsUnsafe(byte[] byteBuffer, char[] charBuffer, int offsetByteArray, int charCount) throws IOException {
        int charsOffset = 0;
        while (charsOffset < charCount) {
            final int c = byteBuffer[offsetByteArray++] & 0xff;
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
                    charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[offsetByteArray++] & 0x3F);
                    break;
                case 14:
                    charBuffer[charsOffset++] = (char) ((c & 0x0F) << 12 |
                            (byteBuffer[offsetByteArray++] & 0x3F) << 6 | (byteBuffer[offsetByteArray++] & 0x3F));
                    break;
                default:
                    throwOnBrokenChar(c);
            }
        }
        return offsetByteArray;
    }

    private BufferedStreamInput(byte[] buf, int offset, int length, int markpos) {
        this.buf = buf;
        this.pos = offset;
        this.count = offset + length;
        this.markpos = markpos;
    }

    @Override
    public short readShort() throws IOException {
        if (available() > 1) {
            final int position = pos;
            pos += 2;
            return (short) (((buf[position] & 0xFF) << 8) | (buf[position + 1] & 0xFF));
        }
        return super.readShort();
    }

    @Override
    public int readInt() throws IOException {
        if (available() > 3) {
            final int position = pos;
            pos += 4;
            return intFromBytes(position, buf);
        }
        return super.readInt();
    }

    @Override
    public int readVInt() throws IOException {
        if (available() > 4) {
            byte b = buf[pos++];
            int i = b & 0x7F;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7F) << 7;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7F) << 14;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7F) << 21;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            if ((b & 0x80) != 0) {
                throw new IOException("Invalid vInt ((" + Integer.toHexString(b) + " & 0x7f) << 28) | " + Integer.toHexString(i));
            }
            return i | ((b & 0x7F) << 28);
        }
        return readVIntSlow(this);
    }

    @Override
    public long readLong() throws IOException {
        if (available() > 7) {
            final int position = pos;
            pos += 8;
            return longFromBytes(position, buf);
        }
        return super.readLong();
    }

    public static long longFromBytes(int position, byte[] buf) {
        return (((long) (((buf[position] & 0xFF) << 24) | ((buf[position + 1] & 0xFF) << 16) | ((buf[position + 2] & 0xFF) << 8)
                | (buf[position + 3] & 0xFF))) << 32)
                | ((((buf[position + 4] & 0xFF) << 24) | ((buf[position + 5] & 0xFF) << 16) | ((buf[position + 6] & 0xFF) << 8)
                | (buf[position + 7] & 0xFF)) & 0xFFFFFFFFL);
    }

    public static int intFromBytes(int position, byte[] buf) {
        return ((buf[position] & 0xFF) << 24) | ((buf[position + 1] & 0xFF) << 16)
                | ((buf[position + 2] & 0xFF) << 8) | (buf[position + 3] & 0xFF);
    }

    @Override
    public long readVLong() throws IOException {
        if (available() > 9) {
            byte b = buf[pos++];
            long i = b & 0x7FL;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7FL) << 7;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7FL) << 14;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7FL) << 21;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7FL) << 28;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7FL) << 35;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7FL) << 42;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= (b & 0x7FL) << 49;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
            i |= ((b & 0x7FL) << 56);
            if ((b & 0x80) == 0) {
                return i;
            }
            b = buf[pos++];
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
        if (available() > 9) {
            long accumulator = 0L;
            int i = 0;
            long currentByte;
            while (((currentByte = buf[pos++]) & 0x80L) != 0) {
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
    public int available() {
        return count - pos;
    }

    @Override
    public void mark(int readlimit) {
        marklimit = readlimit;
        markpos = pos;
    }

    @Override
    public boolean markSupported() {
        return true;
    }
}
