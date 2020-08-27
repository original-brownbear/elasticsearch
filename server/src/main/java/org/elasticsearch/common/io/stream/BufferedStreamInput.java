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
import org.elasticsearch.common.io.Streams;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * TODO: null out inputstream when fully read
 * TODO: make BytesArray stream a special case
 *
 */
public final class BufferedStreamInput extends StreamInput {

    private byte[] buf;

    protected int count;

    protected int pos;

    protected int markpos = -1;

    protected int marklimit;

    private InputStream in;

    public BufferedStreamInput(byte[] buf, InputStream in) {
        this.buf = buf;
        this.in = in;
    }

    @Override
    public byte readByte() throws IOException {
        if (pos >= count) {
            fill();
            if (pos >= count)
                throw new EOFException();
        }
        return getBufIfOpen()[pos++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        final int read = Streams.readFully(this, b, offset, len);
        if (read != len) {
            throw new EOFException();
        }
    }

    @Override
    public int read() throws IOException {
        if (pos >= count) {
            fill();
            if (pos >= count)
                return -1;
        }
        return getBufIfOpen()[pos++] & 0xff;
    }

    /**
     * Read characters into a portion of an array, reading from the underlying
     * stream at most once if necessary.
     */
    private int read1(byte[] b, int off, int len) throws IOException {
        int avail = count - pos;
        if (avail <= 0) {
            /* If the requested length is at least as large as the buffer, and
               if there is no mark/reset activity, do not bother to copy the
               bytes into the local buffer.  In this way buffered streams will
               cascade harmlessly. */
            if (len >= getBufIfOpen().length && markpos < 0) {
                return getInIfOpen().read(b, off, len);
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

    @Override
    public synchronized int read(byte b[], int off, int len) throws IOException {
        getBufIfOpen(); // Check for closed stream
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

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
    public int available() throws IOException {
        int n = count - pos;
        int avail = getInIfOpen().available();
        return n > (Integer.MAX_VALUE - avail)
            ? Integer.MAX_VALUE
            : n + avail;
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        // noop
    }

    @Override
    public void close() throws IOException {
        buf = null;
    }

    /**
     * Check to make sure that buffer has not been nulled out due to
     * close; if not return it;
     */
    private byte[] getBufIfOpen() throws IOException {
        byte[] buffer = this.buf;
        if (buffer == null)
            throw new IOException("Stream closed");
        return buffer;
    }

    /**
     * Fills the buffer with more data, taking into account
     * shuffling and other tricks for dealing with marks.
     * Assumes that it is being called by a synchronized method.
     * This method also assumes that all data has already been read in,
     * hence pos > count.
     */
    private void fill() throws IOException {
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
                int nsz = ArrayUtil.oversize(marklimit, 1);
                if (nsz > marklimit)
                    nsz = marklimit;
                byte[] nbuf = new byte[nsz];
                System.arraycopy(buffer, 0, nbuf, 0, pos);
                if (buf == null) {
                    // Can't replace buf if there was an async close.
                    // Note: This would need to be changed if fill()
                    // is ever made accessible to multiple threads.
                    // But for now, the only way CAS can fail is via close.
                    // assert buf == null;
                    throw new IOException("Stream closed");
                } else {
                    buf = nbuf;
                }
                buffer = nbuf;
            }
        }
        count = pos;
        int n = getInIfOpen().read(buffer, pos, buffer.length - pos);
        if (n > 0)
            count = n + pos;
    }

    private InputStream getInIfOpen() throws IOException {
        InputStream input = in;
        if (input == null)
            throw new IOException("Stream closed");
        return input;
    }
}
