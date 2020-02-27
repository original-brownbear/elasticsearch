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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Objects;

public final class FileBackedBytesReference extends AbstractBytesReference {

    private final long offset;
    private final int length;
    private final IndexInput indexInput;

    public FileBackedBytesReference(IndexInput indexInput, long offset, int length) {
        this.indexInput = indexInput;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public byte get(int index) {
        synchronized (indexInput) {
            try {
                indexInput.seek(offset + index);
                return indexInput.readByte();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        Objects.checkFromIndexSize(from, length, this.length);
        return new FileBackedBytesReference(indexInput.clone(), offset + from, length);
    }

    @Override
    public BytesRef toBytesRef() {
        final byte[] bytes = new byte[length];
        synchronized (indexInput) {
            try {
                indexInput.seek(offset);
                indexInput.readBytes(bytes, 0, length);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new BytesRef(bytes, 0, length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        final byte[] buffer = new byte[1024];
        int bytesLeft = length;
        synchronized (indexInput) {
            while (bytesLeft > 0) {
                int chunkSize = Math.min(bytesLeft, buffer.length);
                indexInput.readBytes(buffer, 0, chunkSize);
                os.write(buffer, 0, chunkSize);
                bytesLeft -= chunkSize;
            }
        }
        os.flush();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

}
