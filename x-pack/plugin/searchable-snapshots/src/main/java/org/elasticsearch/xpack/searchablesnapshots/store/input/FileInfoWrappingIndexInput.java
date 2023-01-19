/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.xpack.searchablesnapshots.store.input.ChecksumBlobContainerIndexInput.checksumToBytesArray;

public final class FileInfoWrappingIndexInput extends IndexInput {

    private final BlobStoreIndexShardSnapshot.FileInfo fileInfo;

    private final BaseSearchableSnapshotIndexInput delegate;

    private byte[] checksumBytes;

    public FileInfoWrappingIndexInput(
        String name,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        BaseSearchableSnapshotIndexInput delegate
    ) {
        super(name);
        this.fileInfo = fileInfo;
        this.delegate = delegate;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public long getFilePointer() {
        return delegate.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
        delegate.seek(pos);
    }

    @Override
    public long length() {
        return delegate.length();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        final BaseSearchableSnapshotIndexInput slicedDelegate = (BaseSearchableSnapshotIndexInput) delegate.slice(
            sliceDescription,
            offset,
            length
        );
        if (length == 0) {
            return slicedDelegate;
        }
        final long position = delegate.getAbsolutePosition();
        final long checksumPosition = fileInfo.length() - CodecUtil.footerLength();
        if (position < checksumPosition) {
            return slicedDelegate;
        }
        final FileInfoWrappingIndexInput slice = new FileInfoWrappingIndexInput(sliceDescription, fileInfo, slicedDelegate);
        slice.checksumBytes = checksumBytes;
        return slice;
    }

    @Override
    public IndexInput clone() {
        BaseSearchableSnapshotIndexInput clone = delegate.clone();
        clone.isClone = true;
        return clone;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
        delegate.readBytes(b, offset, len, useBuffer);
    }

    @Override
    public short readShort() throws IOException {
        return delegate.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return delegate.readInt();
    }

    @Override
    public int readVInt() throws IOException {
        return delegate.readVInt();
    }

    @Override
    public int readZInt() throws IOException {
        return delegate.readZInt();
    }

    @Override
    public long readLong() throws IOException {
        return delegate.readLong();
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        delegate.readLongs(dst, offset, length);
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        delegate.readInts(dst, offset, length);
    }

    @Override
    public void readFloats(float[] floats, int offset, int len) throws IOException {
        delegate.readFloats(floats, offset, len);
    }

    @Override
    public long readVLong() throws IOException {
        return delegate.readVLong();
    }

    @Override
    public long readZLong() throws IOException {
        return delegate.readZLong();
    }

    @Override
    public String readString() throws IOException {
        return delegate.readString();
    }

    @Override
    public Map<String, String> readMapOfStrings() throws IOException {
        return delegate.readMapOfStrings();
    }

    @Override
    public Set<String> readSetOfStrings() throws IOException {
        return delegate.readSetOfStrings();
    }

    @Override
    public byte readByte() throws IOException {
        final long position = delegate.getAbsolutePosition();
        final long checksumPosition = fileInfo.length() - CodecUtil.footerLength();
        if (position < checksumPosition) {
            return delegate.readByte();
        }
        final byte[] b = new byte[1];
        readBytes(b, 0, 1);
        return b[0];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (maybeReadChecksumFromFileInfo(b, offset, len)) {
            delegate.seek(delegate.getFilePointer() + len);
            return;
        }
        delegate.readBytes(b, offset, len);
    }

    /**
     * Detects read operations that are executed on the last 16 bytes of the index input which is where Lucene stores the footer checksum
     * of Lucene files. If such a read is detected this method tries to complete the read operation by reading the checksum from the
     * {@link BlobStoreIndexShardSnapshot.FileInfo} in memory rather than reading the bytes from the {@link BufferedIndexInput} because
     * that could trigger more cache operations.
     *
     * @return true if the footer checksum has been read from the {@link BlobStoreIndexShardSnapshot.FileInfo}
     */
    private boolean maybeReadChecksumFromFileInfo(byte[] b, int offset, int len) throws IOException {
        if (len > CodecUtil.footerLength()) {
            return false;
        }
        final long position = delegate.getAbsolutePosition();
        final long checksumPosition = fileInfo.length() - CodecUtil.footerLength();
        if (position < checksumPosition) {
            return false;
        }
        boolean success = false;
        try {
            final int checksumOffset = toIntBytes(Math.subtractExact(position, checksumPosition));
            assert checksumOffset <= CodecUtil.footerLength() : checksumOffset;
            assert 0 <= checksumOffset : checksumOffset;

            byte[] checksumBytes = this.checksumBytes;
            if (checksumBytes == null) {
                checksumBytes = checksumToBytesArray(fileInfo.checksum());
                this.checksumBytes = checksumBytes;
            }
            System.arraycopy(checksumBytes, checksumOffset, b, offset, len);
            success = true;
        } catch (NumberFormatException e) {
            // tests disable this optimisation by passing an invalid checksum
        }
        return success;
    }
}
