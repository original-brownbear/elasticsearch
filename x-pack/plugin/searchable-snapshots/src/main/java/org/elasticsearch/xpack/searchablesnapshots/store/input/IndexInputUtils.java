/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedIndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Future;

import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.xpack.searchablesnapshots.store.input.ChecksumBlobContainerIndexInput.checksumToBytesArray;

enum IndexInputUtils {
    ;

    /**
     * Detects read operations that are executed on the last 16 bytes of the index input which is where Lucene stores the footer checksum
     * of Lucene files. If such a read is detected this method tries to complete the read operation by reading the checksum from the
     * {@link BlobStoreIndexShardSnapshot.FileInfo} in memory rather than reading the bytes from the {@link BufferedIndexInput} because
     * that could trigger more cache operations.
     *
     * @return true if the footer checksum has been read from the {@link BlobStoreIndexShardSnapshot.FileInfo}
     */
    public static boolean maybeReadChecksumFromFileInfo(
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        long absolutePosition,
        boolean isClone,
        ByteBuffer b
    ) throws IOException {
        final int remaining = b.remaining();
        if (remaining > CodecUtil.footerLength()) {
            return false;
        }
        final long checksumPosition = fileInfo.length() - CodecUtil.footerLength();
        if (absolutePosition < checksumPosition) {
            return false;
        }
        if (isClone) {
            return false;
        }
        boolean success = false;
        try {
            final int checksumOffset = toIntBytes(Math.subtractExact(absolutePosition, checksumPosition));
            assert checksumOffset <= CodecUtil.footerLength() : checksumOffset;
            assert 0 <= checksumOffset : checksumOffset;

            final byte[] checksum = checksumToBytesArray(fileInfo.checksum());
            b.put(checksum, checksumOffset, remaining);
            success = true;
        } catch (NumberFormatException e) {
            // tests disable this optimisation by passing an invalid checksum
        } finally {
            assert b.remaining() == (success ? 0L : remaining) : b.remaining() + " remaining bytes but success is " + success;
        }
        return success;
    }

    static boolean assertFileChannelOpen(FileChannel fileChannel) {
        assert fileChannel != null;
        assert fileChannel.isOpen();
        return true;
    }

    @SuppressForbidden(reason = "Use positional writes on purpose")
    static int positionalWrite(FileChannel fc, long start, ByteBuffer byteBuffer) throws IOException {
        assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        return fc.write(byteBuffer, start);
    }

    static void fillIndexCache(
        SearchableSnapshotDirectory directory,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        IndexInputStats stats,
        CacheFile cacheFile,
        ByteRange indexCacheMiss
    ) {
        final Releasable onCacheFillComplete = stats.addIndexCacheFill();
        final Future<Integer> readFuture = cacheFile.readIfAvailableOrPending(indexCacheMiss, channel -> {
            final int indexCacheMissLength = toIntBytes(indexCacheMiss.length());

            // We assume that we only cache small portions of blobs so that we do not need to:
            // - use a BigArrays for allocation
            // - use an intermediate copy buffer to read the file in sensibly-sized chunks
            // - release the buffer once the indexing operation is complete

            final ByteBuffer byteBuffer = ByteBuffer.allocate(indexCacheMissLength);
            Channels.readFromFileChannelWithEofException(channel, indexCacheMiss.start(), byteBuffer);
            // NB use Channels.readFromFileChannelWithEofException not readCacheFile() to avoid counting this in the stats
            byteBuffer.flip();
            final BytesReference content = BytesReference.fromByteBuffer(byteBuffer);
            directory.putCachedBlob(fileInfo.physicalName(), indexCacheMiss, content, ActionListener.releasing(onCacheFillComplete));
            return indexCacheMissLength;
        });

        if (readFuture == null) {
            // Normally doesn't happen, we're already obtaining a range covering all cache misses above, but theoretically
            // possible in the case that the real populateAndRead call already failed to obtain this range of the file. In that
            // case, simply move on.
            onCacheFillComplete.close();
        }
    }

    public static boolean assertCurrentThreadMayAccessBlobStore() {
        return ThreadPool.assertCurrentThreadPool(
                ThreadPool.Names.SNAPSHOT,
                ThreadPool.Names.GENERIC,
                ThreadPool.Names.SEARCH,
                ThreadPool.Names.SEARCH_THROTTLED,

                // Cache asynchronous fetching runs on a dedicated thread pool.
                SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME,

                // Cache prewarming also runs on a dedicated thread pool.
                SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME
        );
    }
}
