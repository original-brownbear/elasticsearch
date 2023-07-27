/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteBufferReference;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.CachedBlob;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

import static org.elasticsearch.blobcache.BlobCacheUtils.throwEOF;
import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.core.Strings.format;

public final class FrozenIndexInput extends BufferedIndexInput {

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );
    private static final Logger logger = LogManager.getLogger(FrozenIndexInput.class);
    private final CacheFileReference cacheFileReference;
    private final SearchableSnapshotDirectory directory;
    private final int defaultRangeSize;
    private final int recoveryRangeSize;
    private final FileInfo fileInfo;
    private final IOContext context;
    private final IndexInputStats stats;
    private final long offset;

    private final SharedBlobCacheService<CacheKey>.CacheFile cacheFile;
    /**
     * If > 0, represents a logical file within a compound (CFS) file or is a slice thereof represents the offset of the logical
     * compound file within the physical CFS file
     */
    private final long compoundFileOffset;
    /**
     * Range of bytes that should be cached in the blob cache for the current index input's header.
     */
    private final ByteRange headerBlobCacheByteRange;
    /**
     * Range of bytes that should be cached in the blob cache for the current index input's footer. This footer byte range should only be
     * required for slices of CFS files; regular files already have their footers extracted from the
     * {@link FileInfo} (see method {@link IndexInputUtils#maybeReadChecksumFromFileInfo}).
     */
    private final ByteRange footerBlobCacheByteRange;
    private final boolean isCfs;
    private final long length;
    // last read position is kept around in order to detect (non)contiguous reads for stats
    private long lastReadPosition;
    // last seek position is kept around in order to detect forward/backward seeks for stats
    private long lastSeekPosition;
    // the following are only mutable so they can be adjusted after cloning/slicing
    private volatile boolean isClone;
    private AtomicBoolean closed;

    public FrozenIndexInput(
        String name,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        int rangeSize,
        int recoveryRangeSize
    ) {
        this(
            name,
            directory,
            fileInfo,
            context,
            stats,
            0L,
            0L,
            fileInfo.length(),
            new CacheFileReference(directory, fileInfo.physicalName(), fileInfo.length()),
            directory.getFrozenCacheFile(name, fileInfo.length()),
            rangeSize,
            recoveryRangeSize,
            directory.getBlobCacheByteRange(name, fileInfo.length()),
            ByteRange.EMPTY
        );
        stats.incrementOpenCount();
    }

    private FrozenIndexInput(
        String name,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long compoundFileOffset,
        long length,
        CacheFileReference cacheFileReference,
        SharedBlobCacheService<CacheKey>.CacheFile cacheFile,
        int defaultRangeSize,
        int recoveryRangeSize,
        ByteRange headerBlobCacheByteRange,
        ByteRange footerBlobCacheByteRange
    ) {
        super(name, context);
        this.isCfs = IndexFileNames.matchesExtension(name, "cfs");
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.context = Objects.requireNonNull(context);
        assert fileInfo.metadata().hashEqualsContents() == false
            : "this method should only be used with blobs that are NOT stored in metadata's hash field " + "(fileInfo: " + fileInfo + ')';
        this.stats = Objects.requireNonNull(stats);
        this.offset = offset;
        this.length = length;
        this.closed = new AtomicBoolean(false);
        this.isClone = false;
        this.directory = Objects.requireNonNull(directory);
        this.cacheFileReference = cacheFileReference;
        this.compoundFileOffset = compoundFileOffset;
        this.defaultRangeSize = defaultRangeSize;
        this.recoveryRangeSize = recoveryRangeSize;
        this.lastReadPosition = offset;
        this.lastSeekPosition = offset;
        this.headerBlobCacheByteRange = Objects.requireNonNull(headerBlobCacheByteRange);
        this.footerBlobCacheByteRange = Objects.requireNonNull(footerBlobCacheByteRange);
        assert offset >= compoundFileOffset;
        assert getBufferSize() <= BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE; // must be able to cache at least one buffer's worth
        this.cacheFile = cacheFile.copy();
    }

    public static boolean assertCurrentThreadIsNotCacheFetchAsync() {
        final String threadName = Thread.currentThread().getName();
        assert false == threadName.contains('[' + SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME + ']')
            : "expected the current thread [" + threadName + "] to belong to the cache fetch async thread pool";
        return true;
    }

    private void readWithoutBlobCache(ByteBuffer b) throws Exception {
        final long position = getAbsolutePosition();
        final int length = b.remaining();
        if (cacheFile.tryRead(b, position)) {
            // fast-path succeeded, increment stats and return
            stats.addCachedBytesRead(length);
            return;
        }
        readWithoutBlobCacheSlow(b, position, length);
    }

    // slow path for readWithoutBlobCache, extracted to a separate method to make the fast-path inline better
    private void readWithoutBlobCacheSlow(ByteBuffer b, long position, int length) throws Exception {
        // Semaphore that, when all permits are acquired, ensures that async callbacks (such as those used by readCacheFile) are not
        // accessing the byte buffer anymore that was passed to readWithoutBlobCache
        // In particular, it's important to acquire all permits before adapting the ByteBuffer's offset
        final ByteBufferReference byteBufferReference = new ByteBufferReference(b);
        logger.trace("readInternal: read [{}-{}] from [{}]", position, position + length, this);
        try {
            final ByteRange rangeToWrite = BlobCacheUtils.computeRange(
                directory.isRecoveryFinalized() ? defaultRangeSize : recoveryRangeSize,
                position,
                length,
                fileInfo.length()
            );
            assert rangeToWrite.start() <= position && position + length <= rangeToWrite.end()
                : "[" + position + "-" + (position + length) + "] vs " + rangeToWrite;
            final ByteRange rangeToRead = ByteRange.of(position, position + length);

            final int bytesRead = cacheFile.populateAndRead(rangeToWrite, rangeToRead, (channel, pos, relativePos, len) -> {
                logger.trace(
                    "{}: reading logical {} channel {} pos {} length {} (details: {})",
                    fileInfo.physicalName(),
                    rangeToRead.start(),
                    pos,
                    relativePos,
                    length,
                    cacheFile
                );
                final int read = SharedBytes.readCacheFile(channel, pos, relativePos, len, byteBufferReference, cacheFile);
                stats.addCachedBytesRead(read);
                return read;
            }, (channel, channelPos, relativePos, len, progressUpdater) -> {
                final long startTimeNanos = stats.currentTimeNanos();
                try (InputStream input = openInputStreamFromBlobStore(rangeToWrite.start() + relativePos, len)) {
                    assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
                    logger.trace(
                        "{}: writing channel {} pos {} length {} (details: {})",
                        fileInfo.physicalName(),
                        channelPos,
                        relativePos,
                        len,
                        cacheFile
                    );
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        input,
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.get().clear(),
                        cacheFile
                    );
                    final long endTimeNanos = stats.currentTimeNanos();
                    stats.addCachedBytesWritten(len, endTimeNanos - startTimeNanos);
                }
            });
            assert bytesRead == length : bytesRead + " vs " + length;
            byteBufferReference.finish(bytesRead);
        } finally {
            byteBufferReference.finish(0);
        }
    }

    private long getAbsolutePosition() {
        final long position = getFilePointer() + this.offset;
        assert position >= 0L : "absolute position is negative: " + position;
        assert position <= fileInfo.length() : position + " vs " + fileInfo.length();
        return position;
    }

    private ByteRange rangeToReadFromBlobCache(long position, int readLength) {
        final long end = position + readLength;
        if (headerBlobCacheByteRange.contains(position, end)) {
            return headerBlobCacheByteRange;
        } else if (footerBlobCacheByteRange.contains(position, end)) {
            return footerBlobCacheByteRange;
        }
        return ByteRange.EMPTY;
    }

    private void readWithBlobCache(ByteBuffer b, ByteRange blobCacheByteRange) throws Exception {
        final long position = getAbsolutePosition();
        final int length = b.remaining();

        final CacheFile cacheFile = cacheFileReference.get();

        // Can we serve the read directly from disk? If so, do so and don't worry about anything else.
        final Future<Integer> waitingForRead = cacheFile.readIfAvailableOrPending(ByteRange.of(position, position + length), chan -> {
            final int read = readCacheFile(chan, position, b);
            assert read == length : read + " vs " + length;
            return read;
        });

        if (waitingForRead != null) {
            final Integer read = waitingForRead.get();
            assert read == length;
            return;
        }

        final CachedBlob cachedBlob = directory.getCachedBlob(fileInfo.physicalName(), blobCacheByteRange);
        assert cachedBlob == CachedBlob.CACHE_MISS || cachedBlob == CachedBlob.CACHE_NOT_READY || cachedBlob.from() <= position;
        assert cachedBlob == CachedBlob.CACHE_MISS || cachedBlob == CachedBlob.CACHE_NOT_READY || length <= cachedBlob.length();

        if (cachedBlob == CachedBlob.CACHE_MISS || cachedBlob == CachedBlob.CACHE_NOT_READY) {
            // We would have liked to find a cached entry but we did not find anything: the cache on the disk will be requested
            // so we compute the region of the file we would like to have the next time. The region is expressed as a tuple of
            // {start, end} where positions are relative to the whole file.

            // We must fill in a cache miss even if CACHE_NOT_READY since the cache index is only created on the first put.
            // TODO TBD use a different trigger for creating the cache index and avoid a put in the CACHE_NOT_READY case.
            final Future<Integer> populateCacheFuture = populateAndRead(b, position, length, cacheFile, blobCacheByteRange);

            IndexInputUtils.fillIndexCache(directory, fileInfo, stats, cacheFile, blobCacheByteRange);
            if (compoundFileOffset > 0L
                && blobCacheByteRange.equals(headerBlobCacheByteRange)
                && footerBlobCacheByteRange.isEmpty() == false) {
                IndexInputUtils.fillIndexCache(directory, fileInfo, stats, cacheFile, footerBlobCacheByteRange);
            }

            final int bytesRead = populateCacheFuture.get();
            assert bytesRead == length : bytesRead + " vs " + length;
        } else {
            final int sliceOffset = toIntBytes(position - cachedBlob.from());
            assert sliceOffset + length <= cachedBlob.to()
                : "reading " + length + " bytes from " + sliceOffset + " exceed cached blob max position " + cachedBlob.to();

            logger.trace("reading [{}] bytes of file [{}] at position [{}] using cache index", length, fileInfo.physicalName(), position);
            final BytesRefIterator cachedBytesIterator = cachedBlob.bytes().slice(sliceOffset, length).iterator();
            BytesRef bytesRef;
            int copiedBytes = 0;
            while ((bytesRef = cachedBytesIterator.next()) != null) {
                b.put(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                copiedBytes += bytesRef.length;
            }
            assert copiedBytes == length : "copied " + copiedBytes + " but expected " + length;
            stats.addIndexCacheBytesRead(cachedBlob.length());

            try {
                final ByteRange cachedRange = ByteRange.of(cachedBlob.from(), cachedBlob.to());
                cacheFile.populateAndRead(
                    cachedRange,
                    cachedRange,
                    channel -> cachedBlob.length(),
                    (channel, from, to, progressUpdater) -> {
                        final long startTimeNanos = stats.currentTimeNanos();
                        final BytesRefIterator iterator = cachedBlob.bytes()
                            .slice(toIntBytes(from - cachedBlob.from()), toIntBytes(to - from))
                            .iterator();
                        long writePosition = from;
                        BytesRef current;
                        while ((current = iterator.next()) != null) {
                            final ByteBuffer byteBuffer = ByteBuffer.wrap(current.bytes, current.offset, current.length);
                            while (byteBuffer.remaining() > 0) {
                                writePosition += IndexInputUtils.positionalWrite(channel, writePosition, byteBuffer);
                                progressUpdater.accept(writePosition);
                            }
                        }
                        assert writePosition == to : writePosition + " vs " + to;
                        final long endTimeNanos = stats.currentTimeNanos();
                        stats.addCachedBytesWritten(to - from, endTimeNanos - startTimeNanos);
                        logger.trace("copied bytes [{}-{}] of file [{}] from cache index to disk", from, to, fileInfo);
                    },
                    directory.cacheFetchAsyncExecutor()
                );
            } catch (Exception e) {
                logger.debug(
                    () -> format(
                        "failed to store bytes [%s-%s] of file [%s] obtained from index cache",
                        cachedBlob.from(),
                        cachedBlob.to(),
                        fileInfo
                    ),
                    e
                );
                // oh well, no big deal, at least we can return them to the caller.
            }
        }
    }

    private Future<Integer> populateAndRead(ByteBuffer b, long position, int length, CacheFile cacheFile, ByteRange rangeToWrite) {
        final ByteRange rangeToRead = ByteRange.of(position, position + length);
        assert rangeToRead.isSubRangeOf(rangeToWrite) : rangeToRead + " vs " + rangeToWrite;
        assert rangeToRead.length() == b.remaining() : b.remaining() + " vs " + rangeToRead;

        return cacheFile.populateAndRead(
            rangeToWrite,
            rangeToRead,
            channel -> readCacheFile(channel, position, b),
            this::writeCacheFile,
            directory.cacheFetchAsyncExecutor()
        );
    }

    private void readComplete(long position, int length) {
        stats.incrementBytesRead(lastReadPosition, position, length);
        lastReadPosition = position + length;
        lastSeekPosition = lastReadPosition;
    }

    private int readCacheFile(final FileChannel fc, final long position, final ByteBuffer buffer) throws IOException {
        assert IndexInputUtils.assertFileChannelOpen(fc);
        final int bytesRead = Channels.readFromFileChannel(fc, position, buffer);
        if (bytesRead == -1) {
            throwEOF(position, buffer.remaining(), cacheFileReference);
        }
        stats.addCachedBytesRead(bytesRead);
        return bytesRead;
    }

    private void writeCacheFile(final FileChannel fc, final long start, final long end, final LongConsumer progressUpdater)
        throws IOException {
        assert IndexInputUtils.assertFileChannelOpen(fc);
        assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        final long length = end - start;
        final ByteBuffer copyBuffer = writeBuffer.get().clear();
        logger.trace("writing range [{}-{}] to cache file [{}]", start, end, cacheFileReference);

        long bytesCopied = 0L;
        long remaining = end - start;
        final long startTimeNanos = stats.currentTimeNanos();
        try (InputStream input = openInputStreamFromBlobStore(start, length)) {
            while (remaining > 0L) {
                final int bytesRead = BlobCacheUtils.readSafe(input, copyBuffer, start, remaining, cacheFileReference);
                IndexInputUtils.positionalWrite(fc, start + bytesCopied, copyBuffer.flip());
                copyBuffer.clear();
                bytesCopied += bytesRead;
                remaining -= bytesRead;
                progressUpdater.accept(start + bytesCopied);
            }
            final long endTimeNanos = stats.currentTimeNanos();
            stats.addCachedBytesWritten(bytesCopied, endTimeNanos - startTimeNanos);
        }
    }

    private int readDirectlyIfAlreadyClosed(long position, ByteBuffer b, Exception e) throws IOException {
        if (e instanceof AlreadyClosedException || e.getCause() instanceof AlreadyClosedException) {
            try {
                // cache file was evicted during the range fetching, read bytes directly from blob container
                final int length = b.remaining();
                logger.trace("direct reading of range [{}-{}] for cache file [{}]", position, position + length, cacheFileReference);

                final long startTimeNanos = stats.currentTimeNanos();
                try (InputStream input = openInputStreamFromBlobStore(position, length)) {
                    final int bytesRead = Streams.read(input, b, length);
                    if (bytesRead < length) {
                        throwEOF(position, length - bytesRead, cacheFileReference);
                    }
                    final long endTimeNanos = stats.currentTimeNanos();
                    stats.addDirectBytesRead(bytesRead, endTimeNanos - startTimeNanos);
                    return bytesRead;
                }
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
        }
        throw new IOException("failed to read data from cache", e);
    }

    /**
     * Opens an {@link InputStream} for the given range of bytes which reads the data directly from the blob store. If the requested range
     * spans multiple blobs then this stream will request them in turn.
     *
     * @param position The start of the range of bytes to read, relative to the start of the corresponding Lucene file.
     * @param readLength The number of bytes to read
     */
    private InputStream openInputStreamFromBlobStore(final long position, final long readLength) throws IOException {
        assert IndexInputUtils.assertCurrentThreadMayAccessBlobStore();
        if (fileInfo.numberOfParts() == 1L) {
            assert position + readLength <= fileInfo.length()
                : "cannot read [" + position + "-" + (position + readLength) + "] from [" + fileInfo + "]";
            stats.addBlobStoreBytesRequested(readLength);
            return directory.blobContainer().readBlob(fileInfo.name(), position, readLength);
        } else {
            final int startPart = getPartNumberForPosition(position);
            final int endPart = getPartNumberForPosition(position + readLength - 1);

            for (int currentPart = startPart; currentPart <= endPart; currentPart++) {
                final long startInPart = (currentPart == startPart) ? getRelativePositionInPart(position) : 0L;
                final long endInPart;
                endInPart = currentPart == endPart
                    ? getRelativePositionInPart(position + readLength - 1) + 1
                    : fileInfo.partBytes(currentPart);
                stats.addBlobStoreBytesRequested(endInPart - startInPart);
            }

            return new SlicedInputStream(endPart - startPart + 1) {
                @Override
                protected InputStream openSlice(int slice) throws IOException {
                    final int currentPart = startPart + slice;
                    final long startInPart = (currentPart == startPart) ? getRelativePositionInPart(position) : 0L;
                    final long endInPart;
                    endInPart = currentPart == endPart
                        ? getRelativePositionInPart(position + readLength - 1) + 1
                        : fileInfo.partBytes(currentPart);
                    return directory.blobContainer().readBlob(fileInfo.partName(currentPart), startInPart, endInPart - startInPart);
                }
            };
        }
    }

    /**
     * Compute the part number that contains the byte at the given position in the corresponding Lucene file.
     */
    private int getPartNumberForPosition(long position) {
        ensureValidPosition(position);
        final int part = fileInfo.numberOfParts() == 1 ? 0 : Math.toIntExact(position / fileInfo.partSize().getBytes());
        assert part <= fileInfo.numberOfParts() : "part number [" + part + "] exceeds number of parts: " + fileInfo.numberOfParts();
        assert part >= 0 : "part number [" + part + "] is negative";
        return part;
    }

    /**
     * Compute the position of the given byte relative to the start of its part.
     * @param position the position of the required byte (within the corresponding Lucene file)
     */
    private long getRelativePositionInPart(long position) {
        ensureValidPosition(position);
        final long pos = position % fileInfo.partSize().getBytes();
        assert pos < fileInfo.partBytes(getPartNumberForPosition(pos)) : "position in part [" + pos + "] exceeds part's length";
        assert pos >= 0L : "position in part [" + pos + "] is negative";
        return pos;
    }

    private void ensureValidPosition(long position) {
        assert position >= 0L && position < fileInfo.length() : position + " vs " + fileInfo.length();
        // noinspection ConstantConditions in case assertions are disabled
        if (position < 0L || position >= fileInfo.length()) {
            throw new IllegalArgumentException("Position [" + position + "] is invalid for a file of length [" + fileInfo.length() + "]");
        }
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        assert assertCurrentThreadIsNotCacheFetchAsync();

        final int bytesToRead = b.remaining();
        // We can detect that we're going to read the last 16 bytes (that contains the footer checksum) of the file. Such reads are often
        // executed when opening a Directory and since we have the checksum in the snapshot metadata we can use it to fill the ByteBuffer.
        if (IndexInputUtils.maybeReadChecksumFromFileInfo(fileInfo, getAbsolutePosition(), isClone, b)) {
            logger.trace("read footer of file [{}], bypassing all caches", fileInfo.physicalName());
        } else {
            final long position = getAbsolutePosition();
            if (logger.isTraceEnabled()) {
                logger.trace("readInternal: read [{}-{}] ([{}] bytes) from [{}]", position, position + bytesToRead, bytesToRead, this);
            }

            try {
                final ByteRange blobCacheByteRange = rangeToReadFromBlobCache(position, bytesToRead);
                if (blobCacheByteRange.isEmpty()) {
                    readWithoutBlobCache(b);
                } else {
                    readWithBlobCache(b, blobCacheByteRange);
                }
            } catch (final Exception e) {
                // may have partially filled the buffer before the exception was thrown, so try and get the remainder directly.
                final int alreadyRead = bytesToRead - b.remaining();
                final int bytesRead = readDirectlyIfAlreadyClosed(position + alreadyRead, b, e);
                assert alreadyRead + bytesRead == bytesToRead : alreadyRead + " + " + bytesRead + " vs " + bytesToRead;
            }

            readComplete(position, bytesToRead);
        }
        assert b.remaining() == 0L : b.remaining();
        stats.addLuceneBytesRead(bytesToRead);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        BlobCacheUtils.ensureSeek(pos, this);
        final long position = pos + this.offset;
        stats.incrementSeeks(lastSeekPosition, position);
        lastSeekPosition = position;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) && isClone == false) {
            stats.incrementCloseCount();
            cacheFileReference.releaseOnClose();
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public FrozenIndexInput clone() {
        final FrozenIndexInput clone = (FrozenIndexInput) super.clone();
        clone.closed = new AtomicBoolean(false);
        clone.isClone = true;
        return clone;
    }

    @Override
    public String toString() {
        return super.toString() + "[length=" + length() + ", file pointer=" + getFilePointer() + ", offset=" + offset + ']';
    }

    @Override
    protected String getFullSliceDescription(String sliceDescription) {
        final String resourceDesc = super.toString();
        if (sliceDescription != null) {
            return "slice(" + sliceDescription + ") of " + resourceDesc;
        }
        return resourceDesc;
    }

    @Override
    public IndexInput slice(String sliceName, long sliceOffset, long sliceLength) {
        BlobCacheUtils.ensureSlice(sliceName, sliceOffset, sliceLength, this);

        // Are we creating a slice from a CFS file?
        final boolean sliceCompoundFile = isCfs
            && IndexFileNames.getExtension(sliceName) != null
            && compoundFileOffset == 0L // not already a compound file
            && isClone == false; // tests aggressively clone and slice

        final ByteRange sliceHeaderByteRange;
        final ByteRange sliceFooterByteRange;
        final long sliceCompoundFileOffset;

        if (sliceCompoundFile) {
            sliceCompoundFileOffset = this.offset + sliceOffset;
            sliceHeaderByteRange = directory.getBlobCacheByteRange(sliceName, sliceLength).shift(sliceCompoundFileOffset);
            if (sliceHeaderByteRange.isEmpty() == false && sliceHeaderByteRange.length() < sliceLength) {
                sliceFooterByteRange = ByteRange.of(sliceLength - CodecUtil.footerLength(), sliceLength).shift(sliceCompoundFileOffset);
            } else {
                sliceFooterByteRange = ByteRange.EMPTY;
            }
        } else {
            sliceCompoundFileOffset = this.compoundFileOffset;
            sliceHeaderByteRange = ByteRange.EMPTY;
            sliceFooterByteRange = ByteRange.EMPTY;
        }
        final FrozenIndexInput slice = new FrozenIndexInput(
            sliceName,
            directory,
            fileInfo,
            context,
            stats,
            this.offset + sliceOffset,
            sliceCompoundFileOffset,
            sliceLength,
            cacheFileReference,
            cacheFile,
            defaultRangeSize,
            recoveryRangeSize,
            sliceHeaderByteRange,
            sliceFooterByteRange
        );
        slice.isClone = true;
        return slice;
    }
}
