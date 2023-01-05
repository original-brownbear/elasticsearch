/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.store.input.CachedBlobContainerIndexInput;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;

public class FullSearchableSnapshotDirectory extends SearchableSnapshotDirectory {

    private static final Logger logger = LogManager.getLogger(FullSearchableSnapshotDirectory.class);

    private final boolean prewarmCache;

    public FullSearchableSnapshotDirectory(
        Supplier<BlobContainer> blobContainer,
        Supplier<BlobStoreIndexShardSnapshot> snapshot,
        BlobStoreCacheService blobStoreCacheService,
        String repository,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId shardId,
        Settings indexSettings,
        LongSupplier currentTimeNanosSupplier,
        CacheService cacheService,
        Path cacheDir,
        ShardPath shardPath,
        ThreadPool threadPool
    ) {
        super(
            blobContainer,
            snapshot,
            blobStoreCacheService,
            repository,
            snapshotId,
            indexId,
            shardId,
            indexSettings,
            currentTimeNanosSupplier,
            cacheService,
            cacheDir,
            shardPath,
            threadPool
        );
        this.prewarmCache = useCache ? SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.get(indexSettings) : false;
    }

    @Override
    protected IndexInput doOpenIndexInput(
        String name,
        IOContext context,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        IndexInputStats inputStats
    ) {
        return new CachedBlobContainerIndexInput(
            name,
            this,
            fileInfo,
            context,
            inputStats,
            cacheService.getRangeSize(),
            cacheService.getRecoveryRangeSize()
        );
    }

    @Override
    protected void prewarmCache(ActionListener<Void> listener) {
        if (prewarmCache == false) {
            recoveryState.setPreWarmComplete();
            listener.onResponse(null);
            return;
        }

        final BlockingQueue<Tuple<ActionListener<Void>, CheckedRunnable<Exception>>> queue = new LinkedBlockingQueue<>();
        final Executor executor = threadPool.executor(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);

        final CountDownActionListener completionListener = new CountDownActionListener(
            snapshot().totalFileCount(),
            ActionListener.wrap(ignored -> {
                recoveryState.setPreWarmComplete();
                listener.onResponse(null);
            }, listener::onFailure)
        );

        for (BlobStoreIndexShardSnapshot.FileInfo file : snapshot().indexFiles()) {
            boolean hashEqualsContents = file.metadata().hashEqualsContents();
            if (hashEqualsContents || isExcludedFromCache(file.physicalName())) {
                if (hashEqualsContents) {
                    recoveryState.getIndex().addFileDetail(file.physicalName(), file.length(), true);
                } else {
                    recoveryState.ignoreFile(file.physicalName());
                }
                completionListener.onResponse(null);
                continue;
            }
            recoveryState.getIndex().addFileDetail(file.physicalName(), file.length(), false);
            boolean submitted = false;
            try {
                final IndexInput input = openInput(file.physicalName(), CachedBlobContainerIndexInput.CACHE_WARMING_CONTEXT);
                assert input instanceof CachedBlobContainerIndexInput : "expected cached index input but got " + input.getClass();

                final int numberOfParts = file.numberOfParts();
                final StepListener<Void> fileCompletionListener = new StepListener<>();
                fileCompletionListener.addListener(completionListener);
                fileCompletionListener.whenComplete(ignored -> {
                    logger.debug("{} file [{}] prewarmed", shardId, file.physicalName());
                    input.close();
                }, e -> {
                    logger.warn(() -> format("%s prewarming failed for file [%s]", shardId, file.physicalName()), e);
                    IOUtils.closeWhileHandlingException(input);
                });

                final CountDownActionListener partsListener = new CountDownActionListener(numberOfParts, fileCompletionListener);
                submitted = true;
                for (int p = 0; p < numberOfParts; p++) {
                    final int part = p;
                    queue.add(Tuple.tuple(partsListener, () -> {
                        ensureOpen();

                        logger.trace("{} warming cache for [{}] part [{}/{}]", shardId, file.physicalName(), part + 1, numberOfParts);
                        final long startTimeInNanos = statsCurrentTimeNanosSupplier.getAsLong();
                        final long persistentCacheLength = ((CachedBlobContainerIndexInput) input).prefetchPart(part).v1();
                        if (persistentCacheLength == file.length()) {
                            recoveryState.markIndexFileAsReused(file.physicalName());
                        } else {
                            recoveryState.getIndex().addRecoveredBytesToFile(file.physicalName(), file.partBytes(part));
                        }

                        logger.trace(
                            () -> format(
                                "%s part [%s/%s] of [%s] warmed in [%s] ms",
                                shardId,
                                part + 1,
                                numberOfParts,
                                file.physicalName(),
                                timeValueNanos(statsCurrentTimeNanosSupplier.getAsLong() - startTimeInNanos).millis()
                            )
                        );
                    }));
                }
            } catch (IOException e) {
                logger.warn(() -> format("%s unable to prewarm file [%s]", shardId, file.physicalName()), e);
                if (submitted == false) {
                    completionListener.onFailure(e);
                }
            }
        }

        logger.debug("{} warming shard cache for [{}] files", shardId, queue.size());

        // Start as many workers as fit into the prewarming pool at once at the most
        final int workers = Math.min(threadPool.info(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME).getMax(), queue.size());
        for (int i = 0; i < workers; ++i) {
            prewarmNext(executor, queue);
        }
    }

    private void prewarmNext(final Executor executor, final BlockingQueue<Tuple<ActionListener<Void>, CheckedRunnable<Exception>>> queue) {
        try {
            final Tuple<ActionListener<Void>, CheckedRunnable<Exception>> next = queue.poll(0L, TimeUnit.MILLISECONDS);
            if (next == null) {
                return;
            }
            executor.execute(ActionRunnable.run(ActionListener.runAfter(next.v1(), () -> prewarmNext(executor, queue)), next.v2()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn(() -> format("%s prewarming worker has been interrupted", shardId), e);
        }
    }

    @Override
    public void clearCache() {
        for (BlobStoreIndexShardSnapshot.FileInfo file : files()) {
            cacheService.removeFromCache(createCacheKey(file.physicalName()));
        }
    }
}
