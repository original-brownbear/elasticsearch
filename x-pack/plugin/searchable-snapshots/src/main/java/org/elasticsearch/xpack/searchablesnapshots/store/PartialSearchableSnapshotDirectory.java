/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;
import org.elasticsearch.xpack.searchablesnapshots.store.input.FrozenIndexInput;

import java.nio.file.Path;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class PartialSearchableSnapshotDirectory extends SearchableSnapshotDirectory {

    private final FrozenCacheService frozenCacheService;

    public PartialSearchableSnapshotDirectory(
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
        ThreadPool threadPool,
        FrozenCacheService frozenCacheService
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
        this.frozenCacheService = frozenCacheService;
    }

    @Override
    protected IndexInput doOpenIndexInput(
        String name,
        IOContext context,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        IndexInputStats inputStats
    ) {
        return new FrozenIndexInput(
            name,
            this,
            fileInfo,
            context,
            inputStats,
            frozenCacheService.getRangeSize(),
            frozenCacheService.getRecoveryRangeSize()
        );
    }

    @Override
    protected void prewarmCache(ActionListener<Void> listener) {
        recoveryState.setPreWarmComplete();
        listener.onResponse(null);
    }

    @Override
    public void clearCache() {
        for (BlobStoreIndexShardSnapshot.FileInfo file : files()) {
            frozenCacheService.removeFromCache(createCacheKey(file.physicalName()));
        }
    }

    public FrozenCacheService.FrozenCacheFile getFrozenCacheFile(String fileName, long length) {
        return frozenCacheService.getFrozenCacheFile(createCacheKey(fileName), length);
    }
}
