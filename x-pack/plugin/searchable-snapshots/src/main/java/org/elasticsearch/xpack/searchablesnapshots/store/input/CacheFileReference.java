/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.util.concurrent.atomic.AtomicReference;

class CacheFileReference implements CacheFile.EvictionListener {

    private final long fileLength;
    private final CacheKey cacheKey;
    private final SearchableSnapshotDirectory directory;
    final AtomicReference<CacheFile> cacheFile = new AtomicReference<>(); // null if evicted or not yet acquired

    CacheFileReference(SearchableSnapshotDirectory directory, String fileName, long fileLength) {
        this.cacheKey = directory.createCacheKey(fileName);
        this.fileLength = fileLength;
        this.directory = directory;
    }

    CacheFile get() throws Exception {
        CacheFile currentCacheFile = cacheFile.get();
        if (currentCacheFile != null) {
            return currentCacheFile;
        }

        final CacheFile newCacheFile = directory.getCacheFile(cacheKey, fileLength);
        synchronized (this) {
            currentCacheFile = cacheFile.get();
            if (currentCacheFile != null) {
                return currentCacheFile;
            }
            newCacheFile.acquire(this);
            final CacheFile previousCacheFile = cacheFile.getAndSet(newCacheFile);
            assert previousCacheFile == null;
            return newCacheFile;
        }
    }

    @Override
    public void onEviction(final CacheFile evictedCacheFile) {
        synchronized (this) {
            if (cacheFile.compareAndSet(evictedCacheFile, null)) {
                evictedCacheFile.release(this);
            }
        }
    }

    void releaseOnClose() {
        synchronized (this) {
            final CacheFile currentCacheFile = cacheFile.getAndSet(null);
            if (currentCacheFile != null) {
                currentCacheFile.release(this);
            }
        }
    }

    @Override
    public String toString() {
        return "CacheFileReference{"
                + "cacheKey='"
                + cacheKey
                + '\''
                + ", fileLength="
                + fileLength
                + ", acquired="
                + (cacheFile.get() != null)
                + '}';
    }
}
