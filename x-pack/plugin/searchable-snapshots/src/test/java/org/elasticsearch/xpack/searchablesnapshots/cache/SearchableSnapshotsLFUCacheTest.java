/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SearchableSnapshotsLFUCacheTest extends ESTestCase {

    public void testConcurrentStress() throws Exception {
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final Settings settings = Settings.builder()
                    .put(SearchableSnapshotsLFUCache.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1L))
                    .put(SearchableSnapshotsLFUCache.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1024L))
                    .build();
            final SearchableSnapshotsLFUCache cache = new SearchableSnapshotsLFUCache(settings, threadPool);
            final String snapshotUUID = UUIDs.randomBase64UUID(random());
            final String indexName = "test-idx-" + randomAlphaOfLength(5);
            final ShardId shardId = new ShardId(new Index(indexName, UUIDs.randomBase64UUID(random())), 0);
            final String fileName = "test-file-" + randomAlphaOfLength(5);
            final CacheKey file = new CacheKey(snapshotUUID, indexName, shardId, fileName);
            final long fileLength = randomLongBetween(1L, ByteSizeValue.ofMb(5L).getBytes());
            final SearchableSnapshotsLFUCache.SharedCacheFile cacheFile = cache.getSharedCacheFile(file, fileLength);

            final Path sourceFile = createTempFile();
            long written = 0L;
            final byte[] buffer = new byte[Math.toIntExact(Math.min(8096, fileLength))];
            try (OutputStream out = Files.newOutputStream(sourceFile)) {
                while (written < fileLength) {
                    random().nextBytes(buffer);
                    final int toWrite = Math.min(buffer.length, Math.toIntExact(fileLength - written));
                    out.write(buffer, 0, toWrite);
                    written += toWrite;
                }
            }

            final Tuple<Long, Long> readRange = Tuple.tuple(0L, randomLongBetween(1, fileLength));
            final Tuple<Long, Long> writeRange = Tuple.tuple(readRange.v1(), randomLongBetween(readRange.v2(), fileLength));
            try (FileChannel target = FileChannel.open(createTempFile(), StandardOpenOption.WRITE)) {
                final Future<Integer> readFuture = cacheFile.populateAndRead(writeRange, readRange,
                        (channel, channelPos, relativePos, length) -> {
                            target.position(relativePos);
                            return Math.toIntExact(channel.transferTo(channelPos, length, target));
                        },
                        (channel, channelPos, relativePos, length, progressUpdater) -> {
                            final long transferred;
                            try (FileChannel src = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
                                channel.position(channelPos);
                                transferred = channel.transferFrom(src, relativePos, length);
                            }
                            progressUpdater.accept(transferred);
                        }, threadPool.generic());
                readFuture.get();
            }


        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 30L, TimeUnit.SECONDS));
        }
    }
}