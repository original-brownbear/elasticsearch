/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.blobstore.cache.single;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.core.internal.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

public final class SingleFileCache extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(SingleFileCache.class);

    private final Path path;

    private final long size;

    private final int pageSize;

    private final CachePage[] pages;

    private final FileChannel channel;

    public SingleFileCache(Path path, long size, int pageSize) throws IOException {
        super("single-file-cache");
        this.path = path;
        this.size = size;
        this.pageSize = pageSize;
        int pageCount = Math.toIntExact(size / pageSize);
        pages = new CachePage[pageCount];
        channel = FileChannel.open(path, StandardOpenOption.WRITE);
    }

    CachePage acquirePage() {
        synchronized (pages) {
            for (int i = 0; i < pages.length; i++) {
                final CachePage p = pages[i];
                if (p == null) {
                    final CachePage newPage = new CachePage(i);
                    pages[i] = newPage;
                    return newPage;
                }
            }
        }
        throw new IllegalStateException("no more pages left");
    }

    int pageSize() {
        return pageSize;
    }

    @Override
    protected void closeInternal() {
        try {
            channel.close();
        } catch (Exception e) {
            logger.warn("Failed to close file channel", e);
        }
    }

    private void releaseCachePage(CachePage page) {
        synchronized (pages) {
            pages[page.index] = null;
        }
    }

    final class CachePage extends AbstractRefCounted {

        private final int index;

        private int limit = -1;

        private CachePage(int index) {
            super("cache-page");
            this.index = index;
        }

        public void get(int offset, int length, ByteBuffer buffer) {
            assert offset + length <= limit;
        }

        public void initWith(Supplier<InputStream> stream, int length) throws IOException {
            if (limit >= 0) {
                return;
            }
            assert length <= pageSize;
            incRef();
            synchronized (SingleFileCache.this) {
                try {
                    channel.position(((long) pageSize) * index);
                    final OutputStream os = Channels.newOutputStream(channel);
                    Streams.copy(org.elasticsearch.common.io.Streams.limitStream(stream.get(), length), os, false);
                    os.flush();
                    limit = length;
                } finally {
                    decRef();
                }
            }
        }

        @Override
        protected void closeInternal() {
            releaseCachePage(this);
        }
    }
}
