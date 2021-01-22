/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.blobstore.cache.single;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public final class BlobStoreBlockCache extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(BlobStoreBlockCache.class);

    private final Path path;

    private final int pageSize;

    private final Page[] pages;

    private final LinkedList<Page> availablePages = new LinkedList<>();

    private final FileChannel channel;

    public BlobStoreBlockCache(Path path, long size, int pageSize) throws IOException {
        super("single-file-cache");
        this.path = path;
        this.pageSize = pageSize;
        int pageCount = Math.toIntExact(size / pageSize);
        // TODO : enforce safe minimum page count
        pages = new Page[pageCount];
        channel = FileChannel.open(path, StandardOpenOption.WRITE);
    }

    @Override
    protected void closeInternal() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void evictPage(Page page) {
        pages[page.index] = page;
        availablePages.push(new Page(page.index));
    }

    @Nullable
    private Page acquirePageForWriting() {
        logger.trace("trying to acquire new page for writing");
        Page page;
        synchronized (availablePages) {
            page = availablePages.poll();
        }
        throw new IllegalStateException("Failed to acquire page for writing");
    }

    private final class Page extends AbstractRefCounted {

        private final int index;

        private int writerIndex;

        Page (int index) {
            super("block-cache-page");
            this.index = index;
        }

        void get(int offset, ByteBuffer dst) {
            if (offset + dst.remaining() >= writerIndex) {
                throw new IllegalArgumentException("Can't read beyond writer index");
            }
            // TODO: actual read
        }

        void put(ByteBuffer src) {
            writerIndex += src.remaining();
            // TODO: actual write
        }

        @Override
        protected void closeInternal() {
            evictPage(this);
        }
    }

    public final class SingleFileCache extends AbstractRefCounted {

        private final long size;

        private final List<Integer> offsets = new ArrayList<>();
        private final List<Page> pages = new ArrayList<>();

        SingleFileCache(long size) {
            super("single-file-cache");
            this.size = size;
        }

        // TODO: this should be safe for use by multiple readers concurrently
        //       the dst buffer will see its position moved by however many bytes could be read
        //       if it wasn't filled completely but the file contains more bytes then #write should be used to cache the missing bytes
        public void read(long offset, ByteBuffer dst) {
            final int pageNum = Math.toIntExact(offset / pageSize);
            final int pageIndex = Collections.binarySearch(offsets, pageNum);
            if (pageIndex < 0) {
                return;
            }
            final int indexInPage = Math.toIntExact(offset % pageSize);
            final Page p = pages.get(pageIndex);
            if (p.tryIncRef()) {
                try {
                    p.get(indexInPage, dst);
                } finally {
                    p.decRef();
                }
            } else {
                offsets.remove(pageIndex);
                pages.remove(pageIndex);
            }
        }

        // TODO: length should be a multiple of page size or the number of bytes remaining in the file ideally
        //       this will always work out since we ensure we have more pages than files that may be open concurrently
        public void write(long offset, InputStream stream, long length) {

        }

        @Override
        protected void closeInternal() {

        }
    }

}
