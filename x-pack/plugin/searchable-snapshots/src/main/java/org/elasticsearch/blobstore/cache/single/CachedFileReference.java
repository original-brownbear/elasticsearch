/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache.single;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public final class CachedFileReference extends AbstractRefCounted {

    private final long length;

    private Map<Integer, SingleFileCache.CachePage> pages;

    private final SingleFileCache singleFileCache;

    CachedFileReference(SingleFileCache singleFileCache, long length) {
        super("cached-file-reference");
        this.length = length;
        this.singleFileCache = singleFileCache;
        singleFileCache.incRef();
    }

    public int read(long offset, ByteBuffer buffer, CheckedBiFunction<Long, Long, InputStream, IOException> blobReader) throws IOException {
        int pageIndex = Math.toIntExact(offset % singleFileCache.pageSize());
        final SingleFileCache.CachePage relevantPage;
        final SingleFileCache.CachePage p = pages.get(pageIndex);
        if (p != null) {
            if (p.tryIncRef()) {
                relevantPage = p;
            } else {
                relevantPage = singleFileCache.acquirePage();
                pages.put(pageIndex, relevantPage);
            }
        } else {
            // TODO: shorter + locking
            relevantPage = singleFileCache.acquirePage();
            pages.put(pageIndex, relevantPage);
        }
        // TODO: real offsets for page alignment
        relevantPage.initWith(() -> blobReader.apply(offset, (long) buffer.remaining()), buffer.remaining());
        return 0;
    }

    @Override
    protected void closeInternal() {
        for (SingleFileCache.CachePage value : pages.values()) {
            value.decRef();
        }
    }
}
