/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.blobstore.cache.single;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;

public class SingleFileCacheTests extends ESTestCase {

    public void testAcquireAndReleasePages() throws Exception {
        final int pageSize = 2048;
        final int pages = 512;
        final SingleFileCache cache = new SingleFileCache(createTempFile(), pages * pageSize, pageSize);
        try {
            final SingleFileCache.CachePage page = cache.acquirePage();
            final int length = randomIntBetween(1, pageSize);
            page.initWith(() -> new ByteArrayInputStream(randomByteArrayOfLength(length)), length);
        } finally {
            cache.decRef();
        }
    }
}
