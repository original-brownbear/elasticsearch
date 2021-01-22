/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.blobstore.cache.single;

import org.elasticsearch.test.ESTestCase;

public class BlobStoreBlockCacheTests extends ESTestCase {

    public void testAcquireAndReleasePages() throws Exception {
        final int pageSize = 2048;
        final int pages = 512;
        final BlobStoreBlockCache cache = new BlobStoreBlockCache(createTempFile(), pages * pageSize, pageSize);
        try {

        } finally {
            cache.decRef();
        }
    }
}
