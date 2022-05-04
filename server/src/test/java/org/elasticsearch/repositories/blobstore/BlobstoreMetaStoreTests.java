/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;

public class BlobstoreMetaStoreTests extends ESTestCase {

    public void testStoreAndRetrieveSnapshotInfo() throws Exception {
        try (BlobstoreMetaStore metaStore = new BlobstoreMetaStore()) {
            final SnapshotInfo snapshotInfo = SnapshotInfoTestUtils.createRandomSnapshotInfo();
            metaStore.put(snapshotInfo);
            final SnapshotInfo read = metaStore.get(snapshotInfo.repository(), snapshotInfo.snapshotId().getUUID());
            metaStore.persist("/tmp/derby-db");
            assertEquals(snapshotInfo, read);
        }
    }
}
