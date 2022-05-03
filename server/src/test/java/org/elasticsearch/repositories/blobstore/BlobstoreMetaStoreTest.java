package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;

public class BlobstoreMetaStoreTest extends ESTestCase {

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
