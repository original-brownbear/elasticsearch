package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

public final class SnapshotInfoStore {

    public static final String STORE_BLOB_PREFIX = "snapshot-store-";

    public static String add(BlobContainer container, SnapshotInfo snapshotInfo, String generation) throws IOException {
        try (InputStream in = container.readBlob(STORE_BLOB_PREFIX + generation)) {

        } catch (FileNotFoundException e) {
            // TODO: create from scratch
        }
    }

    public static List<SnapshotInfo> get(BlobContainer container, List<SnapshotId> ids) {
        return null;
    }

    public static void delete(List<SnapshotId> ids) {

    }

    private Iterator<SnapshotInfo> allIterator(InputStream inputStream) {

    }
}
