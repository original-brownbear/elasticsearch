/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobMetaData;

/**
 * {@link BlobStoreRepository} metadata store.
 */
public interface BlobStoreRepositoryMetadata {

    /**
     * Requests a blob id for writing to. Implementations must ensure that the blobs returned by this method are registered as pending
     * uploads the same way they would be if requested by a call to {@link #addUploads(Iterable, ActionListener)}.
     * @param prefix Prefix of the blob
     * @param parts How many parts does this blob have
     * @param listener Listener that is passed the blobs name on completion
     */
    void requestBlobId(String prefix, int parts, ActionListener<String> listener);

    /**
     * Marks the given blobs as deleted.
     * @param blobs Blobs to delete
     */
    void addTombstones(Iterable<String> blobs, ActionListener<Void> listener);

    /**
     * Remove the tombstones for the given blobs.
     * @param blobs Blobs to prune tombstones for
     */
    void pruneTombstones(Iterable<String> blobs, ActionListener<Void> listener);

    /**
     * Mark the given blobs as pending upload.
     * @param blobs Blobs to be uploaded
     * @throws BlobBusyException if some of the requested blobs are already being modified
     */
    void addUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) throws BlobBusyException;


    void completeUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener);

    void pendingUploads(ActionListener<Iterable<String>> listener);

    void tombstones(ActionListener<Iterable<String>> listener);

    /**
     * List all blobs with the given prefix
     * @param prefix Prefix
     */
    void list(String prefix, ActionListener<Iterable<BlobMetaData>> listener);

    enum BlobState {
        DELETED,
        UPLOADING,
        DONE
    }
}
