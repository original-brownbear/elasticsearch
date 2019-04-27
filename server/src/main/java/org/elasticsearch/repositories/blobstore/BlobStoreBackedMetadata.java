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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

final class BlobStoreBackedMetadata extends AbstractLifecycleComponent implements BlobStoreRepositoryMetadata, ClusterStateApplier {


    private final Supplier<BlobContainer> blobContainerSupplier;

    private final ClusterService clusterService;

    private final TransportService transportService;

    private volatile String currentStateUUID;

    BlobStoreBackedMetadata(Supplier<BlobContainer> blobContainerSupplier, ClusterService clusterService,
                            TransportService transportService) {
        this.blobContainerSupplier = blobContainerSupplier;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {

    }

    @Override
    public void addTombstones(Iterable<String> blobs, ActionListener<Void> listener) {

    }

    @Override
    public void pruneTombstones(Iterable<String> blobs, ActionListener<Void> listener) {

    }

    @Override
    public void addUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) {

    }

    @Override
    public void completeUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) {

    }

    @Override
    public void pendingUploads(ActionListener<Iterable<String>> listener) {

    }

    @Override
    public void tombstones(ActionListener<Iterable<String>> listener) {

    }

    @Override
    public void list(String prefix, ActionListener<Iterable<BlobMetaData>> listener) {

    }

    private Map<String, BlobStoreBlobMetaData> load() {
        return Collections.emptyMap();
    }

    private void store(Map<String, BlobStoreBlobMetaData> data, ActionListener<Void> listener) {

    }

    @Override
    protected void doStart() {
        clusterService.addLowPriorityApplier(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeApplier(this);
    }

    @Override
    protected void doClose() {
    }

    private static final class BlobStoreBlobMetaData implements BlobMetaData {

        private final String name;

        private final long length;

        private final BlobState state;

        BlobStoreBlobMetaData(String name, long length, BlobState state) {
            this.name = name;
            this.length = length;
            this.state = state;
        }

        public BlobState state() {
            return state;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public long length() {
            return length;
        }
    }
}
