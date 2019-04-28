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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class BlobStoreBackedMetadata extends AbstractLifecycleComponent implements BlobStoreRepositoryMetadata, ClusterStateApplier {

    private static final String UPDATE_BLOB_STATUS_ACTION_NAME = "internal:cluster/blobstore/update_blob_status";

    private static final Logger logger = LogManager.getLogger(BlobStoreBackedMetadata.class);

    private final Supplier<BlobContainer> blobContainerSupplier;

    private final ClusterService clusterService;

    private final TransportService transportService;

    private final String repoName;

    private final ThreadPool threadPool;

    private final Map<RepoStateId, RepoMetaTrie> stateCache = new LinkedHashMap<>() {
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 1;
        }
    };

    private volatile List<ActionListener<RepoMetaTrie>> outstandingListeners = Collections.emptyList();

    private volatile RepoStateId repoStateId;

    BlobStoreBackedMetadata(String repoName, Supplier<BlobContainer> blobContainerSupplier, ClusterService clusterService,
                            TransportService transportService) {
        this.repoName = repoName;
        this.blobContainerSupplier = blobContainerSupplier;
        this.clusterService = clusterService;
        this.threadPool = clusterService.getClusterApplierService().threadPool();
        this.transportService = transportService;

    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedMaster() == true) {

            final RepositoriesState repositoriesState = event.state().custom(RepositoriesState.TYPE);

            if (repositoriesState == null) {
                initRepositoriesState();
                return;
            }
            final RepoStateId stateId = repositoriesState.getStateId(repoName);
            if (stateId == null) {
                initRepoInClusterState(repositoriesState);
                return;
            }

            repoStateId = stateId;


            if (event.previousState().nodes().isLocalNodeElectedMaster() == false) {
                // TODO: master failover:
                //       1. Take care of outstanding deletes
                //       2. Check that ongoing uploads still have their nodes up and running
            }
        }
    }

    private void findLatestState(ActionListener<RepoStateId> listener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() throws Exception {
                Map<String, BlobMetaData> blobs = blobContainerSupplier.get().listBlobsByPrefix("blobmeta-");
                listener.onResponse(
                    blobs.keySet().stream().map(blob -> {
                        String[] parts = blob.split("-");
                        if (parts.length != 3) {
                            return null;
                        }
                        try {
                            return new RepoStateId(Long.parseLong(parts[2]), parts[1]);
                        } catch (NumberFormatException e) {
                            return null;
                        }
                    }).filter(Objects::nonNull).max(Comparator.comparing(RepoStateId::time)).orElse(null)
                );
            }
        });
    }

    private void initRepositoriesState() {
        findLatestState(new ActionListener<>() {
            @Override
            public void onResponse(RepoStateId repoStateId) {
                if (repoStateId != null) {
                    clusterService.submitStateUpdateTask("init repositories state", new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            return ClusterState.builder(currentState).putCustom(
                                RepositoriesState.TYPE, new RepositoriesState(
                                    Collections.singletonMap(repoName, repoStateId)
                                )
                            ).build();
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.warn("Failed to submit repository cluster initialization task.");
                        }
                    });
                } else {
                    threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(this) {
                        @Override
                        protected void doRun() {
                            store(new RepoStateId(0L, UUIDs.base64UUID()), new RepoMetaTrie(Collections.emptyMap()), listener);
                        }
                    });
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to get initial repo state", e);
            }
        });
    }

    private void initRepoInClusterState(final RepositoriesState repositoriesState) {
        clusterService.submitStateUpdateTask("init repo in state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState).putCustom(
                    RepositoriesState.TYPE, repositoriesState.with(repoName, new RepoStateId(0L, UUIDs.base64UUID()))
                ).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Failed to submit repository state initialization task.");
            }
        });
    }

    @Override
    public void addTombstones(Iterable<String> blobs, ActionListener<Void> listener) {

    }

    @Override
    public void pruneTombstones(Iterable<String> blobs, ActionListener<Void> listener) {

    }

    @Override
    public void addUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) {
        final String nodeId = transportService.getLocalNode().getId();
        // TODO: If not master use network to add these uploads and get names.
        load(ActionListener.wrap(
            repoMetaTrie -> {
                final RepoStateId stateId = this.repoStateId.next();
                store(stateId, repoMetaTrie.withUploads(blobs, nodeId), ActionListener.wrap(v ->
                    clusterService.submitStateUpdateTask("init repo in state", new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            final RepositoriesState repositoriesState = currentState.custom(RepositoriesState.TYPE);
                            return ClusterState.builder(currentState).putCustom(
                                RepositoriesState.TYPE, repositoriesState.with(repoName, stateId)
                            ).build();
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            listener.onFailure(e);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                            listener.onResponse(null);
                        }
                    }), listener::onFailure));
            },
            listener::onFailure
        ));
    }

    @Override
    public void completeUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) {

    }

    @Override
    public void pendingUploads(ActionListener<Iterable<String>> listener) {

    }

    @Override
    public void tombstones(ActionListener<Iterable<String>> listener) {
        assert clusterService.state().nodes().isLocalNodeElectedMaster();
    }

    @Override
    public void list(String prefix, ActionListener<Iterable<? extends BlobMetaData>> listener) {
        load(ActionListener.map(listener, repoMetaTrie -> repoMetaTrie.list(prefix)));
    }

    private void load(ActionListener<RepoMetaTrie> listener) {
        final RepoMetaTrie repoMetaTrie;
        synchronized (this) {
            assert repoStateId != null;
            repoMetaTrie = stateCache.get(repoStateId);
        }
        if (repoMetaTrie == null) {
            synchronized (this) {
                final List<ActionListener<RepoMetaTrie>> listeners = new ArrayList<>(outstandingListeners);
                listeners.add(listener);
                outstandingListeners = listeners;
            }
        } else {
            listener.onResponse(repoMetaTrie);
        }
    }

    private void store(RepoStateId stateId, RepoMetaTrie data, ActionListener<RepoStateId> listener) {
        ActionListener.completeWith(listener, () -> {
            final BytesStreamOutput tmp = new BytesStreamOutput();
            data.writeTo(tmp);
            blobContainerSupplier.get().writeBlob(stateId.blob(), tmp.bytes().streamInput(), tmp.bytes().length(), false);
            final List<ActionListener<RepoMetaTrie>> listeners;
            synchronized (this) {
                stateCache.put(stateId, data);
                listeners = outstandingListeners;
                outstandingListeners = Collections.emptyList();
            }
            repoStateId = stateId;
            ActionListener.onResponse(listeners, data);
            return stateId;
        });
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

    private static final class RepoMetaTrie implements Writeable {

        private final Map<String, BlobStoreBlobMetaData> data;

        public Iterable<BlobStoreBlobMetaData> list(String prefix) {
            return data.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        }

        RepoMetaTrie(Map<String, BlobStoreBlobMetaData> data) {
            this.data = data;
        }

        RepoMetaTrie(StreamInput input) {
            data = Collections.emptyMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(data, StreamOutput::writeString, (out1, value) -> {
                out1.writeLong(value.length);
                out1.writeEnum(value.state);
                out1.writeOptionalString(value.uploader);
            });
        }

        public RepoMetaTrie withUploads(Iterable<BlobMetaData> metaData, String nodeId) {
            final Map<String, BlobStoreBlobMetaData> updated = new HashMap<>(data);
            for (BlobMetaData datum : metaData) {
                updated.put(datum.name(), new BlobStoreBlobMetaData(datum.name(), datum.length(), BlobState.UPLOADING, nodeId));
            }
            return new RepoMetaTrie(updated);
        }
    }

    private static final class RepoStateId {

        private final String uuid;

        private final long vectorTime;

        RepoStateId(long vectorTime, String uuid) {
            this.vectorTime = vectorTime;
            this.uuid = uuid;
        }

        public long time() {
            return vectorTime;
        }

        public String blob() {
            return "blobmeta-" + uuid + '-' + vectorTime;
        }

        public RepoStateId next() {
            return new RepoStateId(vectorTime + 1, UUIDs.base64UUID());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final RepoStateId stateId = (RepoStateId) o;
            return vectorTime == stateId.vectorTime &&
                uuid.equals(stateId.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, vectorTime);
        }
    }

    public static final class RepositoriesState extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

        public static final String TYPE = "repositories_state";

        private final Map<String, RepoStateId> states;

        public RepositoriesState(StreamInput in) throws IOException {
            this(in.readMap(StreamInput::readString, input -> new RepoStateId(input.readLong(), input.readString())));
        }

        RepositoriesState(Map<String, RepoStateId> states) {
            this.states = states;
        }

        public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(ClusterState.Custom.class, TYPE, in);
        }

        public RepositoriesState with(String repoName, RepoStateId id) {
            final Map<String, RepoStateId> newStates = new HashMap<>(states);
            final RepoStateId old = newStates.put(repoName, id);
            assert old == null || old.vectorTime < id.vectorTime;
            return new RepositoriesState(Collections.unmodifiableMap(newStates));
        }

        public RepoStateId getStateId(String name) {
            return states.get(name);
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_8_0_0;
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(states, StreamOutput::writeString, (out1, value) -> {
                out1.writeLong(value.vectorTime);
                out1.writeString(value.uuid);
            });
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("repos");
            for (Map.Entry<String, RepoStateId> entry : states.entrySet()) {
                String repo = entry.getKey();
                RepoStateId stateId = entry.getValue();
                builder.startObject(repo);
                builder.field("time", stateId.vectorTime);
                builder.field("uuid", stateId.uuid);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }

    private static final class BlobStoreBlobMetaData implements BlobMetaData {

        private final String name;

        private final long length;

        private final BlobState state;

        @Nullable
        private final String uploader;

        /**
         * @param uploader NodeId of uploading node
         */
        BlobStoreBlobMetaData(String name, long length, BlobState state, @Nullable String uploader) {
            this.name = name;
            this.length = length;
            this.state = state;
            assert uploader == null || state == BlobState.UPLOADING;
            this.uploader = uploader;
        }

        public String uploader() {
            return uploader;
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


    /**
     * Internal request that is used to send changes in snapshot status to master
     */
    public static class BlobUploadRequest extends MasterNodeRequest<BlobUploadRequest> {
        private Snapshot snapshot;
        private ShardId shardId;

        public BlobUploadRequest() {

        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshot = new Snapshot(in);
            shardId = ShardId.readShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshot.writeTo(out);
            shardId.writeTo(out);
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public ShardId shardId() {
            return shardId;
        }

        @Override
        public String toString() {
            // TODO: FOO
            return "TODO";
        }
    }

    static class BlobUploadRequestResponse extends ActionResponse {

    }

    private class BlobUploadAction
        extends TransportMasterNodeAction<BlobUploadRequest, BlobUploadRequestResponse> {

        BlobUploadAction(TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(
                UPDATE_BLOB_STATUS_ACTION_NAME, transportService, clusterService, threadPool,
                actionFilters, indexNameExpressionResolver, BlobUploadRequest::new
            );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected BlobUploadRequestResponse newResponse() {
            return new BlobUploadRequestResponse();
        }

        @Override
        protected void masterOperation(BlobUploadRequest request, ClusterState state,
            ActionListener<BlobUploadRequestResponse> listener) {

        }

        @Override
        protected ClusterBlockException checkBlock(BlobUploadRequest request, ClusterState state) {
            return null;
        }
    }
}
