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
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class BlobStoreMetadataService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final String UPDATE_BLOB_STATUS_ACTION_NAME = "internal:cluster/blobstore/update_blob_status";

    private static final Logger logger = LogManager.getLogger(BlobStoreMetadataService.class);

    private final ClusterService clusterService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Map<RepoStateId, RepoMetaTrie> stateCache = new LinkedHashMap<>() {
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 10;
        }
    };

    private final BlobUploadAction blobUploadAction;

    private volatile Map<String, MetaDataStorage> metaStores = Collections.emptyMap();

    public BlobStoreMetadataService(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        this.threadPool = clusterService.getClusterApplierService().threadPool();
        this.transportService = transportService;
        // TODO: Pass these down
        blobUploadAction = new BlobUploadAction(transportService, clusterService, threadPool, new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver());
    }

    public BlobStoreRepositoryMetadataService getMetaStore(String repoName, BlobRepositoryMetaPersistence persistence) {
        synchronized (this) {
            MetaDataStorage store = metaStores.get(repoName);
            if (store == null) {
                store = new MetaDataStorage(repoName, persistence);
                Map<String, MetaDataStorage> updatedStores = new HashMap<>(metaStores);
                updatedStores.put(repoName, store);
                metaStores = updatedStores;
            }
            return store;
        }
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    public final class MetaDataStorage implements BlobStoreRepositoryMetadataService {

        private final Object mutex = new Object();

        private final String repoName;

        private final BlobRepositoryMetaPersistence persistence;

        private volatile List<ActionListener<RepoMetaTrie>> outstandingListeners = Collections.emptyList();

        /**
         * State Id of last meta state that was written to persistent storage and published to the cluster state.
         */
        private volatile RepoStateId safeRepoStateId;

        /**
         * State Id of last meta state that was written to persistent storage.
         */
        private volatile RepoStateId persistedStateId;

        private volatile RepoStateId nextStateId;

        private volatile RepoMetaTrie pendingTrie;

        MetaDataStorage(String repoName, BlobRepositoryMetaPersistence persistence) {
            this.repoName = repoName;
            this.persistence = persistence;
        }

        void repoStateId(RepoStateId id) {
            safeRepoStateId = id;
            synchronized (mutex) {
                final RepoMetaTrie trie = id.vectorTime == 0 ? new RepoMetaTrie(Collections.emptyMap()) : stateCache.get(id);
                if (trie == null) {
                    persistence.load(id, new ActionListener<>() {
                        @Override
                        public void onResponse(RepoMetaTrie repoMetaTrie) {
                            final List<ActionListener<RepoMetaTrie>> currentListeners = outstandingListeners;
                            outstandingListeners = Collections.emptyList();
                            ActionListener.onResponse(currentListeners, repoMetaTrie);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("Failed to load new state", e);
                        }
                    });
                } else {
                    final List<ActionListener<RepoMetaTrie>> currentListeners = outstandingListeners;
                    outstandingListeners = Collections.emptyList();
                    ActionListener.onResponse(currentListeners, trie);
                }
            }
        }

        @Override
        public void addTombstones(Iterable<String> blobs, ActionListener<Void> listener) {

        }

        @Override
        public void pruneTombstones(Iterable<String> blobs, ActionListener<Void> listener) {

        }

        @Override
        public void addUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) {
            final List<String> names = new ArrayList<>();
            for (BlobMetaData blob : blobs) {
                names.add(blob.name());
            }
            transportService.sendRequest(transportService.getLocalNode(), UPDATE_BLOB_STATUS_ACTION_NAME,
                new BlobUploadRequest(repoName, false, names), new BlobUploadResponseHandler(listener));
        }

        public void doAddUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) {
            final String nodeId = transportService.getLocalNode().getId();
            executeUpdate(listener, repoMetaTrie -> {
                final RepoStateId stateId = incrementId();
                synchronized (mutex) {
                    pendingTrie = (pendingTrie == null ? repoMetaTrie : pendingTrie).withUploads(blobs, nodeId);
                }
                stateCache.put(stateId, pendingTrie);
                storeNextState(stateId, "add uploads", listener);
            });
        }

        @Override
        public void completeUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) {
            final List<String> names = new ArrayList<>();
            for (BlobMetaData blob : blobs) {
                names.add(blob.name());
            }
            transportService.sendRequest(transportService.getLocalNode(), UPDATE_BLOB_STATUS_ACTION_NAME,
                new BlobUploadRequest(repoName, true, names), new BlobUploadResponseHandler(listener));
        }

        public void doCompleteUploads(Iterable<BlobMetaData> blobs, ActionListener<Void> listener) {
            final String nodeId = transportService.getLocalNode().getId();
            executeUpdate(listener, repoMetaTrie ->  {
                final RepoStateId stateId = incrementId();
                synchronized (mutex) {
                    pendingTrie = (pendingTrie == null ? repoMetaTrie : pendingTrie).withCompletedUploads(blobs, nodeId);
                }
                stateCache.put(stateId, pendingTrie);
                storeNextState(stateId, "update repo in state", listener);
            });
        }

        private void executeUpdate(ActionListener<Void> listener, CheckedConsumer<RepoMetaTrie, Exception> consumer) {
            if (safeRepoStateId == null) {
                synchronized (mutex) {
                    final List<ActionListener<RepoMetaTrie>> listeners = new ArrayList<>(outstandingListeners);
                    listeners.add(ActionListener.wrap(consumer, listener::onFailure));
                    outstandingListeners = listeners;
                }
            } else {
                final RepoMetaTrie trie;
                synchronized (mutex) {
                    trie = stateCache.get(safeRepoStateId);
                }
                if (trie == null) {
                    persistence.load(safeRepoStateId, ActionListener.wrap(consumer, listener::onFailure));
                } else {
                    try {
                        consumer.accept(trie);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
            }
        }

        private void storeNextState(RepoStateId stateId, String s, ActionListener<Void> listener) {
            persistence.store(stateId, pendingTrie, ActionListener.wrap(v -> {
                synchronized (mutex) {
                    if (persistedStateId.time() > stateId.time()) {
                        listener.onResponse(null);
                        return;
                    }
                    persistedStateId = stateId;
                }
                clusterService.submitStateUpdateTask(s, new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final RepositoriesState repositoriesState = currentState.custom(RepositoriesState.TYPE);
                        final RepositoriesState newRepositoriesState = repositoriesState.with(repoName, persistedStateId);
                        if (newRepositoriesState == repositoriesState) {
                            return currentState;
                        }
                        return ClusterState.builder(currentState).putCustom(
                            RepositoriesState.TYPE, newRepositoriesState
                        ).build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final RepositoriesState repositoriesState = newState.custom(RepositoriesState.TYPE);
                        safeRepoStateId = repositoriesState.getStateId(repoName);
                        listener.onResponse(null);
                    }
                });
            }, listener::onFailure));
        }

        private RepoStateId incrementId() {
            synchronized (mutex) {
                if (nextStateId == null) {
                    nextStateId = safeRepoStateId.next();
                } else {
                    nextStateId = nextStateId.next();
                }
                return nextStateId;
            }
        }

        @Override
        public void pendingUploads(final ActionListener<Iterable<String>> listener) {

        }

        @Override
        public void tombstones(final ActionListener<Iterable<String>> listener) {

        }

        @Override
        public void list(String prefix, ActionListener<Iterable<? extends BlobMetaData>> listener) {
            load(ActionListener.map(listener, repoMetaTrie -> repoMetaTrie.list(prefix)));
        }

        private void load(ActionListener<RepoMetaTrie> listener) {
            final RepoMetaTrie repoMetaTrie;
            if (safeRepoStateId != null) {
                synchronized (mutex) {
                    repoMetaTrie = stateCache.get(safeRepoStateId);
                }
            } else {
                repoMetaTrie = null;
                initRepoInClusterState();
            }
            if (repoMetaTrie == null) {
                synchronized (mutex) {
                    final List<ActionListener<RepoMetaTrie>> listeners = new ArrayList<>(outstandingListeners);
                    listeners.add(listener);
                    outstandingListeners = listeners;
                }
            } else {
                listener.onResponse(repoMetaTrie);
            }
            if (safeRepoStateId != null) {
                persistence.load(safeRepoStateId, new ActionListener<>() {
                    @Override
                    public void onResponse(RepoMetaTrie repoMetaTrie) {
                        final List<ActionListener<RepoMetaTrie>> currentListeners;
                        synchronized (mutex) {
                            currentListeners = outstandingListeners;
                            outstandingListeners = Collections.emptyList();
                        }
                        ActionListener.onResponse(currentListeners, repoMetaTrie);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }
                });
            }
        }

        private void initRepoInClusterState() {
            persistence.recover(new ActionListener<>() {
                @Override
                public void onResponse(RepoStateId foundStateId) {
                    clusterService.submitStateUpdateTask("init repo [" + repoName + "] in state", new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            RepositoriesState repositoriesState = currentState.custom(RepositoriesState.TYPE);
                            repositoriesState =
                                repositoriesState == null ? new RepositoriesState(Collections.emptyMap()) : repositoriesState;
                            if (repositoriesState.getStateId(repoName) != null) {
                                return currentState;
                            }
                            return ClusterState.builder(currentState)
                                .putCustom(RepositoriesState.TYPE, repositoriesState.with(repoName, foundStateId)).build();
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            RepositoriesState repositoriesState = newState.custom(RepositoriesState.TYPE);
                            safeRepoStateId = repositoriesState.getStateId(repoName);
                            persistedStateId = safeRepoStateId;
                            final RepoMetaTrie trie = new BlobStoreMetadataService.RepoMetaTrie(Collections.emptyMap());
                            synchronized (mutex) {
                                stateCache.put(safeRepoStateId, trie);
                                final List<ActionListener<RepoMetaTrie>> currentListeners = outstandingListeners;
                                outstandingListeners = Collections.emptyList();
                                ActionListener.onResponse(currentListeners, stateCache.get(safeRepoStateId));
                            }
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.warn("Failed to submit repository state initialization task", e);
                        }
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to recover stateId", e);
                }
            });
        }

        private final class BlobUploadResponseHandler implements TransportResponseHandler<BlobUploadResponse> {

            private final ActionListener<Void> listener;

            BlobUploadResponseHandler(ActionListener<Void> listener) {
                this.listener = listener;
            }

            @Override
            public BlobUploadResponse read(StreamInput in) throws IOException {
                final BlobUploadResponse resp = new BlobUploadResponse();
                resp.readFrom(in);
                return resp;
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

            @Override
            public void handleResponse(BlobUploadResponse response) {
                listener.onResponse(null);
            }
        }
    }

    public interface BlobRepositoryMetaPersistence {

        void store(RepoStateId stateId, RepoMetaTrie data, ActionListener<RepoStateId> listener);

        void load(RepoStateId stateId, ActionListener<RepoMetaTrie> listener);

        void recover(ActionListener<RepoStateId> listener);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {

        final RepositoriesState repositoriesState = event.state().custom(RepositoriesState.TYPE);

        final Map<String, MetaDataStorage> repos;
        synchronized (this) {
            repos = new HashMap<>(metaStores);
        }

        for (Map.Entry<String, MetaDataStorage> entry : repos.entrySet()) {
            final RepoStateId stateId = repositoriesState == null ? null : repositoriesState.getStateId(entry.getKey());
            if (stateId == null) {
                if (event.state().nodes().isLocalNodeElectedMaster() == true) {
                    entry.getValue().initRepoInClusterState();
                }
                continue;
            }
            entry.getValue().repoStateId(stateId);
        }

        if (event.previousState().nodes().isLocalNodeElectedMaster() == false) {
            // TODO: master failover:
            //       1. Take care of outstanding deletes
            //       2. Check that ongoing uploads still have their nodes up and running
        }
    }

    @Override
    protected void doStart() {
        assert blobUploadAction != null;
        clusterService.addLowPriorityApplier(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeApplier(this);
    }

    @Override
    protected void doClose() {

    }

    public static final class RepoMetaTrie implements Writeable {

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

        RepoMetaTrie(StreamInput input) throws IOException {
            this(
                input.readMap(StreamInput::readString, in -> {
                    String name = in.readString();
                    long length = in.readLong();
                    BlobStoreRepositoryMetadataService.BlobState state = in.readEnum(BlobStoreRepositoryMetadataService.BlobState.class);
                    String uploader = in.readOptionalString();
                    return new BlobStoreBlobMetaData(name, length, state, uploader);
                }));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(data, StreamOutput::writeString, (out1, value) -> {
                out1.writeString(value.name);
                out1.writeLong(value.length);
                out1.writeEnum(value.state);
                out1.writeOptionalString(value.uploader);
            });
        }

        public RepoMetaTrie withUploads(Iterable<BlobMetaData> metaData, String nodeId) {
            final Map<String, BlobStoreBlobMetaData> updated = new HashMap<>(data);
            for (BlobMetaData datum : metaData) {
                updated.put(datum.name(),
                    new BlobStoreBlobMetaData(datum.name(), datum.length(), BlobStoreRepositoryMetadataService.BlobState.UPLOADING, nodeId)
                );
            }
            return new RepoMetaTrie(updated);
        }

        public RepoMetaTrie withCompletedUploads(Iterable<BlobMetaData> metaData, String nodeId) {
            final Map<String, BlobStoreBlobMetaData> updated = new HashMap<>(data);
            for (BlobMetaData datum : metaData) {
                updated.put(datum.name(),
                    new BlobStoreBlobMetaData(datum.name(), datum.length(), BlobStoreRepositoryMetadataService.BlobState.DONE, nodeId)
                );
            }
            return new RepoMetaTrie(updated);
        }
    }

    public static final class RepoStateId {

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
            return vectorTime == stateId.vectorTime && uuid.equals(stateId.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, vectorTime);
        }

        @Override
        public String toString() {
            return blob();
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
            if (Objects.equals(id, old)) {
                return this;
            }
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

        private final BlobStoreRepositoryMetadataService.BlobState state;

        @Nullable
        private final String uploader;

        /**
         * @param uploader NodeId of uploading node
         */
        BlobStoreBlobMetaData(String name, long length, BlobStoreRepositoryMetadataService.BlobState state, String uploader) {
            this.name = name;
            this.length = length;
            this.state = state;
            this.uploader = uploader;
        }

        public String uploader() {
            return uploader;
        }

        public BlobStoreRepositoryMetadataService.BlobState state() {
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

        private String repoName;

        private boolean completed;

        private List<String> blobs;

        BlobUploadRequest() {
        }

        public BlobUploadRequest(String repoName, boolean completed, List<String> blobs) {
            this.repoName = repoName;
            this.completed = completed;
            this.blobs = blobs;
        }

        public String repoName() {
            return repoName;
        }

        public boolean completed() {
            return completed;
        }

        public List<String> blobs() {
            return blobs;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            repoName = in.readString();
            completed = in.readBoolean();
            blobs = in.readStringList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repoName);
            out.writeBoolean(completed);
            out.writeStringCollection(blobs);
        }
    }

    static class BlobUploadResponse extends ActionResponse {
    }

    private class BlobUploadAction extends TransportMasterNodeAction<BlobUploadRequest, BlobUploadResponse> {

        BlobUploadAction(TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(
                UPDATE_BLOB_STATUS_ACTION_NAME, transportService, clusterService, threadPool,
                actionFilters, indexNameExpressionResolver, BlobUploadRequest::new
            );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        protected BlobUploadResponse newResponse() {
            return new BlobUploadResponse();
        }

        @Override
        protected void masterOperation(BlobUploadRequest request, ClusterState state, ActionListener<BlobUploadResponse> listener) {
            ActionListener<Void> wrapped = ActionListener.map(listener, v -> new BlobUploadResponse());
            try {
                if (request.completed()) {
                    metaStores.get(request.repoName()).doCompleteUploads(request.blobs.stream().map(
                        s -> new BlobMetaData() {
                            @Override
                            public String name() {
                                return s;
                            }

                            @Override
                            public long length() {
                                return 0;
                            }
                        }
                    ).collect(Collectors.toList()), wrapped);
                } else {
                    metaStores.get(request.repoName()).doAddUploads(request.blobs.stream().map(
                        s -> new BlobMetaData() {
                            @Override
                            public String name() {
                                return s;
                            }

                            @Override
                            public long length() {
                                return 0;
                            }
                        }
                    ).collect(Collectors.toList()), wrapped);
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        protected ClusterBlockException checkBlock(BlobUploadRequest request, ClusterState state) {
            return null;
        }
    }
}
