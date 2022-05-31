/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Context for finalizing a snapshot.
 */
public final class FinalizeSnapshotContext extends ActionListener.Delegating<
    Tuple<RepositoryData, List<SnapshotInfo>>,
    Tuple<RepositoryData, List<SnapshotInfo>>> {

    private final ShardGenerations updatedShardGenerations;

    /**
     * Obsolete shard generations map computed from the cluster state update that this finalization executed in
     * {@link #updatedClusterState}.
     */
    private final SetOnce<Map<RepositoryShardId, Set<ShardGeneration>>> obsoleteGenerations = new SetOnce<>();

    private final long repositoryStateId;

    private final Metadata clusterMetadata;

    private final List<SnapshotInfo> snapshotInfo;

    private final Version repositoryMetaVersion;

    /**
     * @param updatedShardGenerations updated shard generations
     * @param repositoryStateId       the unique id identifying the state of the repository when the snapshot began
     * @param clusterMetadata         cluster metadata
     * @param snapshotInfo            SnapshotInfo instance to write for this snapshot
     * @param repositoryMetaVersion   version of the updated repository metadata to write
     * @param listener                listener to be invoked with the new {@link RepositoryData} and {@link SnapshotInfo} after completing
     *                                the snapshot
     */
    public FinalizeSnapshotContext(
        ShardGenerations updatedShardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        List<SnapshotInfo> snapshotInfo,
        Version repositoryMetaVersion,
        ActionListener<Tuple<RepositoryData, List<SnapshotInfo>>> listener
    ) {
        super(listener);
        this.updatedShardGenerations = updatedShardGenerations;
        this.repositoryStateId = repositoryStateId;
        this.clusterMetadata = clusterMetadata;
        this.snapshotInfo = snapshotInfo;
        this.repositoryMetaVersion = repositoryMetaVersion;
    }

    public long repositoryStateId() {
        return repositoryStateId;
    }

    public ShardGenerations updatedShardGenerations() {
        return updatedShardGenerations;
    }

    public List<SnapshotInfo> snapshotInfo() {
        return snapshotInfo;
    }

    public Version repositoryMetaVersion() {
        return repositoryMetaVersion;
    }

    public Metadata clusterMetadata() {
        return clusterMetadata;
    }

    public Map<RepositoryShardId, Set<ShardGeneration>> obsoleteShardGenerations() {
        assert obsoleteGenerations.get() != null : "must only be called after #updatedClusterState";
        return obsoleteGenerations.get();
    }

    public ClusterState updatedClusterState(ClusterState state) {
        ClusterState updatedState = state;
        for (SnapshotInfo info : snapshotInfo) {
            updatedState = SnapshotsService.stateWithoutSnapshot(state, info.snapshot());
        }
        obsoleteGenerations.set(
            updatedState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .obsoleteGenerations(snapshotInfo.get(0).repository(), state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY))
        );
        return updatedState;
    }

    @Override
    public void onResponse(Tuple<RepositoryData, List<SnapshotInfo>> repositoryData) {
        delegate.onResponse(repositoryData);
    }
}
