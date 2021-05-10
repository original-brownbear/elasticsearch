package org.elasticsearch.repositories;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.Metadata;

public final class SnapshotFinalizationContext extends ActionListener.Delegating<RepositoryData, RepositoryData> {

    private final Metadata metadata;

    private final long repositoryStateId;

    public SnapshotFinalizationContext(long repositoryStateId,
                                       Metadata metadata,
                                       ShardGenerations shardGenerations,
                                       Version repositoryMetaVersion,
                                       ActionListener<RepositoryData> delegate) {
        super(delegate);
        this.metadata = metadata;
        this.repositoryStateId = repositoryStateId;
    }

    @Override
    public void onResponse(RepositoryData repositoryData) {
        delegate.onResponse(repositoryData);
    }
}
