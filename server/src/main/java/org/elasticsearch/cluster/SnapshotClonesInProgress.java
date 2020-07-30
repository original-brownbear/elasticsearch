package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SnapshotClonesInProgress extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "snapshot-clones";

    @Override
    public Version getMinimalSupportedVersion() {
        return SnapshotsService.CLONE_SNAPSHOT_VERSION;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    public static final class Entry implements Writeable, ToXContent, RepositoryOperation {

        private final String repository;

        private final long repositoryStateId;

        private final SnapshotId source;

        private final SnapshotId target;

        private final Collection<String> indices;

        private final Settings updatedSettings;

        private final Collection<String> ignoredSettings;

        public Entry(String repository, SnapshotId source, SnapshotId target, long repositoryStateId, Collection<String> indices,
                     Settings updatedSettings, Collection<String> ignoredSettings) {
            this.repository = repository;
            this.repositoryStateId = repositoryStateId;
            this.source = source;
            this.target = target;
            this.indices = indices;
            this.updatedSettings = updatedSettings;
            this.ignoredSettings = ignoredSettings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        @Override
        public String repository() {
            return null;
        }

        @Override
        public long repositoryStateId() {
            return 0;
        }
    }
}
