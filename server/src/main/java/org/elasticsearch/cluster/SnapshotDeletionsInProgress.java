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

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A class that represents the snapshot deletions that are in progress in the cluster.
 */
public class SnapshotDeletionsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final Version DELETE_INIT_VERSION = Version.V_8_0_0;

    public static final String TYPE = "snapshot_deletions";

    // the list of snapshot deletion request entries
    private final List<Entry> entries;

    public SnapshotDeletionsInProgress(List<Entry> entries) {
        this.entries = Collections.unmodifiableList(entries);
    }

    public SnapshotDeletionsInProgress(StreamInput in) throws IOException {
        this.entries = Collections.unmodifiableList(in.readList(Entry::new));
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} with the given
     * {@link Entry} added.
     */
    public static SnapshotDeletionsInProgress newInstance(Entry entry) {
        return new SnapshotDeletionsInProgress(Collections.singletonList(entry));
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} which adds
     * the given {@link Entry} to the invoking instance.
     */
    public SnapshotDeletionsInProgress withAddedEntry(Entry entry) {
        List<Entry> entries = new ArrayList<>(getEntries());
        entries.add(entry);
        return new SnapshotDeletionsInProgress(entries);
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} which removes
     * the given entry from the invoking instance.
     */
    public SnapshotDeletionsInProgress withRemovedEntry(Entry entry) {
        List<Entry> entries = new ArrayList<>(getEntries());
        entries.remove(entry);
        return new SnapshotDeletionsInProgress(entries);
    }

    /**
     * Returns an unmodifiable list of snapshot deletion entries.
     */
    public List<Entry> getEntries() {
        return entries;
    }

    /**
     * Returns {@code true} if there are snapshot deletions in progress in the cluster,
     * returns {@code false} otherwise.
     */
    public boolean hasDeletionsInProgress() {
        return entries.isEmpty() == false;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnapshotDeletionsInProgress that = (SnapshotDeletionsInProgress) o;
        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return 31 + entries.hashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TYPE);
        for (Entry entry : entries) {
            builder.startObject();
            {
                builder.field("repository", entry.repository());
                builder.field("snapshot", entry.snapshotNamePattern());
                builder.humanReadableField("start_time_millis", "start_time", new TimeValue(entry.startTime));
                builder.field("repository_state_id", entry.repositoryStateId);
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("SnapshotDeletionsInProgress[");
        for (int i = 0; i < entries.size(); i++) {
            builder.append(entries.get(i).snapshotNamePattern());
            if (i + 1 < entries.size()) {
                builder.append(",");
            }
        }
        return builder.append("]").toString();
    }

    /**
     * A class representing a snapshot deletion request entry in the cluster state.
     */
    public static final class Entry implements Writeable, RepositoryOperation {

        private final String repository;

        private final String snapshotNamePattern;

        @Nullable
        private final List<String> snapshotUUIDs;

        private final long startTime;
        private final long repositoryStateId;

        public Entry(String repository, String snapshotNamePattern, long startTime) {
            this.repository = repository;
            this.snapshotNamePattern = snapshotNamePattern;
            this.snapshotUUIDs = null;
            this.startTime = startTime;
            this.repositoryStateId = RepositoryData.UNKNOWN_REPO_GEN;
        }

        public Entry(Snapshot snapshot, long startTime, long repositoryStateId) {
            this.repository = snapshot.getRepository();
            this.snapshotNamePattern = snapshot.getSnapshotId().getName();
            this.snapshotUUIDs = Collections.singletonList(snapshot.getSnapshotId().getUUID());
            this.startTime = startTime;
            this.repositoryStateId = repositoryStateId;
        }

        public Entry(StreamInput in) throws IOException {
            if (in.getVersion().before(DELETE_INIT_VERSION)) {
                final Snapshot snapshot = new Snapshot(in);
                this.repository = snapshot.getRepository();
                this.snapshotNamePattern = snapshot.getSnapshotId().getName();
                this.snapshotUUIDs = Collections.singletonList(snapshot.getSnapshotId().getUUID());
            } else {
                this.repository = in.readString();
                this.snapshotNamePattern = in.readString();
                final String[] snapshotUUIDs = in.readOptionalStringArray();
                if (snapshotUUIDs == null) {
                    this.snapshotUUIDs = null;
                } else {
                    this.snapshotUUIDs = List.of(snapshotUUIDs);
                }
            }
            this.startTime = in.readVLong();
            this.repositoryStateId = in.readLong();
        }

        public String snapshotNamePattern() {
            return snapshotNamePattern;
        }

        @Nullable
        public List<String> snapshotUUIDs() {
            return snapshotUUIDs;
        }

        /**
         * The start time in milliseconds for deleting the snapshots.
         */
        public long getStartTime() {
            return startTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entry that = (Entry) o;
            return snapshotNamePattern.equals(that.snapshotNamePattern)
                       && startTime == that.startTime
                       && repositoryStateId == that.repositoryStateId
                       && Objects.equals(snapshotUUIDs, that.snapshotUUIDs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotNamePattern, snapshotUUIDs, repository, startTime, repositoryStateId);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().before(DELETE_INIT_VERSION)) {
                assert snapshotUUIDs != null && snapshotUUIDs.size() == 1 :
                    "Can only write a single uuid to delete to [" + out.getVersion() + "] but saw UUIDs " + snapshotUUIDs;
                new Snapshot(repository, new SnapshotId(snapshotNamePattern, snapshotUUIDs.get(0))).writeTo(out);
            } else {
                out.writeString(repository);
                out.writeString(snapshotNamePattern);
                out.writeOptionalStringArray(snapshotUUIDs.toArray(Strings.EMPTY_ARRAY));
            }
            out.writeVLong(startTime);
            out.writeLong(repositoryStateId);
        }

        @Override
        public String repository() {
            return repository;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }
    }
}
