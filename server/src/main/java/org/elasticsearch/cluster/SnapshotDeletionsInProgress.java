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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
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

    public static final Version SNAPSHOT_DELETE_STATE_VERSION = Version.V_8_0_0;

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
                builder.field("snapshot", entry.snapshotName);
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
            builder.append(entries.get(i).snapshotName);
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

        private final String repo;

        private final String snapshotName;

        @Nullable
        private final String snapshotUUID;

        private final State state;

        private final long startTime;

        private final long repositoryStateId;

        public Entry(Snapshot snapshot, long startTime, long repositoryStateId) {
            this(startTime, repositoryStateId, State.UPDATE_META, snapshot.getRepository(), snapshot.getSnapshotId().getName(),
                snapshot.getSnapshotId().getUUID());
        }

        private Entry(long startTime, long repositoryStateId, State state, String repo, String snapshotName, String snapshotUUID) {
            this.startTime = startTime;
            this.repositoryStateId = repositoryStateId;
            this.snapshotName = snapshotName;
            this.repo = repo;
            this.state = state;
            this.snapshotUUID = snapshotUUID;
        }

        public Entry(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(SNAPSHOT_DELETE_STATE_VERSION)) {
                repo = in.readString();
                snapshotName = in.readString();
                state = State.fromValue(in.readByte());
                snapshotUUID = null;
            } else {
                final Snapshot snapshot = new Snapshot(in);
                repo = snapshot.getRepository();
                snapshotName = snapshot.getSnapshotId().getName();
                snapshotUUID = snapshot.getSnapshotId().getUUID();
                state = State.UPDATE_META;
            }
            this.startTime = in.readVLong();
            this.repositoryStateId = in.readLong();
        }

        /**
         * The snapshot to delete.
         */
        public Snapshot getSnapshot() {
            return snapshot;
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
            return startTime == that.startTime
                && repositoryStateId == that.repositoryStateId
                && repo.equals(that.repo)
                && snapshotName.equals(that.snapshotName)
                && snapshotUUID.equals(that.snapshotUUID)
                && state == that.state;
        }

        @Override
        public int hashCode() {
            return Objects.hash(startTime, repositoryStateId, repo, snapshotName, snapshotUUID, state);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(SNAPSHOT_DELETE_STATE_VERSION)) {

            } else {
                new Snapshot(repo, new SnapshotId(snapshotName, snapshotUUID)).writeTo(out);
            }
            out.writeVLong(startTime);
            out.writeLong(repositoryStateId);
        }

        public String snapshotName() {
            return snapshotName;
        }

        @Override
        public String repository() {
            return repo;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }
    }

    public enum State {

        INIT((byte) 0),
        UPDATE_META((byte) 1),
        CLEANUP((byte) 2);

        private final byte value;

        State(byte value) {
            this.value = value;
        }

        public byte value() {
            return value;
        }

        public static State fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return UPDATE_META;
                case 2:
                    return CLEANUP;
                default:
                    throw new IllegalArgumentException("No snapshot deletion state for value [" + value + "]");
            }
        }
    }
}
