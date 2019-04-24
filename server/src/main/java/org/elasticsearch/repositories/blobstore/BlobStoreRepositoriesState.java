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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class BlobStoreRepositoriesState extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {
    public static final String TYPE = "blobstore_repositories";

    private final Map<String, Entry> repos;

    public BlobStoreRepositoriesState(Map<String, Entry> repos) {
        this.repos = repos;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_0_0;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    public Entry repo(String name) {
        return repos.get(name);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(List.copyOf(repos.values()));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) {
        return null;
    }

    public static final class Entry implements Writeable {

        private final long nextVersion;

        private final boolean metaUploaded;

        public Entry(long nextVersion, boolean metaUploaded) {
            this.nextVersion = nextVersion;
            this.metaUploaded = metaUploaded;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(nextVersion);
            out.writeBoolean(metaUploaded);
        }
    }
}
