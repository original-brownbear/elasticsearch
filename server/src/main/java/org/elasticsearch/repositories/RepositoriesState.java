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
package org.elasticsearch.repositories;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class RepositoriesState extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final Version REPO_GEN_IN_CS_VERSION = Version.V_8_0_0;

    public static final String TYPE = "repositories";

    private final Map<String, State> states;

    private RepositoriesState(Map<String, State> states) {
        this.states = states;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return REPO_GEN_IN_CS_VERSION;
    }

    @Override
    public String getWriteableName() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class State {

        private final long generation;

        private final boolean pendingUpdate;

        private State(long generation, boolean pendingUpdate) {
            this.generation = generation;
            this.pendingUpdate = pendingUpdate;
        }
    }

    public static final class Builder {

        private final Map<String, State> stateMap = new HashMap<>();

        private Builder() {
        }

        public Builder putAll(RepositoriesState state) {
            // TODO
            return this;
        }

        public Builder putState(String name, State state) {
            // TODO
            return this;
        }

    }
}
