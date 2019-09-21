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
package org.elasticsearch.test;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

public abstract class DeterministicTestCluster implements Releasable {

    public final Set<String> disconnectedNodes = new HashSet<>();

    public final DeterministicTaskQueue deterministicTaskQueue;

    public DeterministicTestCluster(Random random) {
        deterministicTaskQueue = new DeterministicTaskQueue(
            // TODO does ThreadPool need a node name any more?
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "deterministic-task-queue").build(), random);
    }

    public static Runnable onNodeLog(DiscoveryNode node, Runnable runnable) {
        final String nodeId = getNodeIdForLogContext(node);
        return new Runnable() {
            @Override
            public void run() {
                try (CloseableThreadContext.Instance ignored =
                         CloseableThreadContext.put(AbstractCoordinatorTestCase.NODE_ID_LOG_CONTEXT_KEY, nodeId)) {
                    runnable.run();
                }
            }

            @Override
            public String toString() {
                return nodeId + ": " + runnable.toString();
            }
        };
    }

    public static String getNodeIdForLogContext(DiscoveryNode node) {
        return "{" + node.getId() + "}{" + node.getEphemeralId() + "}";
    }

    public static class DeterministicNode {

        public final Settings nodeSettings;

        public final ClusterSettings clusterSettings;

        public final DiscoveryNode localNode;

        protected final Logger logger;

        public Coordinator coordinator;

        public DisruptableMockTransport mockTransport;

        public ClusterService clusterService;

        public TransportService transportService;

        public DeterministicNode(Settings nodeSettings, DiscoveryNode localNode, Logger logger) {
            this.nodeSettings = nodeSettings;
            this.localNode = localNode;
            clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            this.logger = logger;
        }
    }
}
