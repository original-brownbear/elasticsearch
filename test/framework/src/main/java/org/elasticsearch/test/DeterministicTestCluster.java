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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

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

    protected abstract DisruptableMockTransport.ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination);

    protected abstract Stream<? extends DeterministicNode> allNodes();

    public static String getNodeIdForLogContext(DiscoveryNode node) {
        return "{" + node.getId() + "}{" + node.getEphemeralId() + "}";
    }

    public abstract static class DeterministicNode implements Releasable {

        protected final int nodeIndex;

        public final Settings nodeSettings;

        public final ClusterSettings clusterSettings;

        public final DiscoveryNode localNode;

        protected final Logger logger;

        public Coordinator coordinator;

        public DisruptableMockTransport mockTransport;

        public ClusterService clusterService;

        public TransportService transportService;

        public DeterministicNode(int nodeIndex, Settings nodeSettings, DiscoveryNode localNode, Logger logger) {
            this.nodeIndex = nodeIndex;
            this.nodeSettings = nodeSettings;
            this.localNode = localNode;
            clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            this.logger = logger;
        }

        public String getId() {
            return localNode.getId();
        }

        protected abstract Runnable onNode(Runnable runnable);

        protected static DisruptableMockTransport mockTransport(DeterministicNode node, Supplier<DeterministicTestCluster> cluster) {
            return new DisruptableMockTransport(node.localNode, node.logger) {
                @Override
                protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                    return cluster.get().getConnectionStatus(getLocalNode(), destination);
                }

                @Override
                protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                    return cluster.get().allNodes().map(cn -> cn.mockTransport)
                        .filter(transport -> transport.getLocalNode().getAddress().equals(address))
                        .findAny();
                }

                @Override
                protected void execute(Runnable runnable) {
                    cluster.get().deterministicTaskQueue.scheduleNow(node.onNode(runnable));
                }

                @Override
                protected NamedWriteableRegistry writeableRegistry() {
                    return ESDeterministicTestCase.namedWriteableRegistry;
                }
            };
        }
    }
}
