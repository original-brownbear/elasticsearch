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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.coordination.MockSinglePrioritizingExecutor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;

public class ESDeterministicTestCase extends ESTestCase {

    protected static final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Stream.concat(
        ClusterModule.getNamedWriteables().stream(), NetworkModule.getNamedWriteables().stream()).collect(Collectors.toList()));

    protected final List<NodeEnvironment> nodeEnvironments = new ArrayList<>();

    protected final AtomicInteger nextNodeIndex = new AtomicInteger();

    @After
    public void closeNodeEnvironmentsAfterEachTest() {
        for (NodeEnvironment nodeEnvironment : nodeEnvironments) {
            nodeEnvironment.close();
        }
        nodeEnvironments.clear();
    }

    @Before
    public void resetNodeIndexBeforeEachTest() {
        nextNodeIndex.set(0);
    }

    /**
     * How to behave with a new cluster state
     */
    public enum ClusterStateApplyResponse {
        /**
         * Apply the state (default)
         */
        SUCCEED,

        /**
         * Reject the state with an exception.
         */
        FAIL,

        /**
         * Never respond either way.
         */
        HANG,
    }

    protected TransportInterceptor getTransportInterceptor(DiscoveryNode localNode, ThreadPool threadPool) {
        return NOOP_TRANSPORT_INTERCEPTOR;
    }

    public static String getNodeIdForLogContext(DiscoveryNode node) {
        return "{" + node.getId() + "}{" + node.getEphemeralId() + "}";
    }

    public static class DisruptableClusterApplierService extends ClusterApplierService {
        private final String nodeName;
        private final DeterministicTaskQueue deterministicTaskQueue;
        public AbstractCoordinatorTestCase.ClusterStateApplyResponse clusterStateApplyResponse =
            ClusterStateApplyResponse.SUCCEED;

        public DisruptableClusterApplierService(String nodeName, Settings settings, ClusterSettings clusterSettings,
            DeterministicTaskQueue deterministicTaskQueue, Function<Runnable, Runnable> runnableWrapper) {
            super(nodeName, settings, clusterSettings, deterministicTaskQueue.getThreadPool(runnableWrapper));
            this.nodeName = nodeName;
            this.deterministicTaskQueue = deterministicTaskQueue;
            addStateApplier(event -> {
                switch (clusterStateApplyResponse) {
                    case SUCCEED:
                    case HANG:
                        final ClusterState oldClusterState = event.previousState();
                        final ClusterState newClusterState = event.state();
                        assert oldClusterState.version() <= newClusterState.version() : "updating cluster state from version "
                            + oldClusterState.version() + " to stale version " + newClusterState.version();
                        break;
                    case FAIL:
                        throw new ElasticsearchException("simulated cluster state applier failure");
                }
            });
        }

        @Override
        protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
            return new MockSinglePrioritizingExecutor(nodeName, deterministicTaskQueue);
        }

        @Override
        public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener) {
            if (clusterStateApplyResponse == ClusterStateApplyResponse.HANG) {
                if (randomBoolean()) {
                    // apply cluster state, but don't notify listener
                    super.onNewClusterState(source, clusterStateSupplier, (source1, e) -> {
                        // ignore result
                    });
                }
            } else {
                super.onNewClusterState(source, clusterStateSupplier, listener);
            }
        }

        @Override
        protected void connectToNodesAndWait(ClusterState newClusterState) {
            // don't do anything, and don't block
        }
    }

    public static class AckedFakeThreadPoolMasterService extends FakeThreadPoolMasterService {

        public AbstractCoordinatorTestCase.AckCollector nextAckCollector = new AbstractCoordinatorTestCase.AckCollector();

        public AckedFakeThreadPoolMasterService(String nodeName, String serviceName, Consumer<Runnable> onTaskAvailableToRun) {
            super(nodeName, serviceName, onTaskAvailableToRun);
        }

        @Override
        protected ClusterStatePublisher.AckListener wrapAckListener(ClusterStatePublisher.AckListener ackListener) {
            final AbstractCoordinatorTestCase.AckCollector ackCollector = nextAckCollector;
            nextAckCollector = new AbstractCoordinatorTestCase.AckCollector();
            return new ClusterStatePublisher.AckListener() {
                @Override
                public void onCommit(TimeValue commitTime) {
                    ackCollector.onCommit(commitTime);
                    ackListener.onCommit(commitTime);
                }

                @Override
                public void onNodeAck(DiscoveryNode node, Exception e) {
                    ackCollector.onNodeAck(node, e);
                    ackListener.onNodeAck(node, e);
                }
            };
        }
    }

    public abstract class DeterministicTestCluster implements Releasable {

        public final Set<String> disconnectedNodes = new HashSet<>();

        public final Set<String> blackholedNodes = new HashSet<>();

        public final DeterministicTaskQueue deterministicTaskQueue;

        public DeterministicTestCluster(Random random) {
            deterministicTaskQueue = new DeterministicTaskQueue(
                // TODO does ThreadPool need a node name any more?
                Settings.builder().put(NODE_NAME_SETTING.getKey(), "deterministic-task-queue").build(), random);
        }

        public Runnable onNodeLog(DiscoveryNode node, Runnable runnable) {
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

        public abstract class DeterministicNode implements Releasable {

            protected final int nodeIndex;

            public final Settings nodeSettings;

            public final ClusterSettings clusterSettings;

            public final DiscoveryNode localNode;

            protected final Logger logger;

            public Coordinator coordinator;

            protected final DisruptableMockTransport mockTransport;

            protected DisruptableClusterApplierService clusterApplierService;

            public final ClusterService clusterService;

            public final TransportService transportService;

            public final AckedFakeThreadPoolMasterService masterService;

            public DeterministicNode(int nodeIndex, Settings nodeSettings, DiscoveryNode localNode, Logger logger) {
                this.nodeIndex = nodeIndex;
                this.nodeSettings = nodeSettings;
                this.localNode = localNode;
                clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                this.logger = logger;
                mockTransport = mockTransport(this);
                transportService = mockTransport.createTransportService(
                    nodeSettings, deterministicTaskQueue.getThreadPool(this::onNode),
                    getTransportInterceptor(localNode, deterministicTaskQueue.getThreadPool(this::onNode)),
                    a -> localNode, null, emptySet());
                clusterApplierService = new DisruptableClusterApplierService(localNode.getId(), nodeSettings, clusterSettings,
                    deterministicTaskQueue, this::onNode);
                masterService = new AckedFakeThreadPoolMasterService(localNode.getId(), "test",
                    runnable -> deterministicTaskQueue.scheduleNow(onNode(runnable)));
                clusterService = new ClusterService(nodeSettings, clusterSettings, masterService, clusterApplierService);
                clusterService.setNodeConnectionsService(
                    new NodeConnectionsService(clusterService.getSettings(), deterministicTaskQueue.getThreadPool(this::onNode),
                        transportService));
            }

            public String getId() {
                return localNode.getId();
            }

            protected abstract Runnable onNode(Runnable runnable);

            public boolean isNotUsefullyBootstrapped() {
                return localNode.isMasterNode() == false || coordinator.isInitialConfigurationSet() == false;
            }

            private DisruptableMockTransport mockTransport(DeterministicNode node) {
                return new DisruptableMockTransport(node.localNode, node.logger) {
                    @Override
                    protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                        return DeterministicTestCluster.this.getConnectionStatus(getLocalNode(), destination);
                    }

                    @Override
                    protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                        return DeterministicTestCluster.this.allNodes().map(cn -> cn.mockTransport)
                            .filter(transport -> transport.getLocalNode().getAddress().equals(address))
                            .findAny();
                    }

                    @Override
                    protected void execute(Runnable runnable) {
                        DeterministicTestCluster.this.deterministicTaskQueue.scheduleNow(node.onNode(runnable));
                    }

                    @Override
                    protected NamedWriteableRegistry writeableRegistry() {
                        return namedWriteableRegistry;
                    }
                };
            }
        }
    }
}
