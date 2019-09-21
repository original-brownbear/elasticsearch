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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.coordination.MockSinglePrioritizingExecutor;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.env.NodeEnvironment;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class ESDeterministicTestCase extends ESTestCase {

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
}
