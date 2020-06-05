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
package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsIT extends AbstractSnapshotIntegTestCase {

    public void testLongRunningSnapshotAllowsConcurrentSnapshot() throws InterruptedException, ExecutionException {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        blockDataNode(repoName, dataNode);

        final String indexSlow = "index-slow";
        createIndex(indexSlow, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(indexSlow);
        indexDoc(indexSlow, "some_id", "foo", "bar");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                client().admin().cluster().prepareCreateSnapshot(repoName, "slow-snapshot").setWaitForCompletion(true).execute();

        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String indexFast = "index-fast";
        createIndex(indexFast, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", dataNode2)
                .put("index.routing.allocation.exclude._name", dataNode).build());
        ensureGreen(indexFast);
        indexDoc(indexFast, "some_id", "foo", "bar");

        final CreateSnapshotResponse fastSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, "fast-snapshot")
                .setIndices(indexFast).setWaitForCompletion(true).execute().get();

        assertThat(fastSnapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));

        assertThat(createSlowFuture.isDone(), is(false));
        unblockNode(repoName, dataNode);

        final CreateSnapshotResponse slowSnapshotResponse = createSlowFuture.get();
        assertThat(slowSnapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
    }
}
