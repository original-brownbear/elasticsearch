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

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ForceMergeIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(InternalSettingsPlugin.class);
    }

    public void testForceMergeUUIDConsistent() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String index = "test-index";
        createIndex(index,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureGreen(index);
        final ClusterState state = clusterService().state();
        final IndexRoutingTable indexShardRoutingTables = state.routingTable().getIndicesRouting().get(index);
        final IndexShardRoutingTable shardRouting = indexShardRoutingTables.getShards().get(0);
        final String primaryNodeId = shardRouting.primaryShard().currentNodeId();
        final String replicaNodeId = shardRouting.replicaShards().get(0).currentNodeId();
        final Index idx = shardRouting.primaryShard().index();
        final IndicesService primaryIndicesService =
            internalCluster().getInstance(IndicesService.class, state.nodes().get(primaryNodeId).getName());
        final IndicesService replicaIndicesService = internalCluster().getInstance(IndicesService.class,
            state.nodes().get(replicaNodeId).getName());
        final IndexShard primary = primaryIndicesService.indexService(idx).getShard(0);
        final IndexShard replica = replicaIndicesService.indexService(idx).getShard(0);

        assertThat(getForceMergeUUID(primary), nullValue());
        assertThat(getForceMergeUUID(replica), nullValue());

        final ForceMergeResponse forceMergeResponse =
            client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();

        assertThat(forceMergeResponse.getFailedShards(), is(0));
        assertThat(forceMergeResponse.getSuccessfulShards(), is(2));

        // Force flush to force a new commit that contains the force flush UUID
        final FlushResponse flushResponse = client().admin().indices().prepareFlush(index).setForce(true).get();
        assertThat(flushResponse.getFailedShards(), is(0));
        assertThat(flushResponse.getSuccessfulShards(), is(2));

        final String primaryForceMergeUUID = getForceMergeUUID(primary);
        assertThat(primaryForceMergeUUID, notNullValue());

        final String replicaForceMergeUUID = getForceMergeUUID(replica);
        assertThat(replicaForceMergeUUID, notNullValue());
        assertThat(primaryForceMergeUUID, is(replicaForceMergeUUID));
    }

    @TestIssueLogging(issueUrl = "foo", value = "_root:TRACE")
    public void testSyncRetentionLeasesBeforeForceMerge() throws InterruptedException, IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String index = "test-index";
        createIndex(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                //.put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                //.put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                // TODO: How does setting this break cleaning up all the old data
                // A: Messes with the logic for refreshing in org.elasticsearch.index.shard.IndexShard.scheduledRefresh
                //.put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
                //.put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.timeValueSeconds(10))
                .build());
        ensureGreen(index);
        final long sleep = 5L;
        final ClusterState state = clusterService().state();
        final IndexRoutingTable indexShardRoutingTables = state.routingTable().getIndicesRouting().get(index);
        final IndexShardRoutingTable shardRouting = indexShardRoutingTables.getShards().get(0);
        final String primaryNodeId = shardRouting.primaryShard().currentNodeId();
        final String replicaNodeId = shardRouting.replicaShards().get(0).currentNodeId();
        final String primaryNodeName = state.nodes().get(primaryNodeId).getName();
        final String replicaNodeName = state.nodes().get(replicaNodeId).getName();
        final Index idx = shardRouting.primaryShard().index();
        final IndexService indexServicePrimary = internalCluster().getInstance(IndicesService.class, primaryNodeName).indexService(idx);
        final IndexService indexServiceReplica = internalCluster().getInstance(IndicesService.class, replicaNodeName).indexService(idx);
        final Store.MetadataSnapshot primaryMetaDataEmpty = indexServicePrimary.getShard(0).snapshotStoreMetadata();
        indexRandom(true, client().prepareIndex(index).setId("doc-1").setSource("foo", "bar"));
        indexRandom(true, client().prepareIndex(index).setId("doc-2").setSource("foo", "blub"));
        client().prepareDelete().setIndex(index).setId("doc-2").get();
        final Store.MetadataSnapshot primaryMetaDataBeforeFlush = indexServicePrimary.getShard(0).snapshotStoreMetadata();
        client().admin().indices().prepareFlush(index).setWaitIfOngoing(true).setForce(true).get();
        final Store.MetadataSnapshot primaryMetaDataAfterFlush = indexServicePrimary.getShard(0).snapshotStoreMetadata();
        logger.info("--> flushed");

        TimeUnit.SECONDS.sleep(sleep);

        logger.info("--> force merge 1");
        client().admin().indices().prepareFlush(index).setWaitIfOngoing(true).setForce(true).get();
        final Store.MetadataSnapshot primaryMetaDataAfterWait = indexServicePrimary.getShard(0).snapshotStoreMetadata();
        client().admin().indices().prepareForceMerge(index).setFlush(true).setMaxNumSegments(1).get();
        flushAndRefresh(index);
        client().admin().indices().prepareFlush(index).setWaitIfOngoing(true).setForce(true).get();
        final List<Segment> first = indexServicePrimary.getShard(0).segments(true);
        final Store.MetadataSnapshot primaryMetaDataAfterForceMerge = indexServicePrimary.getShard(0).snapshotStoreMetadata();
        final ImmutableOpenMap<String, Long> sizeBefore = client().admin().indices().prepareStats(index).setSegments(true)
                .setIncludeSegmentFileSizes(true).get().getPrimaries().segments.getFileSizes();

        TimeUnit.SECONDS.sleep(sleep);

        logger.info("--> force merge 2");
        flushAndRefresh(index);
        client().admin().indices().prepareFlush(index).setWaitIfOngoing(true).setForce(true).get();
        client().admin().indices().prepareForceMerge(index).setFlush(true).setMaxNumSegments(1).get();
        final List<Segment> second = indexServicePrimary.getShard(0).segments(true);
        flushAndRefresh(index);
        final Store.MetadataSnapshot primaryMetaDataAfterSecondForceMerge = indexServicePrimary.getShard(0).snapshotStoreMetadata();
        final ImmutableOpenMap<String, Long> sizeAfter = client().admin().indices().prepareStats(index).setSegments(true)
                .setIncludeSegmentFileSizes(true).get().getPrimaries().segments.getFileSizes();

        assertEquals(sizeBefore, sizeAfter);
    }

    private static String getForceMergeUUID(IndexShard indexShard) throws IOException {
        try (Engine.IndexCommitRef indexCommitRef = indexShard.acquireLastIndexCommit(true)) {
            return indexCommitRef.getIndexCommit().getUserData().get(Engine.FORCE_MERGE_UUID_KEY);
        }
    }
}
