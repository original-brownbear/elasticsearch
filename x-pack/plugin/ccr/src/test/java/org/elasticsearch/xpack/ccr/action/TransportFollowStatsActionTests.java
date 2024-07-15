/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportFollowStatsActionTests extends ESTestCase {

    public void testFindFollowerIndicesFromShardFollowTasks() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        IndexMetadata index1 = IndexMetadata.builder("index1").settings(indexSettings).build();
        IndexMetadata index2 = IndexMetadata.builder("index2").settings(indexSettings).build();
        IndexMetadata index3 = IndexMetadata.builder("index3").settings(indexSettings).build();

        PersistentTasksCustomMetadata.Builder persistentTasks = PersistentTasksCustomMetadata.builder()
            .addTask("1", ShardFollowTask.NAME, createShardFollowTask(index1.getIndex()), null)
            .addTask("2", ShardFollowTask.NAME, createShardFollowTask(index2.getIndex()), null)
            .addTask("3", ShardFollowTask.NAME, createShardFollowTask(index3.getIndex()), null);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, persistentTasks.build())
                    // only add index1 and index2
                    .put(index1, false)
                    .put(index2, false)
                    .build()
            )
            .build();
        Set<String> result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState, null);
        assertThat(result.size(), equalTo(2));
        Index index7 = index1.getIndex();
        assertThat(result.contains(index7.name()), is(true));
        Index index6 = index2.getIndex();
        assertThat(result.contains(index6.name()), is(true));

        Index index5 = index2.getIndex();
        result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState, new String[] { index5.name() });
        assertThat(result.size(), equalTo(1));
        Index index4 = index2.getIndex();
        assertThat(result.contains(index4.name()), is(true));

        Index index = index3.getIndex();
        result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState, new String[] { index.name() });
        assertThat(result.size(), equalTo(0));
    }

    static ShardFollowTask createShardFollowTask(Index followerIndex) {
        return new ShardFollowTask(
            null,
            new ShardId(followerIndex, 0),
            new ShardId("leader_index", "", 0),
            1024,
            1024,
            1,
            1,
            TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE,
            TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE,
            10240,
            new ByteSizeValue(512, ByteSizeUnit.MB),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueMillis(10),
            Collections.emptyMap()
        );
    }

}
