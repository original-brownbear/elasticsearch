/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;

import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AutoExpandReplicasIT extends ESIntegTestCase {

    public void testConflictingUpdateAutoExpandReplicaSettings() {
        internalCluster().startNode(
            Settings.builder().put(NodeRoles.addRoles(Settings.EMPTY, Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)))
        );
        assertAcked(
            admin().indices().prepareCreate("test-idx").setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
                    .put(AutoExpandReplicas.SETTING.getKey(), "0-1"))
        );
        ensureGreen("test-idx");
        assertAcked(
            admin().indices()
                .prepareUpdateSettings("test-idx")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2))
        );
        ensureGreen("test-idx");
    }
}
