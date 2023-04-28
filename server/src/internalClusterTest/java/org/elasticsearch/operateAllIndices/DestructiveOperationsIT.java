/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operateAllIndices;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class DestructiveOperationsIT extends ESIntegTestCase {

    @After
    public void afterTest() {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), (String) null));
    }

    public void testDeleteIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));
        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(admin().indices().prepareDelete("1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(admin().indices().prepareDelete("*", "-*").get());

        expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareDelete("i*").get());
        expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareDelete("_all").get());
    }

    public void testDeleteIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(admin().indices().prepareDelete("_all").get());
        } else {
            assertAcked(admin().indices().prepareDelete("*").get());
        }

        assertThat(indexExists("_all"), equalTo(false));
    }

    public void testCloseIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(admin().indices().prepareClose("1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(admin().indices().prepareClose("*", "-*").get());

        expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareClose("i*").get());
        expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareClose("_all").get());
    }

    public void testCloseIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(admin().indices().prepareClose("_all").get());
        } else {
            assertAcked(admin().indices().prepareClose("*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        for (Map.Entry<String, IndexMetadata> indexMetadataEntry : state.getMetadata().indices().entrySet()) {
            assertEquals(IndexMetadata.State.CLOSE, indexMetadataEntry.getValue().getState());
        }
    }

    public void testOpenIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");
        assertAcked(admin().indices().prepareClose("1index", "index1").get());

        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(admin().indices().prepareOpen("*", "-*").get());

        expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareOpen("i*").get());
        expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareOpen("_all").get());
    }

    public void testOpenIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");
        assertAcked(admin().indices().prepareClose("1index", "index1").get());

        if (randomBoolean()) {
            assertAcked(admin().indices().prepareOpen("_all").get());
        } else {
            assertAcked(admin().indices().prepareOpen("*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        for (Map.Entry<String, IndexMetadata> indexMetadataEntry : state.getMetadata().indices().entrySet()) {
            assertEquals(IndexMetadata.State.OPEN, indexMetadataEntry.getValue().getState());
        }
    }

    public void testAddIndexBlockIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(admin().indices().prepareAddBlock(WRITE, "1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(admin().indices().prepareAddBlock(WRITE, "*", "-*").get());

        expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareAddBlock(WRITE, "i*").get());
        expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareAddBlock(WRITE, "_all").get());
    }

    public void testAddIndexBlockDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(admin().indices().prepareAddBlock(WRITE, "_all").get());
        } else {
            assertAcked(admin().indices().prepareAddBlock(WRITE, "*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        assertTrue("write block is set on index1", state.getBlocks().hasIndexBlock("index1", IndexMetadata.INDEX_WRITE_BLOCK));
        assertTrue("write block is set on 1index", state.getBlocks().hasIndexBlock("1index", IndexMetadata.INDEX_WRITE_BLOCK));
    }
}
