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

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CorruptedBlobStoreRepositoryIT extends AbstractSnapshotIntegTestCase {

    public void testConcurrentlyChangeRepositoryContents() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
            .setType("fs").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData =
            getRepositoryData(internalCluster().getMasterNodeInstance(RepositoriesService.class).repository("test-repo"));
        Files.move(repo.resolve("index-" + repositoryData.getGenId()), repo.resolve("index-" + (repositoryData.getGenId() + 1)));

        assertRepositoryBlocked(client, "test-repo", "test-snap-1");

        if (randomBoolean()) {
            logger.info("--> move index-N blob back to initial generation");
            Files.move(repo.resolve("index-" + (repositoryData.getGenId() + 1)), repo.resolve("index-" + repositoryData.getGenId()));

            logger.info("--> verify repository remains blocked");
            assertRepositoryBlocked(client, "test-repo", "test-snap-1");
        }

        logger.info("--> remove repository");
        assertAcked(client.admin().cluster().prepareDeleteRepository("test-repo"));

        logger.info("--> recreate repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
            .setType("fs").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        logger.info("--> delete snapshot");
        client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(SnapshotMissingException.class, () -> client.admin().cluster().prepareGetSnapshots("test-repo")
            .addSnapshots("test-snap-1").get().getSnapshots("test-repo"));
    }

    private void assertRepositoryBlocked(Client client, String repo, String existingSnapshot) {

        logger.info("--> try to delete snapshot");
        final RepositoryException repositoryException3 = expectThrows(RepositoryException.class,
            () -> client.admin().cluster().prepareDeleteSnapshot(repo, existingSnapshot).execute().actionGet());
        assertThat(repositoryException3.getMessage(),
            containsString("Could not read repository data because the contents of the repository do not match with its expected state."));

        logger.info("--> try to create snapshot");
        final RepositoryException repositoryException4 = expectThrows(RepositoryException.class,
            () -> client.admin().cluster().prepareCreateSnapshot(repo, existingSnapshot).execute().actionGet());
        assertThat(repositoryException4.getMessage(),
            containsString("Could not read repository data because the contents of the repository do not match with its expected state."));
    }
}
