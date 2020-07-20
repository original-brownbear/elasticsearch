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

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepair;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.hamcrest.Matchers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class BlobStoreRepairIT extends AbstractSnapshotIntegTestCase {

    public void testRebuildMissingShardGenerationOldFormat() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);
        final String oldVersionSnapshot = initWithSnapshotVersion(repoName, repoPath, SnapshotsService.OLD_SNAPSHOT_FORMAT);

        logger.info("--> recreating repository to clear caches");
        client().admin().cluster().prepareDeleteRepository(repoName).get();
        createRepository(repoName, "fs", repoPath);

        final String indexName = "test-index";
        createIndex(indexName);

        createFullSnapshot(repoName, "snapshot-1");

        logger.info("--> delete old version snapshot");
        client().admin().cluster().prepareDeleteSnapshot(repoName, oldVersionSnapshot).get();

        final IndexId indexId = getRepositoryData(repoName).resolveIndexId(indexName);
        final Path shardPath = repoPath.resolve("indices").resolve(indexId.getId()).resolve("0");
        final Path initialShardMetaPath = shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + "0");
        assertFileExists(initialShardMetaPath);
        Files.delete(initialShardMetaPath);

        final BlobStoreRepository repository = (BlobStoreRepository) internalCluster().getMasterNodeInstance(RepositoriesService.class)
                .repository(repoName);
        final BlobStoreRepair.CheckResult checkResult = PlainActionFuture.get(f -> BlobStoreRepair.check(repository, f));
        final List<BlobStoreRepair.ShardLevelSnapshotIssue> shardLevelSnapshotIssues = checkResult.shardLevelSnapshotIssues();
        assertThat(shardLevelSnapshotIssues, Matchers.hasSize(1));
        final BlobStoreRepair.ShardLevelSnapshotIssue foundIssue = shardLevelSnapshotIssues.get(0);
        assertThat(foundIssue, is(BlobStoreRepair.ShardLevelIssueType.MISSING_INDEX_GENERATION));

        final BlobStoreRepair.CheckResult repairResult =
                PlainActionFuture.get(f -> BlobStoreRepair.executeFixes(repository, checkResult, f));
        assertThat(repairResult.shardLevelSnapshotIssues(), empty());
        assertThat(repairResult.rootLevelSnapshotIssues(), empty());
    }
}
