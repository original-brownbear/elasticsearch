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

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class BlobStoreRepair {

    private static final Logger logger = LogManager.getLogger(BlobStoreRepair.class);

    private static final String MISSING_SHARD_LEVEL_META_GENERATION = "-1";

    public static void check(BlobStoreRepository repository, ActionListener<CheckResult> listener) {
        repository.threadPool().generic().execute(ActionRunnable.supply(listener, () -> {
            final BlobContainer rootContainer = repository.blobContainer();
            final long[] generations = findGenerations(rootContainer);
            if (generations.length == 0) {
                logger.info("Did not find any root level repository metadata in [{}]", rootContainer);
                return new CheckResult(RepositoryData.UNKNOWN_REPO_GEN, Collections.emptyList(), Collections.emptyList());
            }
            final long generation = generations[generations.length - 1];
            final RepositoryData repositoryData;
            try (InputStream inputStream = rootContainer.readBlob(BlobStoreRepository.INDEX_FILE_PREFIX + generation)) {
                repositoryData = RepositoryData.snapshotsFromXContent(JsonXContent.jsonXContent.createParser(
                        new NamedXContentRegistry(Collections.emptyList()), DeprecationHandler.IGNORE_DEPRECATIONS, inputStream),
                        generation, false);
            }
            final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
            final Collection<String> snapshotUUIDsFromSnapBlobs =
                    findSnapshotUUIDsByPrefix(rootContainer, BlobStoreRepository.SNAPSHOT_PREFIX);
            final Collection<String> snapshotUUIDsFromMetaBlobs =
                    findSnapshotUUIDsByPrefix(rootContainer, BlobStoreRepository.METADATA_PREFIX);
            final List<RootLevelSnapshotIssue> rootLevelSnapshotIssues = new ArrayList<>();
            final List<ShardLevelSnapshotIssue> shardLevelSnapshotIssues = new ArrayList<>();
            for (SnapshotId snapshotId : snapshotIds) {
                final String uuid = snapshotId.getUUID();
                if (snapshotUUIDsFromMetaBlobs.contains(uuid) == false) {
                    rootLevelSnapshotIssues.add(new RootLevelSnapshotIssue("Missing meta- blob for [" + uuid + "]"));
                }
                if (snapshotUUIDsFromSnapBlobs.contains(uuid) == false) {
                    rootLevelSnapshotIssues.add(new RootLevelSnapshotIssue("Missing snap- blob for [" + uuid + "]"));
                }
                final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                for (String index : snapshotInfo.indices()) {
                    final IndexId indexId = repositoryData.resolveIndexId(index);
                    IndexMetadata indexMetadata =
                            repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId);
                    final int shards = indexMetadata.getNumberOfShards();
                    for (int i = 0; i < shards; i++) {
                        final String shardGeneration = repositoryData.shardGenerations().getShardGen(indexId, i);
                        final BlobContainer shardContainer = repository.shardContainer(indexId, i);
                        final Set<String> blobsInShard = shardContainer.listBlobs().keySet();
                        final Tuple<BlobStoreIndexShardSnapshots, String> shardLevelMeta = repository.buildBlobStoreIndexShardSnapshots(
                                blobsInShard, shardContainer, shardGeneration);
                        final String foundGeneration = shardLevelMeta.v2();
                        if (MISSING_SHARD_LEVEL_META_GENERATION.equals(foundGeneration)) {
                            logger.debug("Found missing shard level metadata for [{}][{}]", indexId, i);
                            shardLevelSnapshotIssues.add(
                                    new ShardLevelSnapshotIssue(ShardLevelIssueType.MISSING_INDEX_GENERATION, indexId, i, snapshotId));
                        }
                    }
                }
            }
            return new CheckResult(generation, shardLevelSnapshotIssues, rootLevelSnapshotIssues);
        }));
    }

    public static void executeFixes(BlobStoreRepository repository, CheckResult issues, ActionListener<CheckResult> listener) {
        repository.threadPool().generic().execute(ActionRunnable.wrap(listener, l -> {
            logger.info("Repairing the issues for repository [{}][{}]", repository.metadata.name(), issues);
            for (RootLevelSnapshotIssue rootLevelSnapshotIssue : issues.rootLevelSnapshotIssues()) {
                logger.info("Could not fix issue [{}]", rootLevelSnapshotIssue);
            }
            for (ShardLevelSnapshotIssue shardLevelSnapshotIssue : issues.shardLevelSnapshotIssues()) {
                switch (shardLevelSnapshotIssue.type) {
                    case MISSING_INDEX_GENERATION:

                        break;
                    default:
                        logger.info("Could not fix issue [{}]", shardLevelSnapshotIssue);
                }
            }
            check(repository, l);
        }));
    }

    public static Collection<String> findSnapshotUUIDsByPrefix(BlobContainer blobContainer, String prefix) throws IOException {
        return blobContainer.listBlobs().keySet().stream().filter(p -> p.startsWith(prefix))
                .map(p -> p.replace(prefix, "").replace(".dat", ""))
                .collect(Collectors.toSet());
    }

    public static long[] findGenerations(BlobContainer rootContainer) throws IOException {
        return rootContainer.listBlobsByPrefix(BlobStoreRepository.INDEX_FILE_PREFIX).keySet().stream()
                .map(s -> s.replace(BlobStoreRepository.INDEX_FILE_PREFIX, ""))
                .mapToLong(Long::parseLong).sorted().toArray();
    }

    public static final class CheckResult implements ToXContentObject {

        private final long repositoryGeneration;

        private final List<ShardLevelSnapshotIssue> shardLevelSnapshotIssues;

        private final List<RootLevelSnapshotIssue> rootLevelSnapshotIssues;

        private CheckResult(long repositoryGeneration, List<ShardLevelSnapshotIssue> shardLevelSnapshotIssues,
                            List<RootLevelSnapshotIssue> rootLevelSnapshotIssues) {
            this.repositoryGeneration = repositoryGeneration;
            this.shardLevelSnapshotIssues = shardLevelSnapshotIssues;
            this.rootLevelSnapshotIssues = rootLevelSnapshotIssues;
        }

        public long repositoryGeneration() {
            return repositoryGeneration;
        }

        public List<RootLevelSnapshotIssue> rootLevelSnapshotIssues() {
            return rootLevelSnapshotIssues;
        }

        public List<ShardLevelSnapshotIssue> shardLevelSnapshotIssues() {
            return shardLevelSnapshotIssues;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .startObject("issues")
                    .field("shards", shardLevelSnapshotIssues)
                    .field("root", rootLevelSnapshotIssues)
                    .endObject()
                    .endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static final class RootLevelSnapshotIssue implements ToXContentObject {

        private final String description;

        private RootLevelSnapshotIssue(String description) {
            this.description = description;
        }


        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("description", description);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static final class ShardLevelSnapshotIssue implements ToXContentObject {

        private final ShardLevelIssueType type;

        private final IndexId indexId;

        private final int shardId;

        private final SnapshotId snapshotId;

        private ShardLevelSnapshotIssue(ShardLevelIssueType type, IndexId indexId, int shardId, SnapshotId snapshotId) {
            this.type = type;
            this.indexId = indexId;
            this.shardId = shardId;
            this.snapshotId = snapshotId;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("type", type)
                    .field("index_id", indexId)
                    .field("shard_id", shardId)
                    .field("snapshot", snapshotId)
                    .endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public enum ShardLevelIssueType {
        MISSING_INDEX_GENERATION,
        MISSING_SNAPSHOT_META;
    }
}
