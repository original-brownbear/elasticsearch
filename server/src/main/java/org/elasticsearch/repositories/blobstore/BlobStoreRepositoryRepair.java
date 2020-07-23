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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
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
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public final class BlobStoreRepositoryRepair {

    private static final Logger logger = LogManager.getLogger(BlobStoreRepositoryRepair.class);

    public static void check(BlobStoreRepository repository, ActionListener<CheckResult> listener, Executor executor) {
        executor.execute(ActionRunnable.supply(listener, () -> {
            final BlobContainer rootContainer = repository.blobContainer();
            final long[] generations = findGenerations(rootContainer);
            if (generations.length == 0) {
                logger.info("Did not find any root level repository metadata in [{}]", rootContainer);
                return new CheckResult(RepositoryData.UNKNOWN_REPO_GEN, Version.CURRENT, Collections.emptyList(), Collections.emptyList());
            }
            final long generation = generations[generations.length - 1];
            final RepositoryData repositoryData;
            try (InputStream inputStream = rootContainer.readBlob(BlobStoreRepository.INDEX_FILE_PREFIX + generation)) {
                repositoryData = RepositoryData.snapshotsFromXContent(JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, inputStream),
                        generation, false);
            }
            final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
            final Collection<String> snapshotUUIDsFromSnapBlobs =
                    findSnapshotUUIDsByPrefix(rootContainer, BlobStoreRepository.SNAPSHOT_PREFIX);
            final Collection<String> snapshotUUIDsFromMetaBlobs =
                    findSnapshotUUIDsByPrefix(rootContainer, BlobStoreRepository.METADATA_PREFIX);
            final List<RootLevelSnapshotIssue> rootLevelSnapshotIssues = new ArrayList<>();
            final List<ShardLevelSnapshotIssue> shardLevelSnapshotIssues = new ArrayList<>();
            Version minVersion = Version.CURRENT;
            for (SnapshotId snapshotId : snapshotIds) {
                final String uuid = snapshotId.getUUID();
                if (snapshotUUIDsFromMetaBlobs.contains(uuid) == false) {
                    rootLevelSnapshotIssues.add(new RootLevelSnapshotIssue("Missing meta- blob for [" + uuid + "]"));
                }
                if (snapshotUUIDsFromSnapBlobs.contains(uuid) == false) {
                    rootLevelSnapshotIssues.add(new RootLevelSnapshotIssue("Missing snap- blob for [" + uuid + "]"));
                }
                final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                minVersion = Version.min(minVersion, snapshotInfo.version());
                for (String index : snapshotInfo.indices()) {
                    final IndexId indexId = repositoryData.resolveIndexId(index);
                    IndexMetadata indexMetadata =
                            repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId);
                    final int shards = indexMetadata.getNumberOfShards();
                    for (int i = 0; i < shards; i++) {
                        String shardGeneration = repositoryData.shardGenerations().getShardGen(indexId, i);
                        final int shardNum = i;
                        if (snapshotInfo.shardFailures().stream().anyMatch(snapshotShardFailure ->
                                snapshotShardFailure.index().equals(indexId.getName()) && snapshotShardFailure.shardId() == shardNum)) {
                            continue;
                        }
                        final BlobContainer shardContainer = repository.shardContainer(indexId, i);
                        final Set<String> blobsInShard = shardContainer.listBlobs().keySet();
                        if (shardGeneration == null) {
                            final OptionalLong foundGeneration =
                                    blobsInShard.stream().filter(blob -> blob.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX))
                                            .mapToLong(blob -> Long.parseLong(
                                                    blob.substring(BlobStoreRepository.INDEX_FILE_PREFIX.length())))
                                            .max();
                            if (foundGeneration.isPresent()) {
                                shardGeneration = String.valueOf(foundGeneration.getAsLong());
                            } else {
                                logger.debug("Found missing shard level metadata for [{}][{}]", indexId, i);
                                shardLevelSnapshotIssues.add(
                                        new ShardLevelSnapshotIssue(ShardLevelIssueType.MISSING_INDEX_GENERATION, indexId, i, snapshotId));
                                continue;
                            }
                        }
                        final BlobStoreIndexShardSnapshots shardLevelMeta;
                        try {
                            shardLevelMeta = BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT.read(
                                    repository.shardContainer(indexId, i), shardGeneration, NamedXContentRegistry.EMPTY);
                        } catch (FileNotFoundException | CorruptStateException e) {
                            logger.debug(() -> new ParameterizedMessage(
                                    "Found missing or corrupted shard level metadata for [{}][{}]", indexId, shardNum), e);
                            shardLevelSnapshotIssues.add(
                                    new ShardLevelSnapshotIssue(ShardLevelIssueType.MISSING_INDEX_GENERATION, indexId, i, snapshotId));
                            continue;
                        }
                        for (SnapshotFiles snapshotFiles : shardLevelMeta) {
                            for (BlobStoreIndexShardSnapshot.FileInfo indexFile : snapshotFiles.indexFiles()) {
                                if (indexFile.name().startsWith(BlobStoreRepository.VIRTUAL_DATA_BLOB_PREFIX)) {
                                    continue;
                                }
                                final long parts = indexFile.numberOfParts();
                                // TODO: check file existence below
                                if (parts == 1) {
                                    if (blobsInShard.contains(indexFile.name()) == false) {
                                        assert false : "not implemented yet";
                                    }
                                } else {
                                    for (int j = 0; j < indexFile.numberOfParts(); j++) {
                                        if (blobsInShard.contains(indexFile.partName(j)) == false) {
                                            assert false : "not implemented yet";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return new CheckResult(generation, minVersion, shardLevelSnapshotIssues, rootLevelSnapshotIssues);
        }));
    }

    public static void check(BlobStoreRepository repository, ActionListener<CheckResult> listener) {
        check(repository, listener, repository.threadPool().generic());
    }

    public static void executeFixes(BlobStoreRepository repository, CheckResult issues, ActionListener<CheckResult> listener) {
        repository.threadPool().generic().execute(ActionRunnable.wrap(listener, l -> {
            logger.info("Repairing the issues for repository [{}][{}]", repository.metadata.name(), issues);
            for (RootLevelSnapshotIssue rootLevelSnapshotIssue : issues.rootLevelSnapshotIssues()) {
                logger.info("Could not fix issue [{}]", rootLevelSnapshotIssue);
            }
            final Map<Tuple<IndexId, Integer>, List<SnapshotId>> shardLevelIndexGensToFix = new HashMap<>();
            for (ShardLevelSnapshotIssue shardLevelSnapshotIssue : issues.shardLevelSnapshotIssues()) {
                switch (shardLevelSnapshotIssue.type) {
                    case MISSING_INDEX_GENERATION:
                        shardLevelIndexGensToFix.computeIfAbsent(
                                Tuple.tuple(shardLevelSnapshotIssue.indexId, shardLevelSnapshotIssue.shardId),
                                k -> new ArrayList<>()).add(shardLevelSnapshotIssue.snapshotId);
                        break;
                    default:
                        logger.info("Could not fix issue [{}]", shardLevelSnapshotIssue);
                }
            }
            final RepositoryData repositoryData;
            final BlobContainer rootContainer = repository.blobContainer();
            try (InputStream inputStream = rootContainer.readBlob(BlobStoreRepository.INDEX_FILE_PREFIX + issues.repositoryGeneration())) {
                repositoryData = RepositoryData.snapshotsFromXContent(JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, inputStream),
                        issues.repositoryGeneration(), false);
            }
            if (shardLevelIndexGensToFix.isEmpty() == false) {
                final boolean useShardGenerations = SnapshotsService.useShardGenerations(issues.repoMetaVersion());
                for (Map.Entry<Tuple<IndexId, Integer>, List<SnapshotId>> tupleListEntry : shardLevelIndexGensToFix.entrySet()) {
                    final IndexId indexId = tupleListEntry.getKey().v1();
                    final int shardId = tupleListEntry.getKey().v2();
                    final String newGeneration;
                    if (useShardGenerations) {
                        // start over at generation 0
                        // TODO: This is not great on S3 but it seems impossible to guess a safe generation that hasn't been used before?
                        newGeneration = "0";
                    } else {
                        // use expected generation
                        // TODO: This is not great on S3 but moving to a new generation would require writing out a new index-N which
                        //  is tricky
                        newGeneration =
                                repositoryData.shardGenerations().getShardGen(tupleListEntry.getKey().v1(), tupleListEntry.getKey().v2());
                    }
                    final BlobContainer shardContainer = repository.shardContainer(indexId, shardId);
                    final List<BlobStoreIndexShardSnapshot> snapshotLevelMeta = new ArrayList<>();
                    for (SnapshotId snapshotId : tupleListEntry.getValue()) {
                        final BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot =
                                BlobStoreRepository.INDEX_SHARD_SNAPSHOT_FORMAT.read(
                                        shardContainer, snapshotId.getUUID(), NamedXContentRegistry.EMPTY);
                        snapshotLevelMeta.add(blobStoreIndexShardSnapshot);
                    }
                    final BlobStoreIndexShardSnapshots newShardMetaIndex = new BlobStoreIndexShardSnapshots(
                            snapshotLevelMeta.stream()
                                    .map(blobStoreIndexShardSnapshot -> new SnapshotFiles(
                                            blobStoreIndexShardSnapshot.snapshot(), blobStoreIndexShardSnapshot.indexFiles(), null))
                                    .collect(Collectors.toList()));
                    BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT.write(
                            newShardMetaIndex, shardContainer, newGeneration, repository.isCompress());
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

        private final Version repoMetaVersion;

        private final List<ShardLevelSnapshotIssue> shardLevelSnapshotIssues;

        private final List<RootLevelSnapshotIssue> rootLevelSnapshotIssues;

        private CheckResult(long repositoryGeneration, Version repoMetaVersion, List<ShardLevelSnapshotIssue> shardLevelSnapshotIssues,
                            List<RootLevelSnapshotIssue> rootLevelSnapshotIssues) {
            this.repositoryGeneration = repositoryGeneration;
            this.shardLevelSnapshotIssues = shardLevelSnapshotIssues;
            this.rootLevelSnapshotIssues = rootLevelSnapshotIssues;
            this.repoMetaVersion = repoMetaVersion;
        }

        public long repositoryGeneration() {
            return repositoryGeneration;
        }

        public Version repoMetaVersion() {
            return repoMetaVersion;
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

        public ShardLevelIssueType type() {
            return type;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public enum ShardLevelIssueType {
        /**
         * Missing {@code index-{generation}} blob containing a {@link BlobStoreIndexShardSnapshots}.
         */
        MISSING_INDEX_GENERATION,
        /**
         * Missing {@code snap-{uuid}.dat} blob containing a {@link BlobStoreIndexShardSnapshot}.
         */
        MISSING_SNAPSHOT_META,
        /**
         * Missing segment data blob referenced by an existing snapshot.
         */
        MISSING_SEGMENT_DATA_BLOB
    }
}
