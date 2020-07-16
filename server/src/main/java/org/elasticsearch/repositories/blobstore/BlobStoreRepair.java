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
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BlobStoreRepair {

    private static final Logger logger = LogManager.getLogger(BlobStoreRepair.class);

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
            final Map<String, IndexMetadata> cachedIndexMetadata = new HashMap<>();
            for (SnapshotId snapshotId : snapshotIds) {
                final String uuid = snapshotId.getUUID();
                if (snapshotUUIDsFromMetaBlobs.contains(uuid) == false) {
                    rootLevelSnapshotIssues.add(new RootLevelSnapshotIssue("Missing meta- blob for [" + uuid + "]"));
                }
                if (snapshotUUIDsFromSnapBlobs.contains(uuid) == false) {
                    rootLevelSnapshotIssues.add(new RootLevelSnapshotIssue("Missing snap- blob for [" + uuid + "]"));
                }
                final Collection<IndexId> indices = repositoryData.getIndices().values();

            }
            return new CheckResult(generation, shardLevelSnapshotIssues, rootLevelSnapshotIssues);
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

    public static final class CheckResult {

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
    }

    public static final class RootLevelSnapshotIssue implements ToXContent {

        private final String description;

        private RootLevelSnapshotIssue(String description) {
            this.description = description;
        }


        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("description", description);
        }
    }

    public static final class ShardLevelSnapshotIssue implements ToXContent {

        private final String description;

        private ShardLevelSnapshotIssue(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("description", description).endObject();
        }
    }
}
