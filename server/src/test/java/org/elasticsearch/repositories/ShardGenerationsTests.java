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
package org.elasticsearch.repositories;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ShardGenerationsTests extends ESTestCase {

    public void testFindObsoleteGenerations() {
        final int nIndices = randomIntBetween(0, 10);
        final List<IndexId> indicesBefore = new ArrayList<>();
        final List<IndexId> indicesAfter = new ArrayList<>();

        final Map<IndexId, Integer> shards = new HashMap<>();
        final Map<IndexId, Set<Integer>> changedShards = new HashMap<>();

        for (int i = 0; i < nIndices; ++i) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID());
            if (randomBoolean()) {
                indicesBefore.add(indexId);
            }
            if (randomBoolean()) {
                indicesAfter.add(indexId);
            }
            shards.put(indexId, randomIntBetween(1, 5));
        }

        final ShardGenerations.Builder previous = ShardGenerations.builder();

        for (IndexId indexId : indicesBefore) {
            for (int i = 0; i < shards.get(indexId); ++i) {
                previous.add(indexId, i, UUIDs.randomBase64UUID());
            }
        }

        final ShardGenerations previousGenerations = previous.build();

        final ShardGenerations.Builder next = ShardGenerations.builder();
        for (IndexId indexId : indicesAfter) {
            if (indicesBefore.contains(indexId)) {
                final Set<Integer> changed = changedShards.computeIfAbsent(indexId, idx -> new HashSet<>());
                for (int i = 0; i < shards.get(indexId); ++i) {
                    if (randomBoolean()) {
                        next.add(indexId, i, UUIDs.randomBase64UUID());
                        changed.add(i);
                    } else {
                        next.add(indexId, i, previousGenerations.getShardGen(indexId, i));
                    }
                }
            } else {
                for (int i = 0; i < shards.get(indexId); ++i) {
                    next.add(indexId, i, UUIDs.randomBase64UUID());
                }
            }
        }

        final Map<IndexId, Map<Integer, String>> obsolete = next.build().obsoleteShardGenerations(previous.build());

        for (IndexId indexId : indicesBefore) {
            if (indicesAfter.contains(indexId)) {
                assertTrue(obsolete.containsKey(indexId));
                assertThat(obsolete.get(indexId).keySet(), equalTo(changedShards.get(indexId)));
            } else {
                assertFalse(obsolete.containsKey(indexId));
            }
        }
    }

    public void testForIndices() {
        final int nIndices = randomIntBetween(0, 10);
        final Set<IndexId> indexIds = new HashSet<>();
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        final Map<IndexId, Integer> shards = new HashMap<>();

        for (int i = 0; i < nIndices; ++i) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID());
            indexIds.add(indexId);
            for (int j = 0; j < shards.computeIfAbsent(indexId, idx -> randomIntBetween(1, 10)); ++j) {
                shardGenerations.add(indexId, j, UUIDs.randomBase64UUID());
            }
        }
        final Set<IndexId> retainedIndexIds = new HashSet<>();
        for (IndexId indexId : indexIds) {
            if (randomBoolean()) {
                retainedIndexIds.add(indexId);
            }
        }

        final ShardGenerations gens = shardGenerations.build();
        final ShardGenerations filtered = gens.forIndices(retainedIndexIds);

        for (IndexId indexId : indexIds) {
            for (int i = 0; i < shards.get(indexId); ++i) {
                assertThat(gens.getShardGen(indexId, i), notNullValue());
                if (retainedIndexIds.contains(indexId)) {
                    assertThat(filtered.getShardGen(indexId, i), notNullValue());
                } else {
                    assertThat(filtered.getShardGen(indexId, i), nullValue());
                }
            }
        }

        assertThat(filtered.indices(), containsInAnyOrder(retainedIndexIds.toArray()));
    }

    public void testUpdatedGenerationsAddIndex() {
        final int nIndices = randomIntBetween(0, 10);
        final Set<IndexId> indexIds = new HashSet<>();
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        final Map<IndexId, Integer> shards = new HashMap<>();

        for (int i = 0; i < nIndices; ++i) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID());
            indexIds.add(indexId);
            for (int j = 0; j < shards.computeIfAbsent(indexId, idx -> randomIntBetween(1, 10)); ++j) {
                shardGenerations.add(indexId, j, UUIDs.randomBase64UUID());
            }
        }

        final ShardGenerations gens = shardGenerations.build();

        final IndexId addedIndex = new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID());

        final ShardGenerations updatedGens =
            gens.updatedGenerations(ShardGenerations.builder().add(addedIndex, 1, UUIDs.randomBase64UUID()).build());

        assertThat(updatedGens.indices(), containsInAnyOrder(Sets.union(indexIds, Collections.singleton(addedIndex)).toArray()));
    }

    public void testUpdatedGenerationsPartiallyUpdateIndex() {
        final int nIndices = randomIntBetween(1, 10);
        final Set<IndexId> indexIds = new HashSet<>();
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        final Map<IndexId, Integer> shards = new HashMap<>();

        for (int i = 0; i < nIndices; ++i) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID());
            indexIds.add(indexId);
            for (int j = 0; j < shards.computeIfAbsent(indexId, idx -> randomIntBetween(2, 10)); ++j) {
                shardGenerations.add(indexId, j, UUIDs.randomBase64UUID());
            }
        }

        final ShardGenerations gens = shardGenerations.build();

        final IndexId changedIndex = randomFrom(indexIds);

        final String newUUID = UUIDs.randomBase64UUID();

        final ShardGenerations updatedGens =
            gens.updatedGenerations(ShardGenerations.builder().add(changedIndex, 1, newUUID).build());

        assertThat(updatedGens.indices(), containsInAnyOrder(indexIds.toArray()));
        assertThat(updatedGens.getShardGen(changedIndex, 0), notNullValue());
        assertThat(updatedGens.getShardGen(changedIndex, 1), equalTo(newUUID));
    }

    public void testEqualsAndHashCode() {
        final ShardGenerations shardGenerations = randomShardGeneration();
        final ShardGenerations other = shardGenerations.updatedGenerations(ShardGenerations.EMPTY);
        assertThat(shardGenerations, equalTo(other));
        assertThat(shardGenerations.hashCode(), equalTo(other.hashCode()));
    }

    public void testThrowsOnIllegalShardId() {
        final IndexId indexId = new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID());
        final int shards = randomIntBetween(1, 10);
        final int illegalShardId = shards + randomIntBetween(0, 10);
        final ShardGenerations.Builder builder = ShardGenerations.builder();
        for (int i = 0; i < shards; ++i) {
            builder.add(indexId, i, UUIDs.randomBase64UUID());
        }
        final ShardGenerations shardGenerations = builder.build();

        final IllegalArgumentException iae =
            expectThrows(IllegalArgumentException.class, () -> shardGenerations.getShardGen(indexId, illegalShardId));
        assertThat(iae.getMessage(), endsWith("only has [" + shards + "] shards but requested shard [" + illegalShardId + "]"));

        for (int i = 0; i < shards; ++i) {
            assertThat(shardGenerations.getShardGen(indexId, i), notNullValue());
        }
    }

    public void testReturnNullForUnknownIndex() {
        assertThat(randomShardGeneration().getShardGen(
            new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID()), randomIntBetween(1, 10)), nullValue());
    }

    public void testXContent() throws IOException {
        final int nIndices = randomIntBetween(0, 10);
        final Set<IndexId> indexIds = new HashSet<>();
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        final Map<IndexId, Integer> shards = new HashMap<>();

        for (int i = 0; i < nIndices; ++i) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID());
            indexIds.add(indexId);
            for (int j = 0; j < shards.computeIfAbsent(indexId, idx -> randomIntBetween(1, 10)); ++j) {
                shardGenerations.add(indexId, j, UUIDs.randomBase64UUID());
            }
        }
        final ShardGenerations gens = shardGenerations.build();
        final XContentBuilder smileBuilder = SmileXContent.contentBuilder();

        gens.toXContent(smileBuilder, ToXContent.EMPTY_PARAMS);

        final BytesReference bytes = BytesReference.bytes(smileBuilder);

    }

    private static ShardGenerations randomShardGeneration() {
        final int nIndices = randomIntBetween(0, 10);
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        for (int i = 0; i < nIndices; ++i) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(5), UUIDs.randomBase64UUID());
            final int shards = randomIntBetween(1, 10);
            for (int j = 0; j < shards; ++j) {
                shardGenerations.add(indexId, j, UUIDs.randomBase64UUID());
            }
        }
        return shardGenerations.build();
    }
}
