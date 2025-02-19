/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.notNullValue;

public class TopHitsAsSubAggregationIT extends ESIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.search.size", randomIntBetween(1, 10))
            .put("thread_pool.search.queue_size", 1000 * randomIntBetween(1, 10))
            .build();
    }

    public void testTopHitsUnderRangeAggregation() throws Exception {
        final String indexName = "test";
        createIndex(indexName, 10, 0);
        assertAcked(
            indicesAdmin().preparePutMapping(indexName)
                .setSource(
                    jsonBuilder().startObject()
                        .field("dynamic", true)
                        .startObject("properties")
                        .startObject("numeric_value")
                        .field("type", "long")
                        .endObject()
                        .startObject("nested_value")
                        .field("type", "nested")
                        .field("dynamic", true)
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 1000; i++) {
                final String id = i + "_" + j;
                if (randomBoolean()) {
                    prepareIndex(indexName).setId(id)
                        .setSource("numeric_value", i % 10, "nested_value", Map.of("foo", "bar-" + i, "round", j))
                        .get();
                } else {
                    prepareIndex(indexName).setId(id)
                        .setSource(
                            "numeric_value",
                            i,
                            "nested_value",
                            randomList(
                                100,
                                () -> randomBoolean()
                                    ? randomNestedA(randomInt(100), randomInt(100))
                                    : randomNestedB(randomInt(100), randomInt(100))
                            )
                        )
                        .get();
                }
            }
            flushAndRefresh(indexName);
            for (int k = 0; k < 100; k++) {
                assertNoFailuresAndResponse(
                    prepareSearch(indexName).setSize(randomInt(5))
                        .addAggregation(
                            AggregationBuilders.range("some_ranges")
                                .field("numeric_value")
                                .addRange(0, 1)
                                .addRange(1, 2)
                                .addUnboundedFrom(2)
                                .subAggregation(
                                    AggregationBuilders.nested("nested", "nested_value")
                                        .subAggregation(
                                            AggregationBuilders.topHits("some_top_hits")
                                                .fetchSource("nested_value.round", null)
                                                .size(100)
                                                .sort("nested_value.round")
                                        )

                                )
                        )
                        .setFetchSource("numeric_value", null),
                    response -> assertThat(response.getAggregations().get("some_ranges"), notNullValue())
                );
            }
        }
    }

    private static Map<String, ?> randomNestedB(int i, int j) {
        return Map.of("blub", "blllllaaaa-" + i + "-" + j, "round" + (randomBoolean() ? "" : "-a"), j + 100000);
    }

    private static Map<String, ?> randomNestedA(int i, int j) {
        return Map.of("blub", "blllllaaaa-" + i + "-" + j, "round" + (randomBoolean() ? "" : "-b"), j);
    }
}
