/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class TopHitsAsSubAggregationIT extends ESIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.search.size", randomIntBetween(1, 10))
            .put("thread_pool.search.queue_size", 1000 * randomIntBetween(1, 10))
            .put(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), randomIntBetween(1, 500))
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
                        .startObject("properties")
                        .startObject("foo")
                        .field("type", "keyword")
                        .endObject()
                        .endObject()
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
                        .setSource(
                            "numeric_value",
                            i % 10,
                            "rare_value",
                            rarely() && rarely() ? "a" : "b",
                            "nested_value",
                            randomBoolean()
                                ? Map.of("foo", "bar-" + i, "round", j)
                                : List.of(
                                    Map.of("foo", "bar-" + i, "round", j),
                                    Map.of("foo", "bar-" + randomNonNegativeLong(), "round", j),
                                    Map.of("foo", "bar-" + randomNonNegativeLong())
                                )
                        )
                        .get();
                } else {
                    prepareIndex(indexName).setId(id)
                        .setSource(
                            "numeric_value",
                            i,
                            "nested_value",
                            randomBoolean()
                                ? List.of()
                                : randomList(
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
            indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(5).setFlush(true).get();
            final AtomicReference<String> agg = new AtomicReference<>();
            for (int k = 0; k < 100; k++) {
                assertNoFailuresAndResponse(
                    prepareSearch(indexName).setSize(0)
                        .addAggregation(
                            AggregationBuilders.nested("nested", "nested_value")
                                .subAggregation(
                                    AggregationBuilders.topHits("some_top_hits")
                                        .fetchSource(new String[] { "nested_value.round", "nested_value.foo", "rare_value" }, null)
                                        .size(100)
                                        .sorts(
                                            List.of(
                                                SortBuilders.fieldSort("nested_value.round"),
                                                SortBuilders.fieldSort("nested_value.foo"),
                                                SortBuilders.fieldSort("numeric_value"),
                                                SortBuilders.fieldSort("_doc")
                                            )
                                        )
                                )
                        )
                        .setFetchSource("numeric_value", null)
                        .setQuery(new BoolQueryBuilder().filter(new TermQueryBuilder("rare_value", "a"))),
                    response -> {
                        var r = Strings.toString((ToXContent) response.getAggregations().get("nested"), true, true);
                        var prev = agg.getAndSet(r);
                        if (prev != null) {
                            assertEquals(prev, r);
                        }
                    }
                );
            }
        }
    }

    private static Map<String, ?> randomNestedB(int i, int j) {
        return Map.of("foo", "blllllaaaa-" + i + "-" + j, "round" + (randomBoolean() ? "" : "-a"), j + 100000);
    }

    private static Map<String, ?> randomNestedA(int i, int j) {
        return Map.of("foo", "blllllaaaa-" + i + "-" + j, "round" + (randomBoolean() ? "" : "-b"), j);
    }
}
