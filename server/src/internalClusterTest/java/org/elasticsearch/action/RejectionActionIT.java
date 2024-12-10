/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class RejectionActionIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", 1)
            .put("thread_pool.get.size", 1)
            .put("thread_pool.get.queue_size", 1)
            .build();
    }

    public void testSimulatedSearchRejectionLoad() throws Throwable {
        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "1").get();
        }
        int numberOfAsyncOps = randomIntBetween(200, 700);
        final List<ActionFuture<SearchResponse>> responseFutures = new ArrayList<>(numberOfAsyncOps);
        for (int i = 0; i < numberOfAsyncOps; i++) {
            responseFutures.add(
                prepareSearch("test").setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(QueryBuilders.matchQuery("field", "1")).execute()
            );
        }

        // validate all responseFutures
        for (ActionFuture<SearchResponse> f : responseFutures) {
            final SearchResponse searchResponse;
            try {
                searchResponse = f.actionGet();
            } catch (Exception t) {
                Throwable unwrap = ExceptionsHelper.unwrapCause(t);
                if (unwrap instanceof SearchPhaseExecutionException e) {
                    assertAllFailuresCancelledOrRejected(e.shardFailures());
                } else {
                    assertThat(unwrap, instanceOf(EsRejectedExecutionException.class));
                }
                continue;
            }
            assertAllFailuresCancelledOrRejected(searchResponse.getShardFailures());
        }
        assertThat(responseFutures.size(), equalTo(numberOfAsyncOps));
    }

    private static void assertAllFailuresCancelledOrRejected(ShardSearchFailure[] searchResponse) {
        for (ShardSearchFailure failure : searchResponse) {
            assertThat(failure.reason().toLowerCase(Locale.ENGLISH), anyOf(containsString("cancelled"), containsString("rejected")));
        }
    }
}
