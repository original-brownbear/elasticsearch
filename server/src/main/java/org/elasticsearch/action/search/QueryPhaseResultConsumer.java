/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorsReducer;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;
import static org.elasticsearch.action.search.SearchPhaseController.mergeTopDocs;
import static org.elasticsearch.action.search.SearchPhaseController.setShardIndex;

/**
 * A {@link ArraySearchPhaseResults} implementation that incrementally reduces aggregation results
 * as shard results are consumed.
 * This implementation adds the memory that it used to save and reduce the results of shard aggregations
 * in the {@link CircuitBreaker#REQUEST} circuit breaker. Before any partial or final reduce, the memory
 * needed to reduce the aggregations is estimated and a {@link CircuitBreakingException} is thrown if it
 * exceeds the maximum memory allowed in this breaker.
 */
public class QueryPhaseResultConsumer extends ArraySearchPhaseResults<SearchPhaseResult> {

    private final SearchProgressListener progressListener;
    private final QueryPhaseRankCoordinatorContext queryPhaseRankCoordinatorContext;

    private final int topNSize;
    private final boolean hasTopDocs;

    private final Consumer<Exception> onPartialMergeFailure;

    private final int batchReduceSize;
    private List<QuerySearchResult> buffer = new ArrayList<>();

    private final AtomicReference<Exception> failure = new AtomicReference<>();

    private final TopDocsStats topDocsStats;
    private TopDocs reducedTopDocs;
    private volatile boolean hasPartialReduce;
    private volatile int numReducePhases;

    private AggregatorsReducer aggregatorsReducer;
    private final AggregationReduceContext aggregatorsReducerContext;

    /**
     * Creates a {@link QueryPhaseResultConsumer} that incrementally reduces aggregation results
     * as shard results are consumed.
     */
    public QueryPhaseResultConsumer(
        SearchRequest request,
        SearchPhaseController controller,
        Supplier<Boolean> isCanceled,
        SearchProgressListener progressListener,
        int expectedResultSize,
        Consumer<Exception> onPartialMergeFailure
    ) {
        super(expectedResultSize);
        this.progressListener = progressListener;
        this.topNSize = getTopDocsSize(request);
        this.onPartialMergeFailure = onPartialMergeFailure;

        SearchSourceBuilder source = request.source();
        int size = source == null || source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
        int from = source == null || source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
        this.queryPhaseRankCoordinatorContext = source == null || source.rankBuilder() == null
            ? null
            : source.rankBuilder().buildQueryPhaseCoordinatorContext(size, from);
        this.hasTopDocs = (source == null || size != 0) && queryPhaseRankCoordinatorContext == null;
        final boolean hasAggs = source != null && source.aggregations() != null;
        batchReduceSize = (hasAggs || hasTopDocs) ? Math.min(request.getBatchedReduceSize(), expectedResultSize) : expectedResultSize;
        topDocsStats = new TopDocsStats(request.resolveTrackTotalHitsUpTo());
        if (hasAggs) {
            var aggReduceContextBuilder = controller.getReduceContext(isCanceled, source.aggregations());
            aggregatorsReducerContext = request.isFinalReduce()
                ? aggReduceContextBuilder.forFinalReduction()
                : aggReduceContextBuilder.forPartialReduction();
        } else {
            aggregatorsReducerContext = null;
        }
    }

    @Override
    protected synchronized void doClose() {
        Releasables.close(aggregatorsReducer);
        aggregatorsReducer = null;
        this.buffer = null;
    }

    @Override
    public void consumeResult(SearchPhaseResult result, Runnable next) {
        try {
            super.consumeResult(result, () -> {});
            QuerySearchResult querySearchResult = result.queryResult();
            progressListener.notifyQueryResult(querySearchResult.getShardIndex(), querySearchResult);
            consume(querySearchResult);
        } finally {
            next.run();
        }
    }

    @Override
    public SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        Exception f = failure.get();
        if (f != null) {
            throw f;
        }
        List<QuerySearchResult> buffer;
        synchronized (this) {
            // final reduce, we're done with the buffer so we just null it out and continue with a local variable to
            // save field references. The synchronized block is never contended but needed to have a memory barrier and sync buffer's
            // contents with all the previous writers to it
            buffer = this.buffer;
            buffer = buffer == null ? Collections.emptyList() : buffer;
            this.buffer = null;
        }
        // ensure consistent ordering
        buffer.sort(RESULT_COMPARATOR);
        final TopDocsStats topDocsStats = this.topDocsStats;
        final int resultSize = buffer.size() + (reducedTopDocs == null ? 0 : 1);
        final List<TopDocs> topDocsList = hasTopDocs ? new ArrayList<>(resultSize) : null;
        if (topDocsList != null && reducedTopDocs != null) {
            topDocsList.add(reducedTopDocs);
        }
        for (QuerySearchResult result : buffer) {
            topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
            if (topDocsList != null) {
                var topDocs = result.consumeTopDocs().topDocs;
                setShardIndex(topDocs, result.getShardIndex());
                topDocsList.add(topDocs);
            }
        }
        SearchPhaseController.ReducedQueryPhase reducePhase;
        final InternalAggregations aggs;
        if (aggregatorsReducerContext != null) {
            // Add an estimate of the final reduce size
            var a = aggregatorsReducer.get();
            aggregatorsReducer.close();
            aggregatorsReducer = null;
            aggs = aggregatorsReducerContext.isFinalReduce() ? InternalAggregations.executeFinalReduce(aggregatorsReducerContext, a) : a;
        } else {
            aggs = null;
        }
        reducePhase = SearchPhaseController.reducedQueryPhase(
            results.asList(),
            aggs,
            topDocsList == null ? Collections.emptyList() : topDocsList,
            topDocsStats,
            numReducePhases,
            false,
            queryPhaseRankCoordinatorContext
        );
        if (progressListener != SearchProgressListener.NOOP) {
            progressListener.notifyFinalReduce(
                SearchProgressListener.buildSearchShards(results.asList()),
                reducePhase.totalHits(),
                reducePhase.aggregations(),
                reducePhase.numReducePhases()
            );
        }
        return reducePhase;

    }

    private static final Comparator<QuerySearchResult> RESULT_COMPARATOR = Comparator.comparingInt(QuerySearchResult::getShardIndex);

    private void partialReduce(List<QuerySearchResult> toConsume, int numReducePhases) {
        // ensure consistent ordering
        toConsume.sort(RESULT_COMPARATOR);

        List<TopDocs> topDocsList;
        if (hasTopDocs) {
            topDocsList = new ArrayList<>(toConsume.size() + (reducedTopDocs != null ? 1 : 0));
            if (reducedTopDocs != null) {
                topDocsList.add(reducedTopDocs);
            }
        } else {
            topDocsList = null;
        }
        for (QuerySearchResult result : toConsume) {
            topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
            if (topDocsList != null) {
                TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                setShardIndex(topDocs.topDocs, result.getShardIndex());
                topDocsList.add(topDocs.topDocs);
            }
        }
        // we have to merge here in the same way we collect on a shard
        final TopDocs newTopDocs = topDocsList == null ? null : mergeTopDocs(topDocsList, topNSize, 0);
        if (progressListener != SearchProgressListener.NOOP) {
            progressListener.notifyPartialReduce(
                SearchProgressListener.buildSearchShards(results.asList()),
                topDocsStats.getTotalHits(),
                InternalAggregations.EMPTY,
                numReducePhases
            );
        }

        this.reducedTopDocs = newTopDocs;
    }

    public int getNumReducePhases() {
        return numReducePhases;
    }

    private boolean hasFailure() {
        return failure.get() != null;
    }

    private void consume(QuerySearchResult result) {
        if (hasFailure() || result.isNull()) {
            result.consumeAll();
        } else {
            synchronized (this) {
                if (hasFailure()) {
                    result.consumeAll();
                    return;
                }
                if (aggregatorsReducerContext != null) {
                    consumeAggs(result);
                }
                var toConsume = buffer;
                // add one if a partial merge is pending
                int size = toConsume.size() + (hasPartialReduce ? 1 : 0);
                if (size >= batchReduceSize) {
                    hasPartialReduce = true;
                    buffer = new ArrayList<>();
                    try {
                        ++numReducePhases;
                        partialReduce(toConsume, numReducePhases);
                    } catch (Exception t) {
                        onMergeFailure(t);
                        return;
                    }
                    if (hasFailure()) {
                        return;
                    }
                }
                buffer.add(result);
            }
        }
    }

    private void consumeAggs(QuerySearchResult result) {
        try (var aggs = result.consumeAggs()) {
            var expanded = aggs.expand();
            if (aggregatorsReducer == null) {
                aggregatorsReducer = new AggregatorsReducer(expanded, aggregatorsReducerContext, results.length());
            }
            aggregatorsReducer.accept(expanded);
        }
    }

    private synchronized void onMergeFailure(Exception exc) {
        if (failure.compareAndSet(null, exc) == false) {
            return;
        }
        onPartialMergeFailure.accept(exc);
        reducedTopDocs = null;
    }
}
