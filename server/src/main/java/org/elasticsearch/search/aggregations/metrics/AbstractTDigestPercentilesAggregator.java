/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

abstract class AbstractTDigestPercentilesAggregator extends NumericMetricsAggregator.MultiDoubleValue {

    static final class TDigestStates implements Releasable {
        private ObjectArray<TDigestState> states;
        final double compression;
        final TDigestExecutionHint executionHint;
        private final BigArrays bigArrays;

        TDigestStates(double compression, TDigestExecutionHint executionHint, BigArrays bigArrays) {
            this.states = bigArrays.newObjectArray(1);
            this.compression = compression;
            this.executionHint = executionHint;
            this.bigArrays = bigArrays;
        }

        TDigestState getExistingOrNewHistogram(long bucket) {
            states = bigArrays.grow(states, bucket + 1);
            TDigestState state = states.get(bucket);
            if (state == null) {
                state = TDigestState.create(compression, executionHint);
                states.set(bucket, state);
            }
            return state;
        }

        TDigestState getState(long bucketOrd) {
            if (bucketOrd >= states.size()) {
                return null;
            }
            return states.get(bucketOrd);
        }

        @Override
        public void close() {
            states.close();
        }
    }

    protected final TDigestStates states;
    protected final double[] keys;
    protected final DocValueFormat formatter;
    protected final boolean keyed;

    AbstractTDigestPercentilesAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        double[] keys,
        double compression,
        TDigestExecutionHint executionHint,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, context, parent, metadata);
        assert config.hasValues();
        this.keyed = keyed;
        this.formatter = formatter;
        this.states = new TDigestStates(compression, executionHint, context.bigArrays());
        this.keys = keys;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final TDigestState state = states.getExistingOrNewHistogram(bucket);
                    for (int i = 0; i < values.docValueCount(); i++) {
                        state.add(values.nextValue());
                    }
                }
            }
        };
    }

    @Override
    protected LeafBucketCollector getLeafCollector(NumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    states.getExistingOrNewHistogram(bucket).add(values.doubleValue());
                }
            }
        };
    }

    @Override
    public boolean hasMetric(String name) {
        return PercentilesConfig.indexOfKey(keys, Double.parseDouble(name)) >= 0;
    }

    @Override
    protected void doClose() {
        Releasables.close(states);
    }

}
