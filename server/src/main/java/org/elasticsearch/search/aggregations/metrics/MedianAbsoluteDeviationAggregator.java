/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation;

public class MedianAbsoluteDeviationAggregator extends NumericMetricsAggregator.SingleDoubleValue {

    private final DocValueFormat format;

    private final AbstractTDigestPercentilesAggregator.TDigestStates states;

    MedianAbsoluteDeviationAggregator(
        String name,
        ValuesSourceConfig config,
        DocValueFormat format,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        double compression,
        TDigestExecutionHint executionHint
    ) throws IOException {
        super(name, config, context, parent, metadata);
        assert config.hasValues();
        this.format = Objects.requireNonNull(format);
        this.states = new AbstractTDigestPercentilesAggregator.TDigestStates(compression, executionHint, context.bigArrays());
    }

    @Override
    public double metric(long owningBucketOrd) {
        var state = states.getState(owningBucketOrd);
        if (state != null) {
            return computeMedianAbsoluteDeviation(state);
        } else {
            return Double.NaN;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final TDigestState valueSketch = states.getExistingOrNewHistogram(bucket);
                    for (int i = 0; i < values.docValueCount(); i++) {
                        valueSketch.add(values.nextValue());
                    }
                }
            }
        };
    }

    @Override
    protected LeafBucketCollector getLeafCollector(NumericDoubleValues values, LeafBucketCollector sub) {
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
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        var state = states.getState(bucket);
        if (state != null) {
            return new InternalMedianAbsoluteDeviation(name, metadata(), format, state);
        } else {
            return buildEmptyAggregation();
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalMedianAbsoluteDeviation.empty(name, metadata(), format, states.compression, states.executionHint);
    }

    @Override
    public void doClose() {
        Releasables.close(states);
    }

}
