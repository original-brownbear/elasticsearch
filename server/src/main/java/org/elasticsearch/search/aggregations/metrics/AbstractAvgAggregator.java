/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractAvgAggregator<T extends ValuesSource> extends AbstractSingleValueNumericAggregator<T> {

    protected LongArray counts;
    protected DoubleArray compensations;

    protected AbstractAvgAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, context, parent, metadata);
        final BigArrays bigArrays = context.bigArrays();
        counts = bigArrays.newLongArray(1, true);
        acc = bigArrays.newDoubleArray(1, true);
        compensations = bigArrays.newDoubleArray(1, true);
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= acc.size()) {
            return Double.NaN;
        }
        return acc.get(owningBucketOrd) / counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= acc.size()) {
            return buildEmptyAggregation();
        }
        return new InternalAvg(name, acc.get(bucket), counts.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalAvg.empty(name, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts, acc, compensations);
    }
}
