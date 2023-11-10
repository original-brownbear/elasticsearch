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
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractMaxAggregator<T extends ValuesSource> extends AbstractSingleValueNumericAggregator<T> {
    protected AbstractMaxAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, context, parent, metadata);
        acc = context.bigArrays().newDoubleArray(1, false);
        acc.fill(0, acc.size(), Double.NEGATIVE_INFINITY);
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= acc.size()) {
            return Double.NEGATIVE_INFINITY;
        }
        return acc.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= acc.size()) {
            return buildEmptyAggregation();
        }
        return new Max(name, acc.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return Max.createEmptyMax(name, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(acc);
    }

    protected LeafBucketCollectorBase doGetLeafBucketCollector(LeafBucketCollector sub, SortedNumericDoubleValues allValues) {
        final NumericDoubleValues values = MultiValueMode.MAX.select(allValues);
        return new LeafBucketCollectorBase(sub, allValues) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= acc.size()) {
                    long from = acc.size();
                    acc = bigArrays().grow(acc, bucket + 1);
                    acc.fill(from, acc.size(), Double.NEGATIVE_INFINITY);
                }
                if (values.advanceExact(doc)) {
                    final double value = values.doubleValue();
                    double max = acc.get(bucket);
                    max = Math.max(max, value);
                    acc.set(bucket, max);
                }
            }
        };
    }
}
