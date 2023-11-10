/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractSumAggregator<T extends ValuesSource> extends AbstractSingleValueNumericAggregator<T> {

    protected DoubleArray compensations;

    protected AbstractSumAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, context, parent, metadata);
        acc = bigArrays().newDoubleArray(1, true);
        compensations = bigArrays().newDoubleArray(1, true);
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= acc.size()) {
            return 0.0;
        }
        return acc.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= acc.size()) {
            return buildEmptyAggregation();
        }
        return new Sum(name, acc.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return Sum.empty(name, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(acc, compensations);
    }

    protected LeafBucketCollectorBase doGetLeafBucketCollector(LeafBucketCollector sub, SortedNumericDoubleValues values) {
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                acc = bigArrays().grow(acc, bucket + 1);
                compensations = bigArrays().grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount(); // For aggregate metric this should always equal to 1
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = acc.get(bucket);
                    double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);

                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        kahanSummation.add(value);
                    }
                    compensations.set(bucket, kahanSummation.delta());
                    acc.set(bucket, kahanSummation.value());
                }
            }
        };
    }

}
