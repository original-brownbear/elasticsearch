/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.AbstractAvgAggregator;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSource;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;

import java.io.IOException;
import java.util.Map;

class AggregateMetricBackedAvgAggregator extends AbstractAvgAggregator<AggregateMetricsValuesSource.AggregateDoubleMetric> {

    AggregateMetricBackedAvgAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, context, parent, metadata);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        // Retrieve aggregate values for metrics sum and value_count
        final SortedNumericDoubleValues aggregateValueCounts = valuesSource.getAggregateMetricValues(
            aggCtx.getLeafReaderContext(),
            Metric.value_count
        );
        final SortedNumericDoubleValues aggregateSums = valuesSource.getAggregateMetricValues(aggCtx.getLeafReaderContext(), Metric.sum);
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, acc) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                acc = bigArrays().grow(acc, bucket + 1);
                compensations = bigArrays().grow(compensations, bucket + 1);

                // Read aggregate values for sums
                if (aggregateSums.advanceExact(doc)) {
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = acc.get(bucket);
                    double compensation = compensations.get(bucket);

                    kahanSummation.reset(sum, compensation);
                    for (int i = 0; i < aggregateSums.docValueCount(); i++) {
                        double value = aggregateSums.nextValue();
                        kahanSummation.add(value);
                    }

                    acc.set(bucket, kahanSummation.value());
                    compensations.set(bucket, kahanSummation.delta());
                }

                counts = bigArrays().grow(counts, bucket + 1);
                // Read aggregate values for value_count
                if (aggregateValueCounts.advanceExact(doc)) {
                    for (int i = 0; i < aggregateValueCounts.docValueCount(); i++) {
                        double d = aggregateValueCounts.nextValue();
                        long value = Double.valueOf(d).longValue();
                        counts.increment(bucket, value);
                    }
                }
            }
        };
    }

}
