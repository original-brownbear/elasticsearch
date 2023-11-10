/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

class MaxAggregator extends AbstractMaxAggregator<ValuesSource.Numeric> {

    final String pointField;
    final Function<byte[], Number> pointConverter;

    MaxAggregator(String name, ValuesSourceConfig config, AggregationContext context, Aggregator parent, Map<String, Object> metadata)
        throws IOException {
        super(name, config, context, parent, metadata);
        this.pointConverter = pointReaderIfAvailable(config);
        if (pointConverter != null) {
            pointField = config.fieldContext().field();
        } else {
            pointField = null;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (pointConverter != null) {
            Number segMax = findLeafMaxValue(aggCtx.getLeafReaderContext().reader(), pointField, pointConverter);
            if (segMax != null) {
                /*
                 * There is no parent aggregator (see {@link AggregatorBase#getPointReaderOrNull}
                 * so the ordinal for the bucket is always 0.
                 */
                assert acc.size() == 1;
                double max = acc.get(0);
                max = Math.max(max, segMax.doubleValue());
                acc.set(0, max);
                // the maximum value has been extracted, we don't need to collect hits on this segment.
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
        }
        return doGetLeafBucketCollector(sub, valuesSource.doubleValues(aggCtx.getLeafReaderContext()));
    }

    /**
     * Returns the maximum value indexed in the <code>fieldName</code> field or <code>null</code>
     * if the value cannot be inferred from the indexed {@link PointValues}.
     */
    static Number findLeafMaxValue(LeafReader reader, String fieldName, Function<byte[], Number> converter) throws IOException {
        final PointValues pointValues = reader.getPointValues(fieldName);
        if (pointValues == null) {
            return null;
        }
        final Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
            return converter.apply(pointValues.getMaxPackedValue());
        }
        int numBytes = pointValues.getBytesPerDimension();
        final byte[] maxValue = pointValues.getMaxPackedValue();
        final byte[][] result = new byte[1][];
        pointValues.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (liveDocs.get(docID)) {
                    // we need to collect all values in this leaf (the sort is ascending) where
                    // the last live doc is guaranteed to contain the max value for the segment.
                    if (result[0] == null) {
                        result[0] = new byte[packedValue.length];
                    }
                    System.arraycopy(packedValue, 0, result[0], 0, packedValue.length);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                if (Arrays.equals(maxValue, 0, numBytes, maxPackedValue, 0, numBytes)) {
                    // we only check leaves that contain the max value for the segment.
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
            }
        });
        return result[0] != null ? converter.apply(result[0]) : null;
    }
}
