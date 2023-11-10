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
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class MinAggregator extends AbstractMinAggregator<ValuesSource.Numeric> {
    private static final int MAX_BKD_LOOKUPS = 1024;

    final String pointField;
    final Function<byte[], Number> pointConverter;

    MinAggregator(String name, ValuesSourceConfig config, AggregationContext context, Aggregator parent, Map<String, Object> metadata)
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
            Number segMin = findLeafMinValue(aggCtx.getLeafReaderContext().reader(), pointField, pointConverter);
            if (segMin != null) {
                /*
                 * There is no parent aggregator (see {@link MinAggregator#getPointReaderOrNull}
                 * so the ordinal for the bucket is always 0.
                 */
                double min = acc.get(0);
                min = Math.min(min, segMin.doubleValue());
                acc.set(0, min);
                // the minimum value has been extracted, we don't need to collect hits on this segment.
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
        }
        return doGetLeafBucketCollector(sub, valuesSource.doubleValues(aggCtx.getLeafReaderContext()));
    }

    /**
     * Returns the minimum value indexed in the <code>fieldName</code> field or <code>null</code>
     * if the value cannot be inferred from the indexed {@link PointValues}.
     */
    static Number findLeafMinValue(LeafReader reader, String fieldName, Function<byte[], Number> converter) throws IOException {
        final PointValues pointValues = reader.getPointValues(fieldName);
        if (pointValues == null) {
            return null;
        }
        final Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
            return converter.apply(pointValues.getMinPackedValue());
        }
        final Number[] result = new Number[1];
        try {
            pointValues.intersect(new PointValues.IntersectVisitor() {
                private short lookupCounter = 0;

                @Override
                public void visit(int docID) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void visit(int docID, byte[] packedValue) {
                    if (liveDocs.get(docID)) {
                        result[0] = converter.apply(packedValue);
                        // this is the first leaf with a live doc so the value is the minimum for this segment.
                        throw new CollectionTerminatedException();
                    }
                    if (++lookupCounter > MAX_BKD_LOOKUPS) {
                        throw new CollectionTerminatedException();
                    }
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            });
        } catch (CollectionTerminatedException e) {}
        return result[0];
    }
}
