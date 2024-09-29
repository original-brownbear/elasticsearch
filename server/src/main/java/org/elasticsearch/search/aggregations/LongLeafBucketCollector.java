/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;

public abstract class LongLeafBucketCollector extends LeafBucketCollectorBase {

    private final NumericDocValues values;

    public LongLeafBucketCollector(LeafBucketCollector sub, NumericDocValues values) {
        super(sub, values);
        this.values = values;
    }

    @Override
    public final void collect(int doc, long bucket) throws IOException {
        if (values.advanceExact(doc)) {
            collect(doc, bucket, values.longValue());
        }
    }

    protected abstract void collect(int doc, long bucket, long value) throws IOException;
}
