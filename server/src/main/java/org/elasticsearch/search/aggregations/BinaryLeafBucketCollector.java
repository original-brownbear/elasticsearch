/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public abstract class BinaryLeafBucketCollector extends LeafBucketCollectorBase {

    private final BinaryDocValues values;

    public BinaryLeafBucketCollector(LeafBucketCollector sub, BinaryDocValues values) {
        super(sub, values);
        this.values = values;
    }

    @Override
    public final void collect(int doc, long bucket) throws IOException {
        if (values.advanceExact(doc)) {
            collect(doc, bucket, values.binaryValue());
        }
    }

    protected abstract void collect(int doc, long bucket, BytesRef value) throws IOException;
}
