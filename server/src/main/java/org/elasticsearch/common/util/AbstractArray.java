/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.lease.ReleaseOnce;

import java.util.Collection;
import java.util.Collections;

abstract class AbstractArray extends ReleaseOnce implements BigArray {

    private final BigArrays bigArrays;
    public final boolean clearOnResize;

    AbstractArray(BigArrays bigArrays, boolean clearOnResize) {
        this.bigArrays = bigArrays;
        this.clearOnResize = clearOnResize;
    }

    @Override
    protected final void closeInternal() {
        try {
            bigArrays.adjustBreaker(-ramBytesUsed(), true);
        } finally {
            doClose();
        }
    }

    protected abstract void doClose();

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }
}
