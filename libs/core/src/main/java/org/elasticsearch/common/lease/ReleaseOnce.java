/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.lease;

import org.elasticsearch.core.internal.io.CloseOnce;

public abstract class ReleaseOnce extends CloseOnce implements Releasable {

    protected abstract void closeInternal();

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            closeInternal();
        }
    }
}
