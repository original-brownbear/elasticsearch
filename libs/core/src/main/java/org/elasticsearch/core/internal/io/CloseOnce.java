/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.core.internal.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class CloseOnce implements Closeable {

    protected final AtomicBoolean closed = new AtomicBoolean();

    protected abstract void closeInternal() throws IOException;

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            closeInternal();
        }
    }
}
