/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public abstract class ReleaseOnce implements Releasable {

    protected static final VarHandle VH_RELEASED_FIELD;

    static {
        try {
            VH_RELEASED_FIELD = MethodHandles.lookup().in(ReleaseOnce.class).findVarHandle(ReleaseOnce.class, "released", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal") // updated via VH_RELEASED_FIELD (and _only_ via VH_RELEASED_FIELD)
    private volatile int released = 0;

    @Override
    public final void close() {
        if (VH_RELEASED_FIELD.compareAndSet(this, 0, 1)) {
            doClose();
        }
    }

    protected abstract void doClose();

    protected final boolean isClosed() {
        return released != 0;
    }
}
