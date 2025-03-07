/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class ResultReference<T> {

    private static final Object DONE = new Object();

    private Object result;

    private static final VarHandle RESULT;

    static {
        try {
            RESULT = MethodHandles.lookup().findVarHandle(ResultReference.class, "result", Object.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public ResultReference(T value) {
        this.result = value;
    }

    @SuppressWarnings("unchecked")
    public T consume() {
        Object res = RESULT.getAndSet(this, DONE);
        assert res != DONE;
        return (T) res;
    }
}
