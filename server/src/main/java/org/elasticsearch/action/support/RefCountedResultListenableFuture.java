/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.RefCounted;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class RefCountedResultListenableFuture<T extends RefCounted> implements ActionListener<T> {

    private static final VarHandle STATE;
    private static final VarHandle LISTENER;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(RefCountedResultListenableFuture.class, "state", int.class);
            LISTENER = MethodHandles.lookup().findVarHandle(RefCountedResultListenableFuture.class, "listener", ActionListener.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static final int NEW = 0;
    public static final int COMPLETING = 1;
    public static final int NORMAL = 2;
    public static final int EXCEPTIONAL = 3;

    @SuppressWarnings("unused") // only accessed via STATE;
    private volatile int state;
    private Object result;
    @SuppressWarnings("unused") // only assigned via LISTENER
    private volatile ActionListener<T> listener;

    @Override
    public void onResponse(T t) {
        t.mustIncRef();
        if (STATE.compareAndSet(this, NEW, COMPLETING)) {
            result = t;
            STATE.setRelease(this, NORMAL); // final state
            var l = listener;
            if (l != null) {
                completeListener(l);
                LISTENER.set(this, null);
            }
        } else {
            t.decRef();
            throw new IllegalStateException("Already completed");
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (STATE.compareAndSet(this, NEW, COMPLETING)) {
            result = e;
            STATE.setRelease(this, EXCEPTIONAL); // final state
            var l = listener;
            if (l != null) {
                failListener(l);
                LISTENER.set(this, null);
            }
        }
    }

    public int state() {
        return state;
    }

    public T result() {
        @SuppressWarnings("unchecked")
        var r = (T) result;
        result = null;
        return r;
    }

    public Exception error() {
        var e = (Exception) result;
        result = null;
        return e;
    }

    public void addListener(ActionListener<T> listener) {
        switch (state) {
            case NEW -> {
                if (LISTENER.compareAndSet(this, null, listener) == false) {
                    throw new IllegalStateException("already listening");
                }
            }
            case COMPLETING -> {
                int s;
                while ((s = state) == COMPLETING) {
                    Thread.yield();
                }
                if (s == NORMAL) {
                    completeListener(listener);
                } else {
                    assert s == EXCEPTIONAL;
                    failListener(listener);
                }
            }
            case NORMAL -> completeListener(listener);
            case EXCEPTIONAL -> failListener(listener);
        }
    }

    private void completeListener(ActionListener<T> listener) {
        @SuppressWarnings("unchecked")
        T r = (T) result;
        try {
            listener.onResponse(r);
        } finally {
            r.decRef();
            result = null;
        }
    }

    private void failListener(ActionListener<T> listener) {
        Exception e;
        e = (Exception) result;
        listener.onFailure(e);
        result = null;
    }
}
