/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public abstract class DelegatingResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {
    protected final TransportResponseHandler<T> delegate;

    protected DelegatingResponseHandler(TransportResponseHandler<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public final String executor() {
        return delegate.executor();
    }

    @Override
    public final T read(StreamInput in) throws IOException {
        return delegate.read(in);
    }
}
