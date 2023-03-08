/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.io.IOException;

/**
 * Wraps the provided {@link XContentParser} and delegates to it.
 */
public class FilterXContentParserWrapper extends FilterXContentParser {
    private final XContentParser delegate;

    public FilterXContentParserWrapper(XContentParser delegate) {
        this.delegate = delegate;
    }

    @Override
    protected final XContentParser delegate() {
        return delegate;
    }

    @Override
    public long longValue(boolean coerce) throws IOException {
        return delegate.longValue(coerce);
    }

    @Override
    public long longValue() throws IOException {
        return delegate.longValue();
    }

    @Override
    public String textOrNull() throws IOException {
        return delegate.textOrNull();
    }
}
