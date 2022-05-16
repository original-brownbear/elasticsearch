/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xcontent;

import java.io.IOException;

public interface ChunkedToXContent {

    /**
     * Tries to write a best-fit chunk of xcontent to the given {@link ChunkedXContentBuilder} until its
     * {@link ChunkedXContentBuilder#isWriteable()} returns false.
     *
     * @param builder chunked builder to write to
     * @return {@code true} if unwritten content remains, {@code false} if not and the builder can be closed
     * @throws IOException on failure
     */
    boolean writeChunk(ChunkedXContentBuilder builder) throws IOException;
}
