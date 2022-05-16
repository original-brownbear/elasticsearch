/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xcontent;

import org.elasticsearch.core.RestApiVersion;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

public final class ChunkedXContentBuilder<T extends OutputStream> {

    private final ChunkingOutputStream<T> chunkingOut;
    private final XContentBuilder builder;

    public ChunkedXContentBuilder(
        XContent xContent,
        Set<String> includes,
        Set<String> excludes,
        ParsedMediaType responseContentType,
        RestApiVersion restApiVersion
    ) throws IOException {
        chunkingOut = new ChunkingOutputStream<>();
        builder = new XContentBuilder(xContent, chunkingOut, includes, excludes, responseContentType, restApiVersion);
    }

    public XContentBuilder builder() {
        return builder;
    }

    public boolean isWriteable() {
        return chunkingOut.remaining > 0;
    }

    public T setNewOutput(T out, int sizeHint) {
        final T prev = chunkingOut.out;
        chunkingOut.out = out;
        chunkingOut.remaining = sizeHint;
        return prev;
    }

    public T finish() {
        builder.close();
        assert chunkingOut.out != null : "must have written bytes to finish";
        return chunkingOut.out;
    }

    private static final class ChunkingOutputStream<T extends OutputStream> extends OutputStream {

        private int remaining;

        private T out;

        @Override
        public void write(int b) throws IOException {
            remaining--;
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            remaining -= len;
            out.write(b, off, len);
        }

        @Override
        public void close() {
            // wrapped streams must be cloned by their owners
        }
    }
}
