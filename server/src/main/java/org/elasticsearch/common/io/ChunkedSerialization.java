/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.io;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;

import java.util.function.Consumer;

/**
 * A chunked serialization operation.
 */
public interface ChunkedSerialization {

    /**
     *
     * @param recycler buffer recycler to serialize into
     * @param sizeHint how many bytes should be serialized in this invocation
     * @return {@code true} if there is more content to be serialized or {@code false} if the content has been fully serialized
     */
    boolean writeChunk(Recycler<BytesRef> recycler, Consumer<ReleasableBytesReference> writer, int sizeHint);
}
