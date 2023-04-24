/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.AbstractBytesStreamOutputTests;
import org.elasticsearch.common.io.stream.BytesStream;

import java.io.IOException;

public class Netty4BytesStreamTests extends AbstractBytesStreamOutputTests {

    @Override
    protected BytesStream newStream() {
        return new Netty4BytesStream();
    }


    public void testWriteLargeBuffer() throws IOException {
        try (BytesStream out = newStream()) {
            for (int i = 0; i < 100; i++) {
                out.writeBytes(randomizedByteArrayWithSize(16 * 8096));
            }
            final BytesReference reference = out.bytes();
            assertTrue(reference.hasArray());
        }
    }
}
