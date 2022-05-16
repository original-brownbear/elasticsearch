/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ChunkedXContentBuilder;

import java.io.IOException;

public class ChunkedNetty4HttpResponse extends DefaultHttpResponse implements HttpResponse {

    private final int sequence;

    private final ChunkedXContentBuilder<ByteBufOutputStream> builder;
    private final ChunkedToXContent chunkedToXContent;

    ChunkedNetty4HttpResponse(int sequence,
                              HttpVersion version,
                              RestStatus status,
                              ChunkedXContentBuilder<ByteBufOutputStream> builder,
                              ChunkedToXContent chunkedToXContent) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()));
        this.sequence = sequence;
        this.chunkedToXContent = chunkedToXContent;
        this.builder = builder;
    }

    public int getSequence() {
        return sequence;
    }

    @Override
    public void addHeader(String name, String value) {
        headers().add(name, value);
    }

    @Override
    public boolean containsHeader(String name) {
        return headers().contains(name);
    }

    public ByteBuf write(ByteBuf out) throws IOException {
        if (builder.isWriteable()) {
            if (chunkedToXContent.writeChunk(builder) == false) {
                final ByteBufOutputStream existing = builder.setNewOutput(new ByteBufOutputStream(out), out.maxFastWritableBytes());
                return existing.buffer();
            }
        }
    }
}
