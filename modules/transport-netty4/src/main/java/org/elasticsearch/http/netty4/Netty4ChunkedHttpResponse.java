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

import org.elasticsearch.http.ChunkedHttpBody;
import org.elasticsearch.http.HttpPipelinedMessage;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class Netty4ChunkedHttpResponse extends DefaultHttpResponse implements HttpResponse, HttpPipelinedMessage {

    private final ChunkedHttpBody body;

    private final int sequence;

    public Netty4ChunkedHttpResponse(HttpVersion version, RestStatus status, ChunkedHttpBody body, int sequence) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()));
        this.body = body;
        this.sequence = sequence;
    }

    @Override
    public void addHeader(String name, String value) {
        headers().add(name, value);
    }

    @Override
    public boolean containsHeader(String name) {
        return headers().contains(name);
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public boolean serializeChunk(ByteBuf buffer) throws IOException {
        ByteBufOutputStream out = new ByteBufOutputStream(buffer);
        return body.serialize(out, buffer.maxFastWritableBytes());
    }
}
