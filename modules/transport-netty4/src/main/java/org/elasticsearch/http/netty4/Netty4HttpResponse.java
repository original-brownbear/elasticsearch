/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpPipelinedMessage;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.netty4.Netty4Utils;

public class Netty4HttpResponse extends DefaultFullHttpResponse implements HttpResponse, HttpPipelinedMessage {

    private final HttpHeaders requestHeaders;

    private final int sequence;

    Netty4HttpResponse(HttpHeaders requestHeaders, HttpVersion version, RestStatus status, BytesReference content, int sequence) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()), Netty4Utils.toByteBuf(content));
        this.requestHeaders = requestHeaders;
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

    public HttpHeaders requestHeaders() {
        return requestHeaders;
    }

    @Override
    public int getSequence() {
        return sequence;
    }
}
