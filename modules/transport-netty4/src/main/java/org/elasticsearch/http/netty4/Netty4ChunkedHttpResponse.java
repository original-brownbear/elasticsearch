/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.io.ChunkedSerialization;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestStatus;

public class Netty4ChunkedHttpResponse extends DefaultHttpResponse implements HttpResponse {

    private final int sequence;

    private final ChunkedSerialization content;

    Netty4ChunkedHttpResponse(int sequence, HttpVersion version, RestStatus status, ChunkedSerialization content) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()));
        this.sequence = sequence;
        this.content = content;
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

    public ChunkedSerialization content() {
        return content;
    }
}
