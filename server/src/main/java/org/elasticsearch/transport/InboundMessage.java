/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import net.jpountz.lz4.LZ4BlockInputStream;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.DeflateCompressor;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.util.Objects;

public class InboundMessage implements Releasable {

    private final Header header;
    private final ReleasableBytesReference content;
    private final Exception exception;
    private final boolean isPing;
    private Releasable breakerRelease;
    private StreamInput streamInput;

    public InboundMessage(Header header, ReleasableBytesReference content, Releasable breakerRelease) {
        this.header = header;
        this.content = content;
        this.breakerRelease = breakerRelease;
        this.exception = null;
        this.isPing = false;
    }

    public InboundMessage(Header header, Exception exception) {
        this.header = header;
        this.content = null;
        this.breakerRelease = null;
        this.exception = exception;
        this.isPing = false;
    }

    public InboundMessage(Header header, boolean isPing) {
        this.header = header;
        this.content = null;
        this.breakerRelease = null;
        this.exception = null;
        this.isPing = isPing;
    }

    public Header getHeader() {
        return header;
    }

    public int getContentLength() {
        if (content == null) {
            return 0;
        } else {
            return content.length();
        }
    }

    public Exception getException() {
        return exception;
    }

    public boolean isPing() {
        return isPing;
    }

    public boolean isShortCircuit() {
        return exception != null;
    }

    public Releasable takeBreakerReleaseControl() {
        final Releasable toReturn = breakerRelease;
        breakerRelease = null;
        return Objects.requireNonNullElse(toReturn, () -> {});
    }

    public StreamInput openOrGetStreamInput(@Nullable Compression.Scheme fallbackScheme) throws IOException {
        assert isPing == false && content != null;
        if (streamInput == null) {
            final StreamInput uncompressed;
            if (header.isCompressed()) {
                final Compression.Scheme scheme = header.getCompressionScheme() == null ? header.getCompressionScheme() : fallbackScheme;
                if (scheme == Compression.Scheme.DEFLATE) {
                    uncompressed = new InputStreamStreamInput(DeflateCompressor.inputStream(content.streamInput(), false));
                } else if (scheme == Compression.Scheme.LZ4) {
                    uncompressed = new InputStreamStreamInput(new LZ4BlockInputStream(content.streamInput()));
                } else {
                    throw new AssertionError("wtf?");
                }
            } else {
                uncompressed = content.streamInput();
            }
            streamInput = uncompressed;
            streamInput.setVersion(header.getVersion());
        }
        return streamInput;
    }

    @Override
    public void close() {
        try {
            IOUtils.close(streamInput, content, breakerRelease);
        } catch (Exception e) {
            assert false : e;
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public String toString() {
        return "InboundMessage{" + header + "}";
    }
}
