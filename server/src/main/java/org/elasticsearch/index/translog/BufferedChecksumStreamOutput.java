/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.store.BufferedChecksum;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * Similar to Lucene's BufferedChecksumIndexOutput, however this wraps a
 * {@link StreamOutput} so anything written will update the checksum
 */
public final class BufferedChecksumStreamOutput extends OutputStreamStreamOutput {
    public BufferedChecksumStreamOutput(OutputStream out) {
        super(new CheckedOutputStream(out, new BufferedChecksum(new CRC32())));
    }

    public long getChecksum() {
        return ((CheckedOutputStream) out).getChecksum().getValue();
    }

    public void resetDigest() {
        ((CheckedOutputStream) out).getChecksum().reset();
    }
}
