/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;

public abstract class BytesStream extends StreamOutput implements Releasable {

    public abstract BytesReference bytes();

    @Override
    public abstract void close();

    public abstract void seek(long position);
}
