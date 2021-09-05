/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.cache.query;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;

public class DisabledQueryCache implements QueryCache {

    private static final DisabledQueryCache INSTANCE = new DisabledQueryCache();

    public static DisabledQueryCache get(Index index) {
        Loggers.getLogger(DisabledQueryCache.class, index).debug("Using no query cache");
        return INSTANCE;
    }

    private DisabledQueryCache() {
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        return weight;
    }

    @Override
    public void clear(String reason) {
        // nothing to do here
    }
}
