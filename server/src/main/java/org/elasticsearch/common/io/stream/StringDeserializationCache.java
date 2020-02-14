/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.CharsRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class StringDeserializationCache {

    public static final StringDeserializationCache DUMMY = new StringDeserializationCache();

    private volatile Map<CharsRef, String> cache;

    private final Map<CharsRef, Integer> refCounts = new HashMap<>();

    public void cache(String... value) {
        synchronized (this) {
            Map<CharsRef, String> additions = null;
            for (String s : value) {
                final CharsRef ref = new CharsRef(s);
                if (refCounts.compute(ref, (r, c) -> {
                    if (c == null) {
                        return 1;
                    } else {
                        return c + 1;
                    }
                }) == 1) {
                    if (additions == null) {
                        additions = new HashMap<>();
                    }
                    additions.put(ref, s.intern());
                }
            }
            if (additions != null) {
                final Map<CharsRef, String> updatedCache = new HashMap<>(cache);
                updatedCache.putAll(additions);
                cache = updatedCache;
            }
        }
    }

    public void remove(String... value) {
        synchronized (this) {
            List<CharsRef> toRemove = null;
            for (String s : value) {
                final CharsRef ref = new CharsRef(s);
                if (refCounts.compute(ref, (r, c) -> {
                    if (c == 1) {
                        return null;
                    }
                    return c - 1;
                }) == null) {
                    if (toRemove == null) {
                        toRemove = new ArrayList<>();
                    }
                    toRemove.add(ref);
                }
            }
        }
    }

    public String get(CharsRef charsRef) {
        return cache.get(charsRef);
    }
}
