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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class DeduplicatingStreamInput extends NamedWriteableAwareStreamInput {

    private final Map<Class<?>, Map<?, ?>> seen = new HashMap<>();

    private final Map<Object, AtomicLong> hitCounts = new HashMap<>();

    public DeduplicatingStreamInput(StreamInput delegate, NamedWriteableRegistry namedWriteableRegistry) {
        super(delegate, namedWriteableRegistry);
    }

    @Override
    public String readString() throws IOException {
        return readCached(in -> super.readString(), String.class);
    }

    @Override
    public <T> T readCached(Writeable.Reader<T> reader, Class<T> clazz) throws IOException {
        final T read = reader.read(this);
        final T res = getCache(clazz).computeIfAbsent(read, Function.identity());
        if (read != res) {
            hitCounts.computeIfAbsent(res, k -> new AtomicLong()).incrementAndGet();
        }
        return res;
    }

    @SuppressWarnings("unchecked")
    private <T> Map<T, T> getCache(Class<T> clazz) {
        return (Map<T, T>) seen.computeIfAbsent(clazz, k -> new HashMap<T, T>());
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
