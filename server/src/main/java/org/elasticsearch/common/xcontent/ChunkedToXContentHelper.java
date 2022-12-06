/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public enum ChunkedToXContentHelper {
    ;

    public static Iterator<ToXContent> wrapWithObject(Iterator<? extends ToXContent> iterator) {
        return Iterators.concat(Iterators.single((builder, params) -> builder.startObject()), iterator, endObject());
    }

    public static Iterator<ToXContent> endObject() {
        return Iterators.single(((builder, params) -> builder.endObject()));
    }

    public static Iterator<ToXContent> map(String name, Map<String, ?> map) {
        return map(name, map, entry -> (ToXContent) (builder, params) -> builder.field(entry.getKey(), entry.getValue()));
    }

    public static Iterator<ToXContent> xContentFragmentValuesMap(String name, Map<String, ? extends ToXContent> map) {
        return map(
            name,
            map,
            entry -> (ToXContent) (builder, params) -> entry.getValue().toXContent(builder.startObject(entry.getKey()), params).endObject()
        );
    }

    public static Iterator<ToXContent> xContentValuesMap(String name, Map<String, ? extends ToXContent> map) {
        return map(
            name,
            map,
            entry -> (ToXContent) (builder, params) -> entry.getValue().toXContent(builder.field(entry.getKey()), params)
        );
    }

    public static <T> Iterator<ToXContent> map(String name, Map<String, T> map, Function<Map.Entry<String, T>, ToXContent> toXContent) {
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.startObject(name)),
            map.entrySet().stream().map(toXContent).iterator(),
            endObject()
        );
    }

    public static Iterator<ToXContent> field(String name, boolean value) {
        return Iterators.single((builder, params) -> builder.field(name, value));
    }

    public static <T extends ToXContent> Iterator<ToXContent> array(String name, Iterable<T> iterable) {
        return array(Iterators.single((builder, params) -> builder.startArray(name)), iterable);
    }

    public static <T> Iterator<ToXContent> array(
        String name,
        Collection<T> collection,
        Function<T, ToXContent> toXContent
    ) {
        return array(Iterators.single((builder, params) -> builder.startArray(name)), () -> collection.stream().map(toXContent).iterator());
    }

    public static <T extends ToXContent> Iterator<ToXContent> array(Iterable<T> iterable) {
        return array(Iterators.single((builder, params) -> builder.startArray()), iterable);
    }

    private static <T extends ToXContent> Iterator<ToXContent> array(Iterator<ToXContent> open, Iterable<T> iterable) {
        return Iterators.concat(open, iterable.iterator(), Iterators.single((builder, params) -> builder.endArray()));
    }
}
