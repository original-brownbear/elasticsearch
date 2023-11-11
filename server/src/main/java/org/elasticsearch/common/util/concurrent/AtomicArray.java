/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ElasticsearchGenerationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A list backed by an {@link AtomicReferenceArray} with potential null values, easily allowing
 * to get the concrete values as a list using {@link #asList()}.
 */
public class AtomicArray<E> {
    private final AtomicReferenceArray<E> array;
    private volatile List<E> nonNullList;

    public AtomicArray(int size) {
        array = new AtomicReferenceArray<>(size);
    }

    /**
     * The size of the expected results, including potential null values.
     */
    public int length() {
        return array.length();
    }

    /**
     * Returns the size of the expected results, excluding potential null values.
     * @return the number of non-null elements
     */
    public int nonNullLength() {
        if (nonNullList != null) {
            return nonNullList.size();
        }
        int count = 0;
        for (int i = 0; i < array.length(); i++) {
            if (array.get(i) != null) {
                count++;
            }
        }
        return count;
    }

    /**
     * Sets the element at position {@code i} to the given value.
     *
     * @param i     the index
     * @param value the new value
     */
    public void set(int i, E value) {
        assert nonNullList == null : nonNullList;
        array.set(i, value);
    }

    public final void setOnce(int i, E value) {
        assert nonNullList == null : nonNullList;
        if (array.compareAndSet(i, null, value) == false) {
            throw new IllegalStateException("index [" + i + "] has already been set");
        }
    }

    /**
     * Gets the current value at position {@code i}.
     *
     * @param i the index
     * @return the current value
     */
    public E get(int i) {
        return array.get(i);
    }

    /**
     * Returns the it as a non null list.
     */
    public List<E> asList() {
        if (nonNullList == null) {
            if (array.length() == 0) {
                nonNullList = Collections.emptyList();
            } else {
                List<E> list = new ArrayList<>(array.length());
                for (int i = 0; i < array.length(); i++) {
                    E e = array.get(i);
                    if (e != null) {
                        list.add(e);
                    }
                }
                nonNullList = list;
            }
        }
        return nonNullList;
    }

    /**
     * Copies the content of the underlying atomic array to a normal one.
     */
    public E[] toArray(E[] a) {
        if (a.length != array.length()) {
            throw new ElasticsearchGenerationException("AtomicArrays can only be copied to arrays of the same size");
        }
        for (int i = 0; i < array.length(); i++) {
            a[i] = array.get(i);
        }
        return a;
    }

    public Stream<E> stream() {
        return IntStream.range(0, array.length()).mapToObj(array::get).filter(Objects::nonNull);
    }
}
