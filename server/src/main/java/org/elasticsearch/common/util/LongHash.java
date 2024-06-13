/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.core.Releasable;

/**
 * Specialized hash table implementation similar to BytesRefHash that maps
 * long values to ids. Collisions are resolved with open addressing and linear
 * probing, growth is smooth thanks to {@link BigArrays} and capacity is always
 * a multiple of 2 for faster identification of buckets.
 * This class is not thread-safe.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
public final class LongHash extends AbstractHash {

    private LongArray keys;

    // Constructor with configurable capacity and default maximum load factor.
    public LongHash(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    // Constructor with configurable capacity and load factor.
    public LongHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        try {
            // `super` allocates a big array so we have to `close` if we fail here or we'll leak it.
            keys = bigArrays.newLongArray(capacity, false);
        } finally {
            if (keys == null) {
                close();
            }
        }
    }

    /**
     * Return the key at <code>0 &lt;= index &lt;= capacity()</code>. The result is undefined if the slot is unused.
     */
    public long get(long id) {
        return keys.get(id);
    }

    /**
     * Get the id associated with <code>key</code> or -1 if the key is not contained in the hash.
     */
    public long find(long key) {
        final long slot = slot(hash(key), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long id = id(index);
            if (id == -1 || keys.get(id) == key) {
                return id;
            }
        }
    }

    private long set(long key, long id) {
        final long maskRef = mask;
        final var keysRef = keys;
        final var idsRef = ids;
        for (long index = slot(hash(key), maskRef);; index = nextSlot(index, maskRef)) {
            final long curId = idsRef.get(index);
            if (curId == 0) { // means unset
                return setOverUnset(key, id, index);
            }
            if (keysRef.get(curId - 1) == key) {
                return -curId;
            }
        }
    }

    private long setOverUnset(long key, long id, long index) {
        id(index, id);
        append(id, key);
        ++size;
        return id;
    }

    private void append(long id, long key) {
        keys = bigArrays.grow(keys, id + 1);
        keys.set(id, key);
    }

    private void reset(long key, long id) {
        final long slot = slot(hash(key), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                id(index, id);
                append(id, key);
                break;
            }
        }
    }

    /**
     * Try to add <code>key</code>. Return its newly allocated id if it wasn't in the hash table yet, or <code>-1-id</code>
     * if it was already present in the hash table.
     */
    public long add(long key) {
        if (size >= maxSize) {
            grow();
        }
        return set(key, size);
    }

    @Override
    protected void removeAndAdd(long index) {
        final long id = id(index, -1);
        assert id >= 0;
        final long key = keys.set(id, 0);
        reset(key, id);
    }

    @Override
    public void close() {
        try (Releasable releasable = keys) {
            super.close();
        }
    }

}
