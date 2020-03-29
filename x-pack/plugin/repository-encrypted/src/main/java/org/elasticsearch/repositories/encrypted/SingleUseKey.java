/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.collect.Tuple;

import javax.crypto.SecretKey;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Container class for a {@code SecretKey} with a unique identifier, and a 4-byte wide {@code Integer} nonce, that can be used for a
 * single encryption operation. Use {@link #createSingleUseKeySupplier(CheckedSupplier)} to obtain a {@code Supplier} that returns
 * a new {@link SingleUseKey} instance on every invocation. The number of unique {@code SecretKey}s (and their associated identifiers)
 * generated is minimized and, at the same time, ensuring that a given {@code nonce} is not reused with the same key.
 */
final class SingleUseKey {
    private static final Logger logger = LogManager.getLogger(SingleUseKey.class);
    static final int MIN_NONCE = Integer.MIN_VALUE;
    static final int MAX_NONCE = Integer.MAX_VALUE;
    private static final int MAX_ATTEMPTS = 9;
    private static final SingleUseKey EXPIRED_KEY = new SingleUseKey(null, null, MAX_NONCE);

    private final String KeyId;
    private final SecretKey Key;
    private final Integer nonce;

    // for tests use only!
    SingleUseKey(String KeyId, SecretKey Key, Integer nonce) {
        this.KeyId = KeyId;
        this.Key = Key;
        this.nonce = nonce;
    }

    public String getKeyId() {
        return KeyId;
    }

    public SecretKey getKey() {
        return Key;
    }

    public Integer getNonce() {
        return nonce;
    }

    /**
     * Returns a {@code Supplier} of {@code SingleUseKey}s so that no two instances contain the same key and nonce pair.
     * A new key is generated only when the {@code nonce} space has been exhausted.
     */
    static <T extends Exception> CheckedSupplier<SingleUseKey, T> createSingleUseKeySupplier(
            CheckedSupplier<Tuple<String, SecretKey>, T> keyGenerator) {
        final AtomicReference<SingleUseKey> keyCurrentlyInUse = new AtomicReference<>(EXPIRED_KEY);
        return createSingleUseKeySupplier(keyGenerator, keyCurrentlyInUse);
    }

    // for tests use only
    static <T extends Exception> CheckedSupplier<SingleUseKey, T> createSingleUseKeySupplier(
            CheckedSupplier<Tuple<String, SecretKey>, T> keyGenerator, AtomicReference<SingleUseKey> keyCurrentlyInUse) {
        final Object lock = new Object();
        return () -> {
            for (int attemptNo = 0; attemptNo < MAX_ATTEMPTS; attemptNo++) {
                final SingleUseKey nonceAndKey = keyCurrentlyInUse.getAndUpdate(prev -> prev.nonce < MAX_NONCE ?
                        new SingleUseKey(prev.KeyId, prev.Key, prev.nonce + 1) : EXPIRED_KEY);
                if (nonceAndKey.nonce < MAX_NONCE) {
                    logger.trace(() -> new ParameterizedMessage("Key with id [{}] reused with nonce [{}]", nonceAndKey.KeyId,
                            nonceAndKey.nonce));
                    return nonceAndKey;
                } else {
                    logger.trace(() -> new ParameterizedMessage("Generating a new key to replace the key with id [{}]", nonceAndKey.KeyId));
                    synchronized (lock) {
                        if (keyCurrentlyInUse.get().nonce == MAX_NONCE) {
                            final Tuple<String, SecretKey> newKey = keyGenerator.get();
                            logger.debug(() -> new ParameterizedMessage("New key with id [{}] has been generated", newKey.v1()));
                            keyCurrentlyInUse.set(new SingleUseKey(newKey.v1(), newKey.v2(), MIN_NONCE));
                        }
                    }
                }
            }
            throw new IllegalStateException("Failure to generate new key");
        };
    }
}
