/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class SingleUseKeyTests extends ESTestCase {

    byte[] testKeyPlaintext;
    SecretKey testKey;
    String testKeyId;

    @Before
    public void setUpMocks() {
        testKeyPlaintext = randomByteArrayOfLength(32);
        testKey = new SecretKeySpec(testKeyPlaintext, "AES");
        testKeyId = randomAlphaOfLengthBetween(2, 32);
    }

    public void testNewKeySupplier() throws Exception {
        CheckedSupplier<SingleUseKey, IOException> singleUseKeySupplier = SingleUseKey.createSingleUseKeySupplier(
                () -> new Tuple<>(testKeyId, testKey));
        SingleUseKey generatedSingleUseKey = singleUseKeySupplier.get();
        assertThat(generatedSingleUseKey.getKeyId(), equalTo(testKeyId));
        assertThat(generatedSingleUseKey.getNonce(), equalTo(SingleUseKey.MIN_NONCE));
        assertThat(generatedSingleUseKey.getKey().getEncoded(), equalTo(testKeyPlaintext));
    }

    public void testNonceIncrement() throws Exception {
        int nonce = randomIntBetween(SingleUseKey.MIN_NONCE, SingleUseKey.MAX_NONCE - 2);
        SingleUseKey singleUseKey = new SingleUseKey(testKeyId, testKey, nonce);
        AtomicReference<SingleUseKey> keyCurrentlyInUse = new AtomicReference<>(singleUseKey);
        @SuppressWarnings("unchecked")
        CheckedSupplier<Tuple<String, SecretKey>, IOException> keyGenerator = mock(CheckedSupplier.class);
        CheckedSupplier<SingleUseKey, IOException> singleUseKeySupplier = SingleUseKey.createSingleUseKeySupplier(keyGenerator,
                keyCurrentlyInUse);
        SingleUseKey generatedSingleUseKey = singleUseKeySupplier.get();
        assertThat(generatedSingleUseKey.getKeyId(), equalTo(testKeyId));
        assertThat(generatedSingleUseKey.getNonce(), equalTo(nonce));
        assertThat(generatedSingleUseKey.getKey().getEncoded(), equalTo(testKeyPlaintext));
        SingleUseKey generatedSingleUseKey2 = singleUseKeySupplier.get();
        assertThat(generatedSingleUseKey2.getKeyId(), equalTo(testKeyId));
        assertThat(generatedSingleUseKey2.getNonce(), equalTo(nonce + 1));
        assertThat(generatedSingleUseKey2.getKey().getEncoded(), equalTo(testKeyPlaintext));
        verifyZeroInteractions(keyGenerator);
    }

    public void testNonceWrapAround() throws Exception {
    }
}
