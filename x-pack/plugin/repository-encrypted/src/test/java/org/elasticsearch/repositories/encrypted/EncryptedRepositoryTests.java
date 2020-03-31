/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EncryptedRepositoryTests extends ESTestCase {

    private char[] repoPassword;
    private BlobPath delegatedPath;
    private BlobStore delegatedBlobStore;
    private BlobStoreRepository delegatedRepository;
    private RepositoryMetaData repositoryMetaData;
    private EncryptedRepository encryptedRepository;
    private EncryptedRepository.EncryptedBlobStore encryptedBlobStore;
    private Map<BlobPath, byte[]> blobsMap;

    @Before
    public void setUpMocks() throws Exception {
        this.repoPassword = randomAlphaOfLength(20).toCharArray();
        this.delegatedPath = randomFrom(
            BlobPath.cleanPath(),
            BlobPath.cleanPath().add(randomAlphaOfLength(8)),
            BlobPath.cleanPath().add(randomAlphaOfLength(4)).add(randomAlphaOfLength(4))
        );
        this.delegatedBlobStore = mock(BlobStore.class);
        this.delegatedRepository = mock(BlobStoreRepository.class);
        when(delegatedRepository.blobStore()).thenReturn(delegatedBlobStore);
        when(delegatedRepository.basePath()).thenReturn(delegatedPath);
        this.repositoryMetaData = new RepositoryMetaData(
            randomAlphaOfLength(4),
            EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME,
            Settings.EMPTY
        );
        ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.threadPool()).thenReturn(mock(ThreadPool.class));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        this.encryptedRepository = new EncryptedRepository(
            repositoryMetaData,
            mock(NamedXContentRegistry.class),
            clusterService,
            delegatedRepository,
            () -> mock(XPackLicenseState.class),
            repoPassword
        );
        this.encryptedBlobStore = (EncryptedRepository.EncryptedBlobStore) encryptedRepository.createBlobStore();
        this.blobsMap = new HashMap<>();
        doAnswer(invocationOnMockBlobStore -> {
            BlobPath blobPath = ((BlobPath) invocationOnMockBlobStore.getArguments()[0]);
            BlobContainer blobContainer = mock(BlobContainer.class);
            // write atomic
            doAnswer(invocationOnMockBlobContainer -> {
                String DEKId = ((String) invocationOnMockBlobContainer.getArguments()[0]);
                InputStream DEKInputStream = ((InputStream) invocationOnMockBlobContainer.getArguments()[1]);
                this.blobsMap.put(blobPath.add(DEKId), BytesReference.toBytes(Streams.readFully(DEKInputStream)));
                return null;
            }).when(blobContainer).writeBlobAtomic(any(String.class), any(InputStream.class), anyLong(), anyBoolean());
            // read
            doAnswer(invocationOnMockBlobContainer -> {
                String DEKId = ((String) invocationOnMockBlobContainer.getArguments()[0]);
                return new ByteArrayInputStream(blobsMap.get(blobPath.add(DEKId)));
            }).when(blobContainer).readBlob(any(String.class));
            return blobContainer;
        }).when(this.delegatedBlobStore).blobContainer(any(BlobPath.class));
    }

    public void testStoreDEKSuccess() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(2, 20);
        SecretKey DEK = new SecretKeySpec(randomByteArrayOfLength(32), "AES");

        encryptedBlobStore.storeDEK(DEKId, DEK);

        Tuple<String, SecretKey> KEK = encryptedRepository.generateKEK(DEKId);
        assertThat(blobsMap.keySet(), contains(delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER).add(DEKId).add(KEK.v1())));
        byte[] wrappedKey = blobsMap.values().iterator().next();
        SecretKey unwrappedKey = AESKeyUtils.unwrap(KEK.v2(), wrappedKey);
        assertThat(unwrappedKey.getEncoded(), equalTo(DEK.getEncoded()));
    }

    public void testGetDEKSuccess() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(2, 20);
        SecretKey DEK = new SecretKeySpec(randomByteArrayOfLength(32), "AES");
        Tuple<String, SecretKey> KEK = encryptedRepository.generateKEK(DEKId);

        byte[] wrappedDEK = AESKeyUtils.wrap(KEK.v2(), DEK);
        blobsMap.put(delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER).add(DEKId).add(KEK.v1()), wrappedDEK);

        SecretKey loadedDEK = encryptedBlobStore.getDEKById(DEKId);
        assertThat(loadedDEK.getEncoded(), equalTo(DEK.getEncoded()));
    }

    public void testGetTamperedDEKFails() throws Exception {
        String DEKId = randomAlphaOfLengthBetween(2, 20);
        SecretKey DEK = new SecretKeySpec("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8), "AES");
        Tuple<String, SecretKey> KEK = encryptedRepository.generateKEK(DEKId);

        byte[] wrappedDEK = AESKeyUtils.wrap(KEK.v2(), DEK);
        int tamperPos = randomIntBetween(0, wrappedDEK.length - 1);
        wrappedDEK[tamperPos] ^= 0xFF;
        blobsMap.put(delegatedPath.add(EncryptedRepository.DEK_ROOT_CONTAINER).add(DEKId).add(KEK.v1()), wrappedDEK);

        RepositoryException e = expectThrows(RepositoryException.class, () -> encryptedBlobStore.getDEKById(DEKId));
        assertThat(e.repository(), equalTo(repositoryMetaData.name()));
        assertThat(e.getMessage(), containsString("Failure to AES unwrap the DEK"));
    }

    public void testGetDEKIOException() {
        doAnswer(invocationOnMockBlobStore -> {
            BlobPath blobPath = ((BlobPath) invocationOnMockBlobStore.getArguments()[0]);
            BlobContainer blobContainer = mock(BlobContainer.class);
            // read
            doAnswer(invocationOnMockBlobContainer -> { throw new IOException("Tested IOException"); }).when(blobContainer)
                .readBlob(any(String.class));
            return blobContainer;
        }).when(this.delegatedBlobStore).blobContainer(any(BlobPath.class));
        IOException e = expectThrows(IOException.class, () -> encryptedBlobStore.getDEKById("id"));
        assertThat(e.getMessage(), containsString("Tested IOException"));
    }

    public void testGenerateKEK() {
        String id1 = "fixed identifier 1";
        String id2 = "fixed identifier 2";
        Tuple<String, SecretKey> KEK1 = encryptedRepository.generateKEK(id1);
        Tuple<String, SecretKey> KEK2 = encryptedRepository.generateKEK(id2);
        assertThat(KEK1.v1(), not(equalTo(KEK2.v1())));
        assertThat(KEK1.v2(), not(equalTo(KEK2.v2())));
        Tuple<String, SecretKey> sameKEK1 = encryptedRepository.generateKEK(id1);
        assertThat(KEK1.v1(), equalTo(sameKEK1.v1()));
        assertThat(KEK1.v2(), equalTo(sameKEK1.v2()));
    }

}
