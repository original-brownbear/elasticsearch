/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class EncryptedFSBlobStoreRepositoryIntegTests extends ESBlobStoreRepositoryIntegTestCase {

    private static int NUMBER_OF_TEST_REPOSITORIES = 32;
    private static List<String> repositoryNames = new ArrayList<>();
    private final AtomicBoolean fillInSecureSettings = new AtomicBoolean(true);

    @BeforeClass
    private static void preGenerateRepositoryNames() {
        for (int i = 0; i < NUMBER_OF_TEST_REPOSITORIES; i++) {
            repositoryNames.add("test-repo-" + i);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settingsBuilder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.getTypeName());
        if (fillInSecureSettings.get()) {
            settingsBuilder.setSecureSettings(nodeSecureSettings());
        }
        return settingsBuilder.build();
    }

    protected MockSecureSettings nodeSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        for (String repositoryName : repositoryNames) {
            byte[] repositoryNameBytes = repositoryName.getBytes(StandardCharsets.UTF_8);
            byte[] repositoryKEK = new byte[32];
            System.arraycopy(repositoryNameBytes, 0, repositoryKEK, 0, repositoryNameBytes.length);
            secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                    getConcreteSettingForNamespace(repositoryName).getKey(), repositoryKEK);
        }
        return secureSettings;
    }

    @Override
    protected String randomRepositoryName() {
        return repositoryNames.remove(randomIntBetween(0, repositoryNames.size() - 1));
    }

    @Override
    protected long blobLengthFromStorageLength(BlobMetaData blobMetaData) {
        return DecryptionPacketsInputStream.getDecryptionLength(blobMetaData.length() -
                EncryptedRepository.DEK_ID_LENGTH_IN_BYTES, EncryptedRepository.PACKET_LENGTH_IN_BYTES);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateEncryptedRepositoryPlugin.class);
    }

    @Override
    protected String repositoryType() {
        return EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME;
    }

    @Override
    protected Settings repositorySettings(String repositoryName) {
        final Settings.Builder settings = Settings.builder();
        settings.put(super.repositorySettings());
        settings.put("location", randomRepoPath());
        settings.put(EncryptedRepositoryPlugin.DELEGATE_TYPE_SETTING.getKey(), FsRepository.TYPE);
        settings.put(EncryptedRepositoryPlugin.KEK_NAME_SETTING.getKey(), repositoryName);
        if (randomBoolean()) {
            long size = 1 << randomInt(10);
            settings.put("chunk_size", new ByteSizeValue(size, ByteSizeUnit.KB));
        }
        return settings.build();
    }

    public void testRepositoryVerificationFailsForDifferentKEK() throws Exception {
        final boolean restoreFillInSecureSettings = fillInSecureSettings.get();
        String newNode = null;
        try {
            // do not use the default secure settings because the newly started node must use different ones
            fillInSecureSettings.set(false);
            String repositoryName = randomRepositoryName();
            // generate a different repository KEK
            byte[] repositoryKEKOnNewNode = randomByteArrayOfLength(32);
            repositoryKEKOnNewNode[0] = 0; // be absolutely sure the KEK is different
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                    getConcreteSettingForNamespace(repositoryName).getKey(), repositoryKEKOnNewNode);
            Settings settingsOfNewNode = Settings.builder().setSecureSettings(secureSettings).build();
            // start new node with different repository KEK
            int nodesCount = cluster().size();
            newNode = internalCluster().startNode(settingsOfNewNode);
            ensureStableCluster(nodesCount + 1);
            // repository create fails verification
            expectThrows(RepositoryVerificationException.class,
                    () -> client().admin().cluster().preparePutRepository(repositoryName)
                    .setType(repositoryType())
                    .setVerify(true)
                    .setSettings(repositorySettings(repositoryName)).get());
            // test verify call fails
            expectThrows(RepositoryVerificationException.class,
                    () -> client().admin().cluster().prepareVerifyRepository(repositoryName).get());
            // stop the node with the wrong DEK
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(newNode));
            ensureStableCluster(nodesCount);
            // start another node with a correct DEK
            byte[] repositoryNameBytes = repositoryName.getBytes(StandardCharsets.UTF_8);
            byte[] repositoryKEK = new byte[32];
            System.arraycopy(repositoryNameBytes, 0, repositoryKEK, 0, repositoryNameBytes.length);
            secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                    getConcreteSettingForNamespace(repositoryName).getKey(), repositoryKEK);
            settingsOfNewNode = Settings.builder().setSecureSettings(secureSettings).build();
            newNode = internalCluster().startNode(settingsOfNewNode);
            ensureStableCluster(nodesCount + 1);
            // repository verification now succeeds
            VerifyRepositoryResponse verifyRepositoryResponse = client().admin().cluster().prepareVerifyRepository(repositoryName).get();
            String finalNewNode = newNode;
            assertTrue(verifyRepositoryResponse.getNodes().stream().anyMatch(node -> node.getName().equals(finalNewNode)));
        } finally {
            fillInSecureSettings.set(restoreFillInSecureSettings);
            if (newNode != null) {
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(newNode));
            }
        }
    }

}
