/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public final class EncryptedFSBlobStoreRepositoryIntegTests extends ESBlobStoreRepositoryIntegTestCase {

    private static List<String> repositoryNames = new ArrayList<>();

    @BeforeClass
    private static void preGenerateRepositoryNames() {
        for (int i = 0; i < 32; i++) {
            repositoryNames.add("test-repo-" + i);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.getTypeName())
                .setSecureSettings(nodeSecureSettings(nodeOrdinal))
                .build();
    }

    protected MockSecureSettings nodeSecureSettings(int nodeOrdinal) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        for (String repositoryName : repositoryNames) {
            byte[] repositoryNameBytes = repositoryName.getBytes(StandardCharsets.UTF_8);
            byte[] repositoryKek = new byte[32];
            System.arraycopy(repositoryNameBytes, 0, repositoryKek, 0, repositoryNameBytes.length);
            secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                    getConcreteSettingForNamespace(repositoryName).getKey(), repositoryKek);
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

}
