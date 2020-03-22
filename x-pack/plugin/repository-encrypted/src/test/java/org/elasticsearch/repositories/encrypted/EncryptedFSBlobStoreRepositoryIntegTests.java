/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.fs.FsBlobStoreRepositoryBaseIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.repositories.encrypted.EncryptedRepository.getEncryptedBlobByteLength;

public final class EncryptedFSBlobStoreRepositoryIntegTests extends FsBlobStoreRepositoryBaseIntegTestCase {

    private static int NUMBER_OF_TEST_REPOSITORIES = 32;
    private static List<String> repositoryNames = new ArrayList<>();

    @BeforeClass
    private static void preGenerateRepositoryNames() {
        for (int i = 0; i < NUMBER_OF_TEST_REPOSITORIES; i++) {
            repositoryNames.add("test-repo-" + i);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.getTypeName())
                .setSecureSettings(nodeSecureSettings())
                .build();
    }

    protected MockSecureSettings nodeSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        for (String repositoryName : repositoryNames) {
            secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                    getConcreteSettingForNamespace(repositoryName).getKey(), repositoryName);
        }
        return secureSettings;
    }

    @Override
    protected String randomRepositoryName() {
        return repositoryNames.remove(randomIntBetween(0, repositoryNames.size() - 1));
    }

    @Override
    protected long blobLengthFromContentLength(long contentLength) {
        return getEncryptedBlobByteLength(contentLength);
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
        return Settings.builder()
                .put(super.repositorySettings(repositoryName))
                .put(EncryptedRepositoryPlugin.DELEGATE_TYPE_SETTING.getKey(), FsRepository.TYPE)
                .put(EncryptedRepositoryPlugin.PASSWORD_NAME_SETTING.getKey(), repositoryName)
                .build();
    }
}
