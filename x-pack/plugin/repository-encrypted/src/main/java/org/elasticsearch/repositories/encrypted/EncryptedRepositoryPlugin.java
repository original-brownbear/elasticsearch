/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class EncryptedRepositoryPlugin extends Plugin implements RepositoryPlugin {
    static final Logger logger = LogManager.getLogger(EncryptedRepositoryPlugin.class);
    static final String REPOSITORY_TYPE_NAME = "encrypted";
    static final List<String> SUPPORTED_ENCRYPTED_TYPE_NAMES = Arrays.asList("fs", "gcs", "azure", "s3");
    static final Setting.AffixSetting<SecureString> ENCRYPTION_PASSWORD_SETTING = Setting.affixKeySetting("repository.encrypted.",
            "password", key -> SecureSetting.secureString(key, null));
    static final Setting<String> DELEGATE_TYPE_SETTING = Setting.simpleString("delegate_type", "");
    static final Setting<String> PASSWORD_NAME_SETTING = Setting.simpleString("password_name", "");

    // "protected" because it is overloaded for tests
    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    public EncryptedRepositoryPlugin() {
        if (false == getLicenseState().isEncryptedSnapshotAllowed()) {
            logger.warn("Snapshotting to an encrypted repository is not permitted for the current license." +
                            "All the other operations over the encrypted repository, eg. restore, work without restrictions.",
                    LicenseUtils.newComplianceException("encrypted snapshots"));
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ENCRYPTION_PASSWORD_SETTING);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env,
                                                           NamedXContentRegistry registry,
                                                           ClusterService clusterService) {
        // load all the passwords from the keystore in memory because the keystore is not readable when the repository is created
        final Map<String, char[]> repositoryPasswordsMapBuilder = new HashMap<>();
        for (String passwordName : ENCRYPTION_PASSWORD_SETTING.getNamespaces(env.settings())) {
            Setting<SecureString> passwordSetting = ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(passwordName);
            SecureString encryptionPassword = passwordSetting.get(env.settings());
            repositoryPasswordsMapBuilder.put(passwordName, encryptionPassword.getChars());
            logger.debug(() -> new ParameterizedMessage("Loaded repository password [" + passwordName + "] from the node keystore"));
        }
        final Map<String, char[]> repositoryPasswordsMap = Map.copyOf(repositoryPasswordsMapBuilder);

        return Collections.singletonMap(REPOSITORY_TYPE_NAME, new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetaData metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetaData metaData, Function<String, Repository.Factory> typeLookup) throws Exception {
                final String delegateType = DELEGATE_TYPE_SETTING.get(metaData.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException("Repository setting [" + DELEGATE_TYPE_SETTING.getKey() + "] must be set");
                }
                if (REPOSITORY_TYPE_NAME.equals(delegateType)) {
                    throw new IllegalArgumentException("Cannot encrypt an already encrypted repository. [" +
                            DELEGATE_TYPE_SETTING.getKey() + "] must not be equal to [" + REPOSITORY_TYPE_NAME + "]");
                }
                final Repository.Factory factory = typeLookup.apply(delegateType);
                if (null == factory || false == SUPPORTED_ENCRYPTED_TYPE_NAMES.contains(delegateType)) {
                    throw new IllegalArgumentException("Unsupported delegate repository type [" + DELEGATE_TYPE_SETTING.getKey() + "]");
                }
                final String repositoryPasswordName = PASSWORD_NAME_SETTING.get(metaData.settings());
                if (Strings.hasLength(repositoryPasswordName) == false) {
                    throw new IllegalArgumentException("Repository setting [" + PASSWORD_NAME_SETTING.getKey() + "] must be set");
                }
                final char[] repositoryPassword = repositoryPasswordsMap.get(repositoryPasswordName);
                if (repositoryPassword == null) {
                    throw new IllegalArgumentException("Secure setting [" +
                            ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryPasswordName).getKey() + "] must be set");
                }
                final Repository delegatedRepository = factory.create(new RepositoryMetaData(metaData.name(), delegateType,
                        metaData.settings()));
                if (false == (delegatedRepository instanceof BlobStoreRepository)
                        || delegatedRepository instanceof EncryptedRepository) {
                    throw new IllegalArgumentException("Unsupported delegate repository type [" + DELEGATE_TYPE_SETTING.getKey() + "]");
                }
                if (false == getLicenseState().isEncryptedSnapshotAllowed()) {
                    logger.warn("Encrypted snapshots are not allowed for the currently installed license." +
                                    "Snapshots to the [" + metaData.name() + "] encrypted repository are not permitted." +
                                    "All the other operations, including restore, work without restrictions.",
                            LicenseUtils.newComplianceException("encrypted snapshots"));
                }
                return new EncryptedRepository(metaData, registry, clusterService, (BlobStoreRepository) delegatedRepository,
                        () -> getLicenseState(), repositoryPassword);
            }
        });
    }
}
