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

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class EncryptedRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static final Logger logger = LogManager.getLogger(EncryptedRepositoryPlugin.class);
    static final String REPOSITORY_TYPE_NAME = "encrypted";
    static final Setting.AffixSetting<InputStream> KEY_ENCRYPTION_KEY_SETTING = Setting.affixKeySetting("repository.encrypted.",
            "key", key -> SecureSetting.secureFile(key, null));
    static final Setting<String> DELEGATE_TYPE_SETTING = Setting.simpleString("delegate_type", "");
    static final Setting<String> KEK_NAME_SETTING = Setting.simpleString("key_name", "");
    static final String KEK_CIPHER_ALGO = "AES";
    static final int KEK_LENGTH_IN_BYTES = 32; // 256-bit AES symmetric key

    public static byte[] wrapAESKey(SecretKey wrappingKey, Key toWrapKey) throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidKeyException, IllegalBlockSizeException {
        if (false == "AES".equals(wrappingKey.getAlgorithm())) {
            throw new IllegalArgumentException("wrappingKey argument is not an AES Key");
        }
        if (false == "AES".equals(toWrapKey.getAlgorithm())) {
            throw new IllegalArgumentException("toWrapKey argument is not an AES Key");
        }
        Cipher c = Cipher.getInstance("AESWrap");
        c.init(Cipher.WRAP_MODE, wrappingKey);
        return c.wrap(toWrapKey);
    }

    public static SecretKey unwrapAESKey(SecretKey wrappingKey, byte[] wrappedKey) throws NoSuchPaddingException,
            NoSuchAlgorithmException,
            InvalidKeyException {
        if (false == "AES".equals(wrappingKey.getAlgorithm())) {
            throw new IllegalArgumentException("wrappingKey argument is not an AES Key");
        }
        Cipher c = Cipher.getInstance("AESWrap");
        c.init(Cipher.UNWRAP_MODE, wrappingKey);
        Key unwrappedKey = c.unwrap(wrappedKey, "AES", Cipher.SECRET_KEY);
        return new SecretKeySpec(unwrappedKey.getEncoded(), "AES"); // make sure unwrapped key is "AES"
    }

    // "protected" because it is overloaded for tests
    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    public EncryptedRepositoryPlugin() {
        if (false == getLicenseState().isEncryptedSnapshotAllowed()) {
            logger.warn("Snapshotting to an encrypted repository is not permitted for the current license." +
                            "All the other operations over the encrypted repository, eg. restore, function without restrictions.",
                    LicenseUtils.newComplianceException("encrypted snapshots"));
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(KEY_ENCRYPTION_KEY_SETTING);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env,
                                                           NamedXContentRegistry registry,
                                                           ClusterService clusterService) {
        // store all KEKs from the keystore in memory (because the keystore is not readable later on)
        final Map<String, SecretKey> repositoryKEKMapBuilder = new HashMap<>();
        for (String KEKName : KEY_ENCRYPTION_KEY_SETTING.getNamespaces(env.settings())) {
            final Setting<InputStream> KEKSetting = KEY_ENCRYPTION_KEY_SETTING.getConcreteSettingForNamespace(KEKName);
            final SecretKey KEK;
            byte[] encodedKEKBytes = null;
            try (InputStream KEKInputStream = KEKSetting.get(env.settings())) {
                encodedKEKBytes = KEKInputStream.readAllBytes();
                if (encodedKEKBytes.length != KEK_LENGTH_IN_BYTES) {
                    throw new IllegalArgumentException("Expected a 32 bytes (256 bit) wide AES key, but key ["
                            + KEKName + "] is [" + encodedKEKBytes.length + "] bytes wide");
                }
                KEK = new SecretKeySpec(encodedKEKBytes, 0, KEK_LENGTH_IN_BYTES, KEK_CIPHER_ALGO);
            } catch (IOException e) {
                throw new UncheckedIOException("Exception while reading [" + KEKName + "] from the node keystore", e);
            } finally {
                if (encodedKEKBytes != null) {
                    Arrays.fill(encodedKEKBytes, (byte) 0);
                }
            }
            logger.debug(() -> new ParameterizedMessage("Loaded repository AES key [" + KEKName + "] from the node keystore"));
            repositoryKEKMapBuilder.put(KEKName, KEK);
        }
        final Map<String, SecretKey> repositoryKEKMap = Map.copyOf(repositoryKEKMapBuilder);

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
                if (null == factory) {
                    throw new IllegalArgumentException("Unrecognized delegate repository type [" + DELEGATE_TYPE_SETTING.getKey() + "]");
                }
                final String repositoryKEKName = KEK_NAME_SETTING.get(metaData.settings());
                if (Strings.hasLength(repositoryKEKName) == false) {
                    throw new IllegalArgumentException("Repository setting [" + KEK_NAME_SETTING.getKey() + "] must be set");
                }
                final SecretKey repositoryKEK = repositoryKEKMap.get(repositoryKEKName);
                if (repositoryKEK == null) {
                    throw new IllegalArgumentException("Secure setting [" +
                            KEY_ENCRYPTION_KEY_SETTING.getConcreteSettingForNamespace(repositoryKEKName).getKey() + "] must be set");
                }
                final Repository delegatedRepository = factory.create(new RepositoryMetaData(metaData.name(), delegateType,
                        metaData.settings()));
                if (false == (delegatedRepository instanceof BlobStoreRepository)
                        || delegatedRepository instanceof EncryptedRepository) {
                    throw new IllegalArgumentException("Unsupported delegate type [" + DELEGATE_TYPE_SETTING.getKey() + "]");
                }
                if (false == getLicenseState().isEncryptedSnapshotAllowed()) {
                    logger.warn("Encrypted snapshots are not allowed for the currently installed license." +
                                    "Snapshots to the [" + metaData.name() + "] encrypted repository are not permitted." +
                                    "All the other operations, including restore, function without restrictions.",
                            LicenseUtils.newComplianceException("encrypted snapshots"));
                }
                return new EncryptedRepository(metaData, registry, clusterService, (BlobStoreRepository) delegatedRepository,
                        () -> getLicenseState(), repositoryKEK);
            }
        });
    }
}
