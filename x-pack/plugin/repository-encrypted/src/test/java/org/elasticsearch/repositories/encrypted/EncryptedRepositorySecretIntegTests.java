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
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public final class EncryptedRepositorySecretIntegTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateEncryptedRepositoryPlugin.class);
    }


    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.getTypeName())
                .build();
    }

    public void testRepositoryCreationFailsForMissingPassword() throws Exception {
        // if the password is missing on the master node, the repository creation fails
        final String repositoryName = randomName();
        MockSecureSettings secureSettingsWithPassword = new MockSecureSettings();
        secureSettingsWithPassword.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                getConcreteSettingForNamespace(repositoryName).getKey(), randomAlphaOfLength(20));
        logger.info("--> start 3 nodes");
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNodeName = internalCluster().startNode();
        logger.info("--> started master node " + masterNodeName);
        ensureStableCluster(1);
        internalCluster().startNodes(2, Settings.builder().setSecureSettings(secureSettingsWithPassword).build());
        logger.info("--> started two other nodes");
        ensureStableCluster(3);
        assertThat(masterNodeName, equalTo(internalCluster().getMasterName()));

        final Settings repositorySettings = repositorySettings(repositoryName);
        RepositoryException e = expectThrows(RepositoryException.class,
                () -> client().admin().cluster().preparePutRepository(repositoryName)
                        .setType(repositoryType())
                        .setVerify(randomBoolean())
                        .setSettings(repositorySettings).get());
        assertThat(e.getMessage(), containsString("failed to create repository"));
        expectThrows(RepositoryMissingException.class, () -> client().admin().cluster().prepareGetRepositories(repositoryName).get());

        if (randomBoolean()) {
            // stop the node with the missing password
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodeName));
            ensureStableCluster(2);
        } else {
            // restart the node with the missing password
            internalCluster().restartNode(masterNodeName, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    Settings.Builder newSettings = Settings.builder().put(super.onNodeStopped(nodeName));
                    newSettings.setSecureSettings(secureSettingsWithPassword);
                    return newSettings.build();
                }
            });
            ensureStableCluster(3);
        }
        // repository creation now successful
        createRepository(repositoryName, repositorySettings, true);
    }

    public void testRepositoryVerificationFailsForMissingPassword() throws Exception {
        // if the password is missing on any non-master node, the repository verification fails
        final String repositoryName = randomName();
        MockSecureSettings secureSettingsWithPassword = new MockSecureSettings();
        secureSettingsWithPassword.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                getConcreteSettingForNamespace(repositoryName).getKey(), randomAlphaOfLength(20));
        logger.info("--> start 2 nodes");
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNodeName = internalCluster().startNode(Settings.builder().setSecureSettings(secureSettingsWithPassword).build());
        logger.info("--> started master node " + masterNodeName);
        ensureStableCluster(1);
        final String otherNodeName = internalCluster().startNode();
        logger.info("--> started other node " + otherNodeName);
        ensureStableCluster(2);
        assertThat(masterNodeName, equalTo(internalCluster().getMasterName()));
        // repository create fails verification
        final Settings repositorySettings = repositorySettings(repositoryName);
        expectThrows(RepositoryVerificationException.class,
                () -> client().admin().cluster().preparePutRepository(repositoryName)
                        .setType(repositoryType())
                        .setVerify(true)
                        .setSettings(repositorySettings).get());
        if (randomBoolean()) {
            // delete and recreate repo
            logger.debug("-->  deleting repository [name: {}]", repositoryName);
            assertAcked(client().admin().cluster().prepareDeleteRepository(repositoryName));
            assertAcked(client().admin().cluster().preparePutRepository(repositoryName)
                    .setType(repositoryType())
                    .setVerify(false)
                    .setSettings(repositorySettings).get());
        }
        // test verify call fails
        expectThrows(RepositoryVerificationException.class,
                () -> client().admin().cluster().prepareVerifyRepository(repositoryName).get());
        if (randomBoolean()) {
            // stop the node with the missing password
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(otherNodeName));
            ensureStableCluster(1);
        } else {
            // restart the node with the missing password
            internalCluster().restartNode(otherNodeName, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    Settings.Builder newSettings = Settings.builder().put(super.onNodeStopped(nodeName));
                    newSettings.setSecureSettings(secureSettingsWithPassword);
                    return newSettings.build();
                }
            });
            ensureStableCluster(2);
        }
        // repository verification now succeeds
        client().admin().cluster().prepareVerifyRepository(repositoryName).get();
    }

    protected String randomName() {
        return randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    }

    protected String repositoryType() {
        return EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME;
    }

    protected Settings repositorySettings(String repositoryName) {
        return Settings.builder()
                .put("compress", randomBoolean())
                .put(EncryptedRepositoryPlugin.DELEGATE_TYPE_SETTING.getKey(), FsRepository.TYPE)
                .put(EncryptedRepositoryPlugin.PASSWORD_NAME_SETTING.getKey(), repositoryName)
                .put("location", randomRepoPath())
                .build();
    }

    protected String createRepository(final String name) {
        return createRepository(name, true);
    }

    protected String createRepository(final String name, final boolean verify) {
        return createRepository(name, repositorySettings(name), verify);
    }

    protected String createRepository(final String name, final Settings settings, final boolean verify) {
        logger.debug("-->  creating repository [name: {}, verify: {}, settings: {}]", name, verify, settings);
        assertAcked(client().admin().cluster().preparePutRepository(name)
                .setType(repositoryType())
                .setVerify(verify)
                .setSettings(settings));

        internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class).forEach(repositories -> {
            assertThat(repositories.repository(name), notNullValue());
            assertThat(repositories.repository(name), instanceOf(BlobStoreRepository.class));
            assertThat(repositories.repository(name).isReadOnly(), is(settings.getAsBoolean("readonly", false)));
        });

        return name;
    }

    protected void deleteRepository(final String name) {
        logger.debug("-->  deleting repository [name: {}]", name);
        assertAcked(client().admin().cluster().prepareDeleteRepository(name));

        internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class).forEach(repositories -> {
            RepositoryMissingException e = expectThrows(RepositoryMissingException.class, () -> repositories.repository(name));
            assertThat(e.getMessage(), containsString(name));
        });
    }
}
