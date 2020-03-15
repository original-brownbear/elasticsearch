/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsBlobStoreRepositoryBaseIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public final class EncryptedFSBlobStoreRepositoryIntegTests extends FsBlobStoreRepositoryBaseIntegTestCase {

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
        return Settings.builder()
                .put(super.repositorySettings())
                .put(EncryptedRepositoryPlugin.DELEGATE_TYPE_SETTING.getKey(), FsRepository.TYPE)
                .put(EncryptedRepositoryPlugin.KEK_NAME_SETTING.getKey(), repositoryName)
                .build();
    }

    public void testRepositoryVerificationFailsForMissingKEK() throws Exception {
        String repositoryName = randomRepositoryName();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                getConcreteSettingForNamespace(repositoryName).getKey(), null);
        Settings settingsOfNewNode = Settings.builder().setSecureSettings(secureSettings).build();
        // start new node with missing repository KEK
        int nodesCount = cluster().size();
        String newNode = internalCluster().startNode(settingsOfNewNode);
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
        // stop the node with the missing KEK
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(newNode));
        ensureStableCluster(nodesCount);
        // repository verification now succeeds
        client().admin().cluster().prepareVerifyRepository(repositoryName).get();
    }

    public void testRepositoryVerificationFailsForDifferentKEK() throws Exception {
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
        String newNode = internalCluster().startNode(settingsOfNewNode);
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
        // stop the node with the wrong KEK
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(newNode));
        ensureStableCluster(nodesCount);
        // start another node with a correct KEK
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
    }

    public void testSnapshotFailsForDifferentKEK() throws Exception {
        String repoBeforeNewNode = randomRepositoryName();
        // create repository before adding the new node
        createRepository(repoBeforeNewNode, repositorySettings(repoBeforeNewNode), true);
        String repoAfterNewNode = randomRepositoryName();
        // generate a different repository KEK
        byte[] repositoryKEKOnNewNode = randomByteArrayOfLength(32);
        repositoryKEKOnNewNode[0] = 0; // be absolutely sure the KEK is different
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                getConcreteSettingForNamespace(repoAfterNewNode).getKey(), repositoryKEKOnNewNode);
        secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                getConcreteSettingForNamespace(repoBeforeNewNode).getKey(), repositoryKEKOnNewNode);
        Settings settingsOfNewNode = Settings.builder().put("node.attr.repoKEK", "wrong").setSecureSettings(secureSettings).build();
        // start a new node with different repository KEKs
        int nodesCount = cluster().size();
        String newNode = internalCluster().startNode(settingsOfNewNode);
        ensureStableCluster(nodesCount + 1);
        // create repository without verification, when a node contains a wrong KEK
        createRepository(repoAfterNewNode, repositorySettings(repoAfterNewNode), false);

        // create an index with the shard on the node with the wrong KEK
        final String indexName = randomName();
        int docCounts = iterations(10, 100);
        final Settings indexSettings = Settings.builder()
                .put(indexSettings())
                .put("index.routing.allocation.include._name", newNode)
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .build();
        logger.info("-->  create random index {} with {} records", indexName, docCounts);
        createIndex(indexName, indexSettings);
        addRandomDocuments(indexName, docCounts);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docCounts);

        // empty snapshot completes successfully for both repos because it does not involve any data
        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoBeforeNewNode, snapshotName);
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoBeforeNewNode,
                snapshotName).setIndices(indexName + "other*").setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        logger.info("-->  create snapshot {}:{}", repoAfterNewNode, snapshotName);
        createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoAfterNewNode,
                snapshotName).setIndices(indexName + "other*").setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));

        // snapshot is PARTIAL because it includes shards on nodes with a different repository KEK
        final String snapshotName2 = snapshotName + "2";
        CreateSnapshotResponse incompleteSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoBeforeNewNode,
                snapshotName2).setWaitForCompletion(true).setIndices(indexName).get();
        assertThat(incompleteSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        assertTrue(incompleteSnapshotResponse.getSnapshotInfo().shardFailures().stream()
                .allMatch(shardFailure -> shardFailure.reason().contains("Repository key id mismatch")));
        incompleteSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoAfterNewNode,
                snapshotName2).setWaitForCompletion(true).setIndices(indexName).get();
        assertThat(incompleteSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        assertTrue(incompleteSnapshotResponse.getSnapshotInfo().shardFailures().stream()
                .allMatch(shardFailure -> shardFailure.reason().contains("Repository key id mismatch")));
    }

    public void testWrongKEKOnMasterNode() throws Exception {
        final int noNodes = internalCluster().size();
        final String repositoryName = randomRepositoryName();
        final Settings repositorySettings = Settings.builder().put(repositorySettings()).put(repositorySettings(repositoryName)).build();
        createRepository(repositoryName, repositorySettings, randomBoolean());
        final String snapshotName = randomName();
        logger.info("-->  create empty snapshot {}:{}", repositoryName, snapshotName);
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repositoryName,
                snapshotName).setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        // restart master nodes until the current master node has the wrong KEK
        Set<String> nodesWithWrongKEK = new HashSet<>();
        do {
            String masterNodeName = internalCluster().getMasterName();
            logger.info("-->  restart master node {}", masterNodeName);
            internalCluster().restartNode(masterNodeName, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    Settings.Builder newSettings = Settings.builder().put(super.onNodeStopped(nodeName));
                    byte[] repositoryKEKRestart = randomByteArrayOfLength(32);
                    repositoryKEKRestart[0] = 0; // be absolutely sure the KEK is different
                    MockSecureSettings secureSettings = new MockSecureSettings();
                    secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                            getConcreteSettingForNamespace(repositoryName).getKey(), repositoryKEKRestart);
                    newSettings.setSecureSettings(secureSettings);
                    return newSettings.build();
                }
            });
            nodesWithWrongKEK.add(masterNodeName);
            ensureStableCluster(noNodes);
        } while (false == nodesWithWrongKEK.contains(internalCluster().getMasterName()));
        // maybe recreate the repository
        if (randomBoolean()) {
            deleteRepository(repositoryName);
            createRepository(repositoryName, repositorySettings, false);
        }
        // all repository operations return "repository key is incorrect", but the repository does not move to the corrupted state
        final BlobStoreRepository blobStoreRepository =
                (BlobStoreRepository) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repositoryName);
        RepositoryException e = expectThrows(RepositoryException.class, () -> PlainActionFuture.<RepositoryData, Exception>get(
                f -> blobStoreRepository.threadPool().generic().execute(ActionRunnable.wrap(f, blobStoreRepository::getRepositoryData))));
        assertThat(e.getCause().getMessage(), containsString("repository key is incorrect"));
        e = expectThrows(RepositoryException.class, () ->
                client().admin().cluster().prepareCreateSnapshot(repositoryName, snapshotName + "2").setWaitForCompletion(true).get());
        assertThat(e.getCause().getMessage(), containsString("repository key is incorrect"));
        GetSnapshotsResponse getSnapshotResponse = client().admin().cluster().prepareGetSnapshots(repositoryName).get();
        assertThat(getSnapshotResponse.getSuccessfulResponses().keySet(), empty());
        assertThat(getSnapshotResponse.getFailedResponses().keySet(), contains(repositoryName));
        assertThat(getSnapshotResponse.getFailedResponses().get(repositoryName).getCause().getMessage(),
                containsString("repository key is incorrect"));
        e = expectThrows(RepositoryException.class, () ->
                client().admin().cluster().prepareRestoreSnapshot(repositoryName, snapshotName).setWaitForCompletion(true).get());
        assertThat(e.getCause().getMessage(), containsString("repository key is incorrect"));
        e = expectThrows(RepositoryException.class, () ->
                client().admin().cluster().prepareDeleteSnapshot(repositoryName, snapshotName).get());
        assertThat(e.getCause().getMessage(), containsString("repository key is incorrect"));
        // restart nodes back, so that the master node has the correct repository key again
        do {
            String masterNodeName = internalCluster().getMasterName();
            logger.info("-->  restart master node {}", masterNodeName);
            internalCluster().restartNode(masterNodeName, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    Settings.Builder newSettings = Settings.builder().put(super.onNodeStopped(nodeName));
                    byte[] repositoryNameBytes = repositoryName.getBytes(StandardCharsets.UTF_8);
                    byte[] repositoryKEK = new byte[32];
                    System.arraycopy(repositoryNameBytes, 0, repositoryKEK, 0, repositoryNameBytes.length);
                    MockSecureSettings secureSettings = new MockSecureSettings();
                    secureSettings.setFile(EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.
                            getConcreteSettingForNamespace(repositoryName).getKey(), repositoryKEK);
                    newSettings.setSecureSettings(secureSettings);
                    return newSettings.build();
                }
            });
            nodesWithWrongKEK.remove(masterNodeName);
            ensureStableCluster(noNodes);
        } while (nodesWithWrongKEK.contains(internalCluster().getMasterName()));
        // ensure get snapshot works
        getSnapshotResponse = client().admin().cluster().prepareGetSnapshots(repositoryName).get();
        assertThat(getSnapshotResponse.getFailedResponses().keySet(), empty());
        assertThat(getSnapshotResponse.getSuccessfulResponses().keySet(), contains(repositoryName));
    }
}
