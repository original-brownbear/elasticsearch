/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsBlobStoreRepositoryBaseIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.repositories.encrypted.EncryptedRepository.getEncryptedBlobByteLength;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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

    public void testSnapshotIsPartialForDifferentKEK() throws Exception {
        String repoBeforeNewNode = randomRepositoryName();
        // create repository before adding the new node
        createRepository(repoBeforeNewNode, repositorySettings(repoBeforeNewNode), true);
        String repoAfterNewNode = randomRepositoryName();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                getConcreteSettingForNamespace(repoBeforeNewNode).getKey(), repoBeforeNewNode + "wrong");
        secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                getConcreteSettingForNamespace(repoAfterNewNode).getKey(), repoAfterNewNode + "wrong");
        Settings settingsOfNewNode = Settings.builder().put("node.attr.repoKEK", "wrong").setSecureSettings(secureSettings).build();
        // start a new node with different repository KEKs
        int nodesCount = cluster().size();
        String newNode = internalCluster().startNode(settingsOfNewNode);
        ensureStableCluster(nodesCount + 1);
        // create repository without verification, when a node does not contain the repository KEK
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
                .allMatch(shardFailure -> shardFailure.reason().contains("Repository secret id mismatch")));
        incompleteSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoAfterNewNode,
                snapshotName2).setWaitForCompletion(true).setIndices(indexName).get();
        assertThat(incompleteSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        assertTrue(incompleteSnapshotResponse.getSnapshotInfo().shardFailures().stream()
                .allMatch(shardFailure -> shardFailure.reason().contains("Repository secret id mismatch")));
    }

    public void testWrongKEKOnMasterNode() throws Exception {
        final int noNodes = internalCluster().size();
        final String repositoryName = randomRepositoryName();
        final Settings repositorySettings = repositorySettings(repositoryName);
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
                    MockSecureSettings secureSettings = new MockSecureSettings();
                    secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                            getConcreteSettingForNamespace(repositoryName).getKey(), repositoryName + "wrong");
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
                    MockSecureSettings secureSettings = new MockSecureSettings();
                    secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                            getConcreteSettingForNamespace(repositoryName).getKey(), repositoryName);
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
