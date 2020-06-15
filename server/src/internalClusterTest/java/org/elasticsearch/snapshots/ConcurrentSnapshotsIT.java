/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsIT extends AbstractSnapshotIntegTestCase {

    public void testLongRunningSnapshotAllowsConcurrentSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        blockDataNode(repoName, dataNode);

        final String indexSlow = "index-slow";
        createIndex(indexSlow, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(indexSlow);
        indexDoc(indexSlow, "some_id", "foo", "bar");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                client().admin().cluster().prepareCreateSnapshot(repoName, "slow-snapshot").setWaitForCompletion(true).execute();

        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String indexFast = "index-fast";
        createIndex(indexFast, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", dataNode2)
                .put("index.routing.allocation.exclude._name", dataNode).build());
        ensureGreen(indexFast);
        indexDoc(indexFast, "some_id", "foo", "bar");

        assertSuccessful(client().admin().cluster().prepareCreateSnapshot(repoName, "fast-snapshot")
                .setIndices(indexFast).setWaitForCompletion(true).execute());

        assertThat(createSlowFuture.isDone(), is(false));
        unblockNode(repoName, dataNode);

        assertSuccessful(createSlowFuture);
    }

    public void testDeletesAreBatched() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());

        createIndex("foo");
        ensureGreen();

        final int numSnapshots = randomIntBetween(1, 4);
        final PlainActionFuture<Collection<CreateSnapshotResponse>> allSnapshotsDone = PlainActionFuture.newFuture();
        final ActionListener<CreateSnapshotResponse> snapshotsListener = new GroupedActionListener<>(allSnapshotsDone, numSnapshots);
        final Collection<String> snapshotNames = new HashSet<>();
        for (int i = 0; i < numSnapshots; i++) {
            final String snapshot = "snap-" + i;
            snapshotNames.add(snapshot);
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshot).setWaitForCompletion(true)
                    .execute(snapshotsListener);
        }
        final Collection<CreateSnapshotResponse> snapshotResponses = allSnapshotsDone.get();
        for (CreateSnapshotResponse snapshotResponse : snapshotResponses) {
            assertThat(snapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        }

        blockDataNode(repoName, dataNode);

        final String indexSlow = "index-slow";
        createIndex(indexSlow, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(indexSlow);
        indexDoc(indexSlow, "some_id", "foo", "bar");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                client().admin().cluster().prepareCreateSnapshot(repoName, "blocked-snapshot").setWaitForCompletion(true).execute();
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final Collection<StepListener<AcknowledgedResponse>> deleteFutures = new ArrayList<>();
        while (snapshotNames.isEmpty() == false) {
            final Collection<String> toDelete = randomSubsetOf(snapshotNames);
            if (toDelete.isEmpty()) {
                continue;
            }
            snapshotNames.removeAll(toDelete);
            final StepListener<AcknowledgedResponse> future = new StepListener<>();
            client().admin().cluster().prepareDeleteSnapshot(repoName, toDelete.toArray(Strings.EMPTY_ARRAY)).execute(future);
            deleteFutures.add(future);
        }

        assertThat(createSlowFuture.isDone(), is(false));

        final long repoGenAfterInitialSnapshots = getRepositoryData(repoName).getGenId();
        assertThat(repoGenAfterInitialSnapshots, is(numSnapshots - 1L));
        unblockNode(repoName, dataNode);

        final SnapshotInfo slowSnapshotInfo = assertSuccessful(createSlowFuture);

        logger.info("--> waiting for batched deletes to finish");
        final PlainActionFuture<Collection<AcknowledgedResponse>> allDeletesDone = new PlainActionFuture<>();
        final ActionListener<AcknowledgedResponse> deletesListener = new GroupedActionListener<>(allDeletesDone, deleteFutures.size());
        for (StepListener<AcknowledgedResponse> deleteFuture : deleteFutures) {
            deleteFuture.whenComplete(deletesListener::onResponse, deletesListener::onFailure);
        }
        allDeletesDone.get();

        logger.info("--> verifying repository state");
        final RepositoryData repositoryDataAfterDeletes = getRepositoryData(repoName);
        // One increment for snapshot, one for all the deletes
        assertThat(repositoryDataAfterDeletes.getGenId(), is(repoGenAfterInitialSnapshots + 2));
        assertThat(repositoryDataAfterDeletes.getSnapshotIds(), contains(slowSnapshotInfo.snapshotId()));
    }

    public void testBlockedRepoDoesNotBlockOtherRepos() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock", randomRepoPath());
        createRepository(otherRepoName, "fs", randomRepoPath());

        createIndex("foo");
        ensureGreen();

        blockMasterFromFinalizingSnapshotOnIndexFile(blockedRepoName);

        final String indexSlow = "index-slow";
        createIndex(indexSlow, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(indexSlow);
        indexDoc(indexSlow, "some_id", "foo", "bar");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                client().admin().cluster().prepareCreateSnapshot(blockedRepoName, "blocked-snapshot").setWaitForCompletion(true).execute();
        waitForBlock(masterNode, blockedRepoName, TimeValue.timeValueSeconds(30L));

        client().admin().cluster().prepareCreateSnapshot(otherRepoName, "snapshot")
                .setIndices("does-not-exist-*")
                .setWaitForCompletion(false).get();

        unblockNode(blockedRepoName, masterNode);
        expectThrows(SnapshotException.class, createSlowFuture::actionGet);

        assertBusy(() -> {
            final List<SnapshotInfo> currentSnapshots = client().admin().cluster().prepareGetSnapshots(otherRepoName)
                    .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT).get().getSnapshots(otherRepoName);
            assertThat(currentSnapshots, empty());
        }, 30L, TimeUnit.SECONDS);
    }

    public void testMultipleReposAreIndependent() throws Exception {
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = internalCluster().startDataOnlyNode(Settings.builder()
                .put("thread_pool.snapshot.core", 5).put("thread_pool.snapshot.max", 5).build());
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock", randomRepoPath());
        createRepository(otherRepoName, "fs", randomRepoPath());

        blockDataNode(blockedRepoName, dataNode);

        final String testIndex = "test-index";
        createIndex(testIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(testIndex);
        indexDoc(testIndex, "some_id", "foo", "bar");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                client().admin().cluster().prepareCreateSnapshot(blockedRepoName, "blocked-snapshot").setWaitForCompletion(true).execute();
        waitForBlock(dataNode, blockedRepoName, TimeValue.timeValueSeconds(30L));

        logger.info("--> waiting for concurrent snapshot to finish");
        assertSuccessful(client().admin().cluster().prepareCreateSnapshot(otherRepoName, "snapshot").setWaitForCompletion(true).execute());

        logger.info("--> unblocking data node");
        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    public void testSnapshotRunsAfterInProgressDelete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());

        createIndex("foo");
        ensureGreen();

        final String indexSlow = "index-slow";
        createIndex(indexSlow, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(indexSlow);
        indexDoc(indexSlow, "some_id", "foo", "bar");

        final String firstSnapshot = "first-snapshot";
        assertSuccessful(client().admin().cluster().prepareCreateSnapshot(repoName, firstSnapshot).setWaitForCompletion(true).execute());

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);

        final ActionFuture<AcknowledgedResponse> deleteFuture =
                client().admin().cluster().prepareDeleteSnapshot(repoName, firstSnapshot).execute();
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        final String secondSnapshot = "second-snapshot";
        final ActionFuture<CreateSnapshotResponse> snapshotFuture =
                client().admin().cluster().prepareCreateSnapshot(repoName, secondSnapshot).setWaitForCompletion(true).execute();

        logger.info("--> unblocking master node");
        unblockNode(repoName, masterNode);
        final UncategorizedExecutionException ex = expectThrows(UncategorizedExecutionException.class, deleteFuture::actionGet);
        assertThat(ex.getRootCause(), instanceOf(IOException.class));

        assertSuccessful(snapshotFuture);
    }

    public void testAbortOneOfMultipleSnapshots() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        blockDataNode(repoName, dataNode);

        final String firstIndex = "index-one";
        createIndex(firstIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(firstIndex);
        indexDoc(firstIndex, "some_id", "foo", "bar");

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse =
                client().admin().cluster().prepareCreateSnapshot(repoName, firstSnapshot).setWaitForCompletion(true).execute();

        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String secondIndex = "index-two";
        createIndex(secondIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", dataNode2)
                .put("index.routing.allocation.exclude._name", dataNode).build());
        ensureGreen(secondIndex);
        indexDoc(secondIndex, "some_id", "foo", "bar");

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = client().admin().cluster()
                .prepareCreateSnapshot(repoName, secondSnapshot).setWaitForCompletion(true).execute();

        final ClusterService clusterService = internalCluster().getMasterNodeInstance(ClusterService.class);

        logger.info("--> wait for snapshot on second data node to finish");
        final CountDownLatch shardFinishedLatch = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
                if (snapshotsInProgress != null && snapshotsInProgress.entries().size() == 2) {
                    for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                        if (entry.snapshot().getSnapshotId().getName().equals(secondSnapshot)) {
                            for (ObjectCursor<SnapshotsInProgress.ShardSnapshotStatus> shard : entry.shards().values()) {
                                if (shard.value.state().completed()) {
                                    shardFinishedLatch.countDown();
                                    clusterService.removeListener(this);
                                }
                            }
                        }
                    }
                }
            }
        });
        shardFinishedLatch.await();

        final CountDownLatch deleteEnqueuedLatch = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final SnapshotDeletionsInProgress deletionsInProgress = event.state().custom(SnapshotDeletionsInProgress.TYPE);
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    deleteEnqueuedLatch.countDown();
                    clusterService.removeListener(this);
                }
            }
        });
        logger.info("--> starting delete for first snapshot");
        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse =
                client().admin().cluster().prepareDeleteSnapshot(repoName, firstSnapshot).execute();

        logger.info("--> wait for delete to be enqueued in cluster state");
        deleteEnqueuedLatch.await();

        logger.info("--> start third snapshot");
        final String thirdSnapshot = "snapshot-three";
        final ActionFuture<CreateSnapshotResponse> thirdSnapshotResponse = client().admin().cluster()
                .prepareCreateSnapshot(repoName, thirdSnapshot).setIndices(secondIndex).setWaitForCompletion(true).execute();

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> unblocking data node");
        unblockNode(repoName, dataNode);
        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));

        final SnapshotInfo secondSnapshotInfo = assertSuccessful(secondSnapshotResponse);
        final SnapshotInfo thirdSnapshotInfo = assertSuccessful(thirdSnapshotResponse);

        assertThat(deleteSnapshotsResponse.get().isAcknowledged(), is(true));

        logger.info("--> verify that the first snapshot is gone");
        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(repoName),
                containsInAnyOrder(secondSnapshotInfo, thirdSnapshotInfo));
    }

    public void testCascadedAborts() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        blockDataNode(repoName, dataNode);

        final String firstIndex = "index-one";
        createIndex(firstIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(firstIndex);
        indexDoc(firstIndex, "some_id", "foo", "bar");

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse =
                client().admin().cluster().prepareCreateSnapshot(repoName, firstSnapshot).setWaitForCompletion(true).execute();

        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String secondIndex = "index-two";
        createIndex(secondIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", dataNode2)
                .put("index.routing.allocation.exclude._name", dataNode).build());
        ensureGreen(secondIndex);
        indexDoc(secondIndex, "some_id", "foo", "bar");

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = client().admin().cluster()
                .prepareCreateSnapshot(repoName, secondSnapshot).setWaitForCompletion(true).execute();

        final ClusterService clusterService = internalCluster().getMasterNodeInstance(ClusterService.class);

        logger.info("--> wait for snapshot on second data node to finish");
        final CountDownLatch shardFinishedLatch = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
                if (snapshotsInProgress != null && snapshotsInProgress.entries().size() == 2) {
                    for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                        if (entry.snapshot().getSnapshotId().getName().equals(secondSnapshot)) {
                            for (ObjectCursor<SnapshotsInProgress.ShardSnapshotStatus> shard : entry.shards().values()) {
                                if (shard.value.state().completed()) {
                                    shardFinishedLatch.countDown();
                                    clusterService.removeListener(this);
                                }
                            }
                        }
                    }
                }
            }
        });
        shardFinishedLatch.await();

        final CountDownLatch deleteEnqueuedLatch = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final SnapshotDeletionsInProgress deletionsInProgress = event.state().custom(SnapshotDeletionsInProgress.TYPE);
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    deleteEnqueuedLatch.countDown();
                    clusterService.removeListener(this);
                }
            }
        });
        logger.info("--> starting delete for first snapshot");
        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse =
                client().admin().cluster().prepareDeleteSnapshot(repoName, firstSnapshot).execute();

        logger.info("--> wait for delete to be enqueued in cluster state");
        deleteEnqueuedLatch.await();

        logger.info("--> start third snapshot");
        final String thirdSnapshot = "snapshot-three";
        final ActionFuture<CreateSnapshotResponse> thirdSnapshotResponse = client().admin().cluster()
                .prepareCreateSnapshot(repoName, thirdSnapshot).setWaitForCompletion(true).execute();

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> {
            final List<SnapshotInfo> currentSnapshots = client().admin().cluster().prepareGetSnapshots(repoName)
                    .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT).get().getSnapshots(repoName);
            assertThat(currentSnapshots, hasSize(3));
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> starting delete for all snapshots");
        final ActionFuture<AcknowledgedResponse> allDeletedResponse =
                client().admin().cluster().prepareDeleteSnapshot(repoName, "*").execute();

        logger.info("--> waiting for second and third snapshot to finish");
        assertBusy(() -> {
            final List<SnapshotInfo> currentSnapshots = client().admin().cluster().prepareGetSnapshots(repoName)
                    .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT).get().getSnapshots(repoName);
            assertThat(currentSnapshots, hasSize(1));
            final SnapshotsInProgress snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries().get(0).state(), is(SnapshotsInProgress.State.ABORTED));
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> unblocking data node");
        unblockNode(repoName, dataNode);

        logger.info("--> verify all snapshots were aborted");
        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
        assertThat(secondSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
        assertThat(thirdSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));

        logger.info("--> verify both deletes have completed");
        assertThat(deleteSnapshotsResponse.get().isAcknowledged(), is(true));
        assertThat(allDeletedResponse.get().isAcknowledged(), is(true));

        logger.info("--> verify that all snapshots are gone");
        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(repoName), empty());
    }

    public void testMasterFailOverWithQueuedDeletes() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        blockDataNode(repoName, dataNode);

        final String firstIndex = "index-one";
        createIndex(firstIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(firstIndex);
        indexDoc(firstIndex, "some_id", "foo", "bar");

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse =
                client().admin().cluster().prepareCreateSnapshot(repoName, firstSnapshot).setWaitForCompletion(true).execute();

        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(5);
        final String secondIndex = "index-two";
        createIndex(secondIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", dataNode2)
                .put("index.routing.allocation.exclude._name", dataNode).build());
        ensureGreen(secondIndex);
        indexDoc(secondIndex, "some_id", "foo", "bar");

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = client().admin().cluster()
                .prepareCreateSnapshot(repoName, secondSnapshot).setWaitForCompletion(true).execute();

        final ClusterService clusterService = internalCluster().getMasterNodeInstance(ClusterService.class);

        logger.info("--> wait for snapshot on second data node to finish");
        final CountDownLatch shardFinishedLatch = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
                if (snapshotsInProgress != null && snapshotsInProgress.entries().size() == 2) {
                    for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                        if (entry.snapshot().getSnapshotId().getName().equals(secondSnapshot)) {
                            for (ObjectCursor<SnapshotsInProgress.ShardSnapshotStatus> shard : entry.shards().values()) {
                                if (shard.value.state().completed()) {
                                    shardFinishedLatch.countDown();
                                    clusterService.removeListener(this);
                                }
                            }
                        }
                    }
                }
            }
        });
        shardFinishedLatch.await();

        final CountDownLatch deleteEnqueuedLatch = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final SnapshotDeletionsInProgress deletionsInProgress = event.state().custom(SnapshotDeletionsInProgress.TYPE);
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    deleteEnqueuedLatch.countDown();
                    clusterService.removeListener(this);
                }
            }
        });
        logger.info("--> starting delete for first snapshot");
        client().admin().cluster().prepareDeleteSnapshot(repoName, firstSnapshot).execute();

        logger.info("--> wait for delete to be enqueued in cluster state");
        deleteEnqueuedLatch.await();

        logger.info("--> blocking data node 2");
        blockDataNode(repoName, dataNode2);

        logger.info("--> start third snapshot");
        final String thirdSnapshot = "snapshot-three";
        client().admin().cluster().prepareCreateSnapshot(repoName, thirdSnapshot).setWaitForCompletion(true).execute();

        waitForBlock(dataNode2, repoName, TimeValue.timeValueSeconds(30L));
        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> {
            final List<SnapshotInfo> currentSnapshots = client().admin().cluster().prepareGetSnapshots(repoName)
                    .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT).get().getSnapshots(repoName);
            assertThat(currentSnapshots, hasSize(3));
        }, 30L, TimeUnit.SECONDS);

        final CountDownLatch secondDeleteEnqueuedLatch = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final SnapshotDeletionsInProgress deletionsInProgress = event.state().custom(SnapshotDeletionsInProgress.TYPE);
                if (deletionsInProgress.getEntries().get(0).getSnapshots().size() == 3) {
                    secondDeleteEnqueuedLatch.countDown();
                    clusterService.removeListener(this);
                }
            }
        });
        logger.info("--> starting delete for all snapshots");
        client().admin().cluster().prepareDeleteSnapshot(repoName, "*").execute();

        logger.info("--> wait for delete to be enqueued in cluster state");
        secondDeleteEnqueuedLatch.await();

        logger.info("--> waiting for second snapshot to finish");
        assertBusy(() -> {
            final List<SnapshotInfo> currentSnapshots = client().admin().cluster().prepareGetSnapshots(repoName)
                    .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT).get().getSnapshots(repoName);
            assertThat(currentSnapshots, hasSize(2));
            final SnapshotsInProgress snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries().get(0).state(), is(SnapshotsInProgress.State.ABORTED));
            assertThat(snapshotsInProgress.entries().get(1).state(), is(SnapshotsInProgress.State.ABORTED));
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> stopping current master node");
        internalCluster().stopCurrentMasterNode();

        logger.info("--> unblocking data nodes");
        unblockNode(repoName, dataNode);
        unblockNode(repoName, dataNode2);

        logger.info("--> verify that all snapshots are gone and no more work is left in the cluster state");
        assertBusy(() -> {
            assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(repoName), empty());
            final ClusterState state = clusterService().state();
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries(), empty());
            final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            assertThat(snapshotDeletionsInProgress.getEntries(), empty());
        }, 30L, TimeUnit.SECONDS);
    }

    private static SnapshotInfo assertSuccessful(ActionFuture<CreateSnapshotResponse> future) throws Exception {
        final SnapshotInfo snapshotInfo = future.get().getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        return snapshotInfo;
    }
}
