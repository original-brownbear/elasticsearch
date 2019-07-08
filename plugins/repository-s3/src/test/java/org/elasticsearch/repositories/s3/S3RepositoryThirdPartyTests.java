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
package org.elasticsearch.repositories.s3;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class S3RepositoryThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(S3RepositoryPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.s3.account"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.key"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", System.getProperty("test.s3.account"));
        secureSettings.setString("s3.client.default.secret_key", System.getProperty("test.s3.key"));
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        Settings.Builder settings = Settings.builder()
            .put("bucket", System.getProperty("test.s3.bucket"))
            .put("base_path", System.getProperty("test.s3.base", "testpath"));
        final String endpoint = System.getProperty("test.s3.endpoint");
        if (endpoint != null) {
            settings = settings.put("endpoint", endpoint);
        }
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
            .setType("s3")
            .setSettings(settings).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    public void testCreateTwentySnapshots() {

        final AtomicInteger putCount = new AtomicInteger(0);

        createIndex("test-idx-1");
        createIndex("test-idx-2");
        final List<String> snapshotNames = new ArrayList<>();
        ensureGreen();
        for (int j = 0; j < 20; ++j) {
            logger.info("--> indexing some data");
            for (int i = 0; i < 100; i++) {
                client().prepareIndex("test-idx-1", "doc", Integer.toString(i)).setSource("foo", "bar" + j * i).get();
                client().prepareIndex("test-idx-2", "doc", Integer.toString(i)).setSource("foo", "bar" + j * i).get();
            }
            client().admin().indices().prepareRefresh().get();

            final String snapshotName = "test-snap-" + System.currentTimeMillis();
            snapshotNames.add(snapshotName);
            logger.info("--> snapshot");
            CreateSnapshotResponse createSnapshotResponse = client().admin()
                .cluster()
                .prepareCreateSnapshot("test-repo", snapshotName)
                .setWaitForCompletion(true)
                .setIndices("test-idx-1", "test-idx-2")
                .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        }

        final List<SnapshotStatus> snapshotStatusList = client().admin().cluster().prepareSnapshotStatus()
            .addSnapshots(snapshotNames.toArray(Strings.EMPTY_ARRAY)).setRepository("test-repo").get().getSnapshots();
        for (final SnapshotStatus snapshotStatus : snapshotStatusList) {
            putCount.addAndGet(snapshotStatus.getStats().getIncrementalFileCount());
        }

        final int expectedPuts = putCount.get()
            + 20 // index-N blobs for 20 snapshots
            + 20 * 2 // meta- blobs for 20 snapshots with 2 indices each
            + 20 * 2 // snap- blobs for 20 snapshots with 2 shards each
            + 20 * 2 // index-N blobs for 20 snapshots with 2 shards each
            + 20  // root level snap-for 20 snapshots
            + 20 // root level meta- for 20 snapshots
            + 1; // Put incompatible snapshots

        // PUT: 384 seen vs. 381 expected (difference explained by 3 puts of verify blobs)
        // PUT /testb/testpath/tests-KS397dCRSZO2r57VS4MZXQ/master.dat
        // PUT /testb/testpath/tests--5zClnTlRrq21p2Ccsrtew/master.dat
        // PUT /testb/testpath/tests--5zClnTlRrq21p2Ccsrtew/data-HQ7b1blGQg6ttIDsqTkUQw.dat

        final int getCount =
        +    20 * 2   // list index-N (twice per snapshot on start and finalize)
        +    20 * 3   // initialize snapshot loads latest index-N separately
        +    20 * 2   // get latest index-N (twice per snapshot)
        +    20 * 2   // get incompatible snapshots (same as index-N)
        +    20 * 2   // get shard index-N
        +    20 * 2;  // list each shard's blobs

        // GET: 386 seen vs. 260  // Difference is calculated as follows (as a result of the final get shards status request):
        // 20        from loading each snapshot-info blob
        // 20 * 2    from getting snap- in each of the 2 shards for all snapshots
        // 20 * 2    from getting index metadata for each snapshot/index pair
        // 20        from index-N generation check
        // 1 + 1 + 1 from loading repositoryData

        // Also 3 GET from the verify action:
        // GET /testb/?prefix=testpath%2Ftests--5zClnTlRrq21p2Ccsrtew%2F&delimiter=%2F&encoding-type=url
        // GET /testb/?prefix=testpath%2F&encoding-type=url
        // GET /testb/?prefix=&delimiter=%2F&encoding-type=url

        logger.info("Put Count: {} data files, expected overall {}", putCount.get(), expectedPuts);
        // All Requests for index-N
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1 TODO: This is stupid, these GETs are totally redundant and shouldn't happen
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-0
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-0
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-0
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-0
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-1
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-0
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-1
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-1
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-1
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-2
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-1
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-2
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-2
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-2
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-2
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-2
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-3
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-2
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-3
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-2
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-3
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-3
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-3
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-3
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-4
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-3
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-4
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-3
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-4
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-4
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-4
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-4
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-5
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-4
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-5
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-4
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-5
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-5
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-5
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-5
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-6
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-5
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-6
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-5
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-6
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-6
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-6
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-6
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-7
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-6
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-7
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-6
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-7
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-7
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-7
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-7
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-8
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-7
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-8
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-7
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-8
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-8
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-8
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-8
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-9
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-8
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-9
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-8
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-9
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-9
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-9
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-9
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-10
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-9
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-10
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-9
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-10
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-10
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-10
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-10
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-11
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-10
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-11
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-10
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-11
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-11
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-11
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-11
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-12
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-11
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-12
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-11
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-12
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-12
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-12
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-12
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-13
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-12
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-13
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-12
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-13
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-13
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-13
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-13
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-14
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-13
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-14
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-13
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-14
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-14
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-14
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-14
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-15
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-14
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-15
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-14
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-15
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-15
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-15
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-15
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-16
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-15
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-16
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-15
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-16
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-16
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-16
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-16
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-17
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-16
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-17
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-16
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-17
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-17
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-17
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-17
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-18
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-17
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-18
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-17
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-18
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-18
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-18
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-18
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-19
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-18
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-19
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-18
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-19
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-19
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1
        //PUT /testb/testpath/indices/qIOl8euRRp655gVFane0Yg/0/index-0
        //PUT /testb/testpath/indices/rJoeOn2pSsetwIHEtmOZgQ/0/index-0
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-0
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/testpath/indices/qIOl8euRRp655gVFane0Yg/0/index-0
        //PUT /testb/testpath/indices/qIOl8euRRp655gVFane0Yg/0/index-1
        //GET /testb/testpath/indices/rJoeOn2pSsetwIHEtmOZgQ/0/index-0
        //PUT /testb/testpath/indices/rJoeOn2pSsetwIHEtmOZgQ/0/index-1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-1
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-1
    }

    @Override
    protected boolean assertCorruptionVisible(BlobStoreRepository repo, Executor genericExec) throws Exception {
        // S3 is only eventually consistent for the list operations used by this assertions so we retry for 10 minutes assuming that
        // listing operations will become consistent within these 10 minutes.
        assertBusy(() -> assertTrue(super.assertCorruptionVisible(repo, genericExec)), 10L, TimeUnit.MINUTES);
        return true;
    }

    @Override
    protected void assertConsistentRepository(BlobStoreRepository repo, Executor executor) throws Exception {
        // S3 is only eventually consistent for the list operations used by this assertions so we retry for 10 minutes assuming that
        // listing operations will become consistent within these 10 minutes.
        assertBusy(() -> super.assertConsistentRepository(repo, executor), 10L, TimeUnit.MINUTES);
    }

    protected void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetaData> blobs) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertBlobsByPrefix(path, prefix, blobs), 10L, TimeUnit.MINUTES);
    }

    @Override
    protected void assertChildren(BlobPath path, Collection<String> children) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertChildren(path, children), 10L, TimeUnit.MINUTES);
    }

    @Override
    protected void assertDeleted(BlobPath path, String name) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertDeleted(path, name), 10L, TimeUnit.MINUTES);
    }
}
