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
        // All Requests
        // PUT /testb/testpath/tests-KS397dCRSZO2r57VS4MZXQ/master.dat
        //PUT /testb/testpath/tests--5zClnTlRrq21p2Ccsrtew/master.dat
        //PUT /testb/testpath/tests--5zClnTlRrq21p2Ccsrtew/data-HQ7b1blGQg6ttIDsqTkUQw.dat
        //GET /testb/?prefix=testpath%2Ftests--5zClnTlRrq21p2Ccsrtew%2F&delimiter=%2F&encoding-type=url
        //GET /testb/?prefix=testpath%2F&encoding-type=url
        //GET /testb/?prefix=&delimiter=%2F&encoding-type=url
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1
        //PUT /testb/testpath/meta-Mmfni2BhQlOBqKvfWaUp7w.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-Mmfni2BhQlOBqKvfWaUp7w.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-Mmfni2BhQlOBqKvfWaUp7w.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__yky87-cLQKioBfFyEy58qg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__YgRVkYl9Qmiq84wiKQRg7w
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__oAJXxeRISlinU_3ZiW8A0w
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__-27HukyuQ2e01FgjOviFag
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-Mmfni2BhQlOBqKvfWaUp7w.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-0
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__Kd0nv2_FTiaj02x5PbPeVw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__8i-dcAt9R52r6rqmbnPYjg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__VScQ4_36SxW6bsoXcMNqrw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__MERIWmDWRiGa7k3w7ZjaZA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-Mmfni2BhQlOBqKvfWaUp7w.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-0
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index--1
        //PUT /testb/testpath/snap-Mmfni2BhQlOBqKvfWaUp7w.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-0
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-P4dPqlvrR-O-obrf6zbDbQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-P4dPqlvrR-O-obrf6zbDbQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-P4dPqlvrR-O-obrf6zbDbQ.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-0
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/___JgB-KtHRo2NTZPXm0lbcQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/___PgdhrUsSqq1HZBLaBNNew
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/___vNzHILsSyq_bOACvWQHoQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__wwPNCwq3T327yuHdK6BDnw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-P4dPqlvrR-O-obrf6zbDbQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-1
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-0
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__7Y17yMTERL2Ys4XT1kLMAw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__431kucd4Q6CmtJWgVQ3qKg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__Dam9SI50TJyEPod6XLVPkA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__mGtlb8fMTsCZpAMXLTDYVw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-P4dPqlvrR-O-obrf6zbDbQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-1
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-0
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-P4dPqlvrR-O-obrf6zbDbQ.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-1
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-1
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-1
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-1
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__yU2CO7UJQqK7MgAFKwPs_A
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__b1jFrvHDTSCFqBPdal0Xrw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__Djr8glF-SEWFIwE0UK1Vcw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__SO0LLa4oR_OgexY9-zD64w
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__HDcUaT_QSFuszOXlXuo2Iw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__1DxrvuiNT_y2kfrzmErXYQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__eBVkZlvxRsWS6i_6slZ_Ew
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-2
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-1
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__Rw7ioGy4RamFv4GebYBp5w
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__T54ufkEsSA-yF_2AIBGTbA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__fUmlZU_OSyO3-i1kwMtO7w
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__DILXw-v_RpG6hdTaBGDRZw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-2
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-1
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-2
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-2
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-2
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-ptamsa36Ro-HmpJH-cXErw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-ptamsa36Ro-HmpJH-cXErw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-ptamsa36Ro-HmpJH-cXErw.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-2
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__BVhmeQC2RL-e--yjL0_Imw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__Om18T4wjR0mk5mBh7NOpSw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__5hu--mrmTbOr8jczZ5aZhQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__2_1vbCRxSbSr-YyGmm4iGQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-ptamsa36Ro-HmpJH-cXErw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-3
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-2
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__gR3t7WgFTgqIOBScGnWdIg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__xvuGodYTSAqrzSWOH6Z7QQ
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__w62rS3QoQT6lmttFY97h6g
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__SsMksfn3RR2EH-f5LYxTAw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-ptamsa36Ro-HmpJH-cXErw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-3
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-2
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-ptamsa36Ro-HmpJH-cXErw.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-3
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-3
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-3
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-3
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__fH-Ux72BRuGBEfLxKYYVlg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__eN2fTTiMQseg6TWyjeJ-ew
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__txEX5N_KT-eVnr_pAhW3Sg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__MMtwl5h2TkqDLXtDTxpSyw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-4
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-3
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__cA43Fo5ARLKzPy_deONCSg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__3jBlyFAJRui6UQ4uCxlvzg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__LKTIn3bnSbWl4dm6wQbCVA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__Z64hSmfgSfiXqFBCPxhABA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-4
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-3
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-4
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-4
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-4
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-HW2DY9irSMmYfBB9rf4tUg.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-HW2DY9irSMmYfBB9rf4tUg.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-HW2DY9irSMmYfBB9rf4tUg.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-4
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__13voVg1MTduxQrQU6_uvUA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__9OKOJRN5RPuh_fchoGLUfQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__cMETkrT2S92WM4gUnhVCWQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__qEE-GQBuQBu73_NSqfkGzw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-HW2DY9irSMmYfBB9rf4tUg.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-5
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-4
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__7s2tgN0PQ_iVLKrywLwlcA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__PQmc2YydQSy2PLZ3ikkLXw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__rUYTzDX5SqmG_FHnoJsi4A
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__bYRWe-SqRR-baUMOZwH3qA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-HW2DY9irSMmYfBB9rf4tUg.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-5
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-4
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-HW2DY9irSMmYfBB9rf4tUg.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-5
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-5
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-5
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-vK671Hr_ScuOjM40JKL1BQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-vK671Hr_ScuOjM40JKL1BQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-vK671Hr_ScuOjM40JKL1BQ.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-5
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__9OF09M1kRCiF0aNQbv8-1Q
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__D3ZG9W3YSX2Ij4JBAtPqDA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__ikzjyxgmSUiKkuWxYkf3Tw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__FZkoeXrWQxiHghh7mjAuAA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-vK671Hr_ScuOjM40JKL1BQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-6
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-5
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__5nx11P-nQQyWhsk4xe7gig
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__v5MpVGh0RNmJN4WOrNtT2Q
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__0YuMxmPOT6-j1FYsmPv38Q
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__JG9gp7W1Qv2v-5abWLyNUA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-vK671Hr_ScuOjM40JKL1BQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-6
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-5
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-vK671Hr_ScuOjM40JKL1BQ.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-6
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-6
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-6
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-EB1m0HeLRHyVRVz_uUqz1g.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-EB1m0HeLRHyVRVz_uUqz1g.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-EB1m0HeLRHyVRVz_uUqz1g.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-6
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__QHccTNVKTN-RKpAqteVotA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__qEt31pS4SyKvfLgRNGBnjw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__hy9WzyMISF-2aFQuRxwDJw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__azKhbb0PSRqLwpzTMtHzeA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-EB1m0HeLRHyVRVz_uUqz1g.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-7
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-6
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__sRKsS4DwSOqjHlqxRKMdLg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/___zom3dQ5S1iDGjNR1mIfOg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__Are2E8RJQom31pPEz8Hdeg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__j4m9Ply-QECWNx_x-rmvcA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-EB1m0HeLRHyVRVz_uUqz1g.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-7
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-6
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-EB1m0HeLRHyVRVz_uUqz1g.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-7
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-7
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-7
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-f_r04g26T6mTmudby3nNvw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-f_r04g26T6mTmudby3nNvw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-f_r04g26T6mTmudby3nNvw.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-7
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__bzt30BAXRb-8UjL1rG-MWQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__lcF1IzQ4TbGKcMvEVaQ-xA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__VTLvlV3lTZS8-kUbnYu3UA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__gTvaNhajTCqibGKD2stLKg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-f_r04g26T6mTmudby3nNvw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-8
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-7
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__8-Kst5k_SEuzCKmQQaOODQ
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__bWRv75OcRnuXtlS6JQ41tA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__VtzuVLovS8yVML0Z8w1UYA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__hsJjkBacTT-0hkYStvf5dw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-f_r04g26T6mTmudby3nNvw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-8
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-7
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-f_r04g26T6mTmudby3nNvw.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-8
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-8
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-8
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-8
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__B7iylEBnRqam1EYDvjyv1g
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__dtK1f-rpTsyHyY5VbmjJYw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__8UznGiEZRJ66PUgctoD41Q
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__kxhyWhVdTWSYBei6QqkkEg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-9
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-8
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__reXXImnUTWi-JFDto9wcPA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__Adw36Y1HSkaiKeM7aDvZsw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__5wN7EHRmQkmDgQB1LRL0Mg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__PAkhtZ-tToWvNHPETOnIRQ
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__N_nC43ttTnqqPWfKsM-1LA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__RyUJrO0vR5eHc5-gesQuCQ
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/___ks9BPdgQ_Wmg9dTPB5riw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-9
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-8
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-9
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-9
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-9
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-9
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__jP-5WAKcQoyuM5PuqX_KqA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__40HZYMk3SOGCwhULH0HWiw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__AUNsHEfmTWaZPVwV5qhnwg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__1f5W8G-PSweSOUcvmjZBzw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-10
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-9
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__qrl4kxh6Sfm-wfuwy7ikYA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__UgXHu7kzRcejfpNPrp_2wA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__8oTJbtM8T7K7Qj3-_nAyLA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__6NXZllXnQcy4evD9MNehgw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-10
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-9
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-10
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-10
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-10
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-10
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__rKN9hhgWQKug4U7fRzkTcg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__hjh2TUZFSIyeOUqH8hLiHQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__-ygLUOjyQgyK9NKyL-eRXg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__T5ZpxKPKQY6eR2Kbv-bHxA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-11
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-10
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__5Nev_Hr4S0iIoNU81n-bJw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__ZYHomcoyR0KklVkq7SpExA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__NN61t1QvTHGjEjg4Ohflxg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__sS5DkQKXSA2Sg4ClToGiBQ
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-11
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-10
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-11
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-11
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-11
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-y6YzmBMHTTG21X8NXJo_pg.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-y6YzmBMHTTG21X8NXJo_pg.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-y6YzmBMHTTG21X8NXJo_pg.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-11
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__xUO_KVtVSL2z1EkFhm9lXg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__De3blkKpQ-Cg4t1p_SchhQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__w1q106I9Tfax4VsBJaxctw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__FzQ1TiwzTVixbFl0EEQAhA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-y6YzmBMHTTG21X8NXJo_pg.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-12
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-11
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__8eAuMU-mRMS07PvJR7aF0A
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__9PQ3ZMpiRVOJHF36dGvkXw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__6xFYoEcOSr2xXgnF03UBFw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__qNHW9dxUS8OXgF3If91GYA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-y6YzmBMHTTG21X8NXJo_pg.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-12
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-11
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-y6YzmBMHTTG21X8NXJo_pg.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-12
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-12
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-12
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-osvS9j47TWaJIpycwppW2Q.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-osvS9j47TWaJIpycwppW2Q.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-osvS9j47TWaJIpycwppW2Q.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-12
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__rGHpJJltSRikbXtj5-07Iw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/___v6aAmhBSleQbyO4euq1YQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__mGiRUZ9ISNmsiQ2KUGAt6g
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__mt6EQoBIQVSx_TsoyzkStg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-osvS9j47TWaJIpycwppW2Q.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-13
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-12
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__FYc2qI60TpKORnzzzRC-lg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__mltZllLiRseUzGz_z7_Qyg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__4VapAkjJT6SfJehFy5hXzg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__g00WRqtBS1yv0X5GcIY4Ww
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-osvS9j47TWaJIpycwppW2Q.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-13
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-12
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-osvS9j47TWaJIpycwppW2Q.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-13
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-13
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-13
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-SHYTWtxRRuCw4pSn-viaRw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-SHYTWtxRRuCw4pSn-viaRw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-SHYTWtxRRuCw4pSn-viaRw.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-13
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__v8HnTX35RKqIb-EYvfCMWg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__9f9hMlVvQOifKRJUeOKvPw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__aY4g_gO0QemO3hCQb0mMzQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__Uhsh0S7JQz2xS7qDXCK7Fw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-SHYTWtxRRuCw4pSn-viaRw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-14
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-13
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__WXYh5uSpR5C0MLyYMvwERw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__QHGPBw31SMqASDwj6LlyAA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__iaAYD1fYQaS_Zi2S7NJ0qg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__U3gpQG96QKKHRbnTALvamg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-SHYTWtxRRuCw4pSn-viaRw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-14
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-13
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-SHYTWtxRRuCw4pSn-viaRw.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-14
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-14
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-14
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-lNLDwg0aTSyOSc_F_Jf63g.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-lNLDwg0aTSyOSc_F_Jf63g.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-lNLDwg0aTSyOSc_F_Jf63g.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-14
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__zMeWBONGQlOc71_Ij0OoaQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__6p98UC83Sv6m_npYuJKtTQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__v9rcc02oQAGvHAjAJH3VKQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__FF1AIvj2TFSTrjMI1Swk9A
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-lNLDwg0aTSyOSc_F_Jf63g.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-15
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-14
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__sCPjFRdeRqWTTIwLLaW-Pg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__2gMVQPkORyKM2jEPsLP3Iw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__SQIf_mIAS9qRGxXIfVsrFg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__YsWWpt_pQiCMpBSdXdwVJg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-lNLDwg0aTSyOSc_F_Jf63g.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-15
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-14
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-lNLDwg0aTSyOSc_F_Jf63g.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-15
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-15
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-15
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-2yUhQaMWRcy8mCbRCY3kKg.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-2yUhQaMWRcy8mCbRCY3kKg.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-2yUhQaMWRcy8mCbRCY3kKg.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-15
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__VSQhxN47RZ6or9N33b4Xrg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__1RTFdSdsQVOLBOQdl1hzPg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__yNNaeeVLTHG9PcmyUx_96Q
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__sadDzPEAQFCQo3wXbdNUFQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-2yUhQaMWRcy8mCbRCY3kKg.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-16
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-15
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__LihS3agTS_SXWCMLdC0oiQ
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__MbPK6SKORWaIsqu6CUmGmA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__ZfaqVpmZShGDkwa9bQCA0g
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__WmubStLrT5a3PSHl3a3Xtw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-2yUhQaMWRcy8mCbRCY3kKg.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-16
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-15
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-2yUhQaMWRcy8mCbRCY3kKg.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-16
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-16
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-16
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-3TllCTfyT0C7X7xxndXoUA.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-3TllCTfyT0C7X7xxndXoUA.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-3TllCTfyT0C7X7xxndXoUA.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-16
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__hH40A_toRLWuSXtQXKnG9A
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__u1b8JINcSvCfntE67AH6Gg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__pEiU7ZBYQn6wC_bm50E4pQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__C-s8neGLQdmSD08bXsco1A
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-3TllCTfyT0C7X7xxndXoUA.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-17
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-16
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__eOvNuhVeQtG8YHtiPXgofA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__LRQN5EjxQz69bXh6QAAH6A
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__3IZEyoP3Shu-LrfoA3DRsw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__3DZeiG6BQWe_cUkZCAyXDA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-3TllCTfyT0C7X7xxndXoUA.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-17
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-16
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-3TllCTfyT0C7X7xxndXoUA.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-17
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-17
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-17
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-Mls41mO4St-8BL7K-UjTaw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-Mls41mO4St-8BL7K-UjTaw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-Mls41mO4St-8BL7K-UjTaw.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-17
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__cVovpgThRsaDneHlpx2QoA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__OW40ZrR5SF65KvuKxVl-SQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__98e8HHAUTPetpL8X47C75w
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__9vMFFV6gRfa_zPcNHV3DFQ
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-Mls41mO4St-8BL7K-UjTaw.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-18
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-17
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__Ymk55Nd5QmmFCHjFe7AFxg
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__yFq9G7-OSDCtMTmSICJIsw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__1Pm_hIfIRHK7L0yuGaXBnQ
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/___OW_rV9kTqyB0EDhcRZ0Kw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-Mls41mO4St-8BL7K-UjTaw.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-18
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-17
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-Mls41mO4St-8BL7K-UjTaw.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-18
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-18
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-18
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/meta-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //GET /testb/?prefix=testpath%2Findices%2Fkl4KeozKR-Sh13vKmu2ZSA%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-18
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__mGhVYAgQRzWYr1GBiOuvjg
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__RDBIp0_zRXChqVebvC8ftw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__1HW-_ERNS-uX1esfho96Nw
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/__omkWFExKQ2SpKiuREYRqqA
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //PUT /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/index-19
        //GET /testb/?prefix=testpath%2Findices%2FfLnoMV_3SIiNnw_i71O1MQ%2F0%2F&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-18
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__FiKkEiD1SBeZ1Udl62oeSw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__WQRS2mZVTlelA7p5-AE3PA
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__WTOu71IISU25rr-umsjJ5w
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/__E1BrwIJgT_KcU_uiK61OPw
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //PUT /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/index-19
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-18
        //GET /testb/testpath/incompatible-snapshots
        //PUT /testb/testpath/snap-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //PUT /testb/testpath/index-19
        //PUT /testb/testpath/index.latest
        //GET /testb/?prefix=testpath%2Findex-&delimiter=%2F&encoding-type=url
        //GET /testb/testpath/index-19
        //GET /testb/testpath/incompatible-snapshots
        //GET /testb/testpath/snap-Mmfni2BhQlOBqKvfWaUp7w.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-Mmfni2BhQlOBqKvfWaUp7w.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-Mmfni2BhQlOBqKvfWaUp7w.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-Mmfni2BhQlOBqKvfWaUp7w.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-Mmfni2BhQlOBqKvfWaUp7w.dat
        //GET /testb/testpath/snap-P4dPqlvrR-O-obrf6zbDbQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-P4dPqlvrR-O-obrf6zbDbQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-P4dPqlvrR-O-obrf6zbDbQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-P4dPqlvrR-O-obrf6zbDbQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-P4dPqlvrR-O-obrf6zbDbQ.dat
        //GET /testb/testpath/snap-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-bLJTtqBlTVSpSz6qc2o4ZQ.dat
        //GET /testb/testpath/snap-ptamsa36Ro-HmpJH-cXErw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-ptamsa36Ro-HmpJH-cXErw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-ptamsa36Ro-HmpJH-cXErw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-ptamsa36Ro-HmpJH-cXErw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-ptamsa36Ro-HmpJH-cXErw.dat
        //GET /testb/testpath/snap-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-wUl2FW4GQvaNXDr3ILo8Uw.dat
        //GET /testb/testpath/snap-HW2DY9irSMmYfBB9rf4tUg.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-HW2DY9irSMmYfBB9rf4tUg.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-HW2DY9irSMmYfBB9rf4tUg.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-HW2DY9irSMmYfBB9rf4tUg.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-HW2DY9irSMmYfBB9rf4tUg.dat
        //GET /testb/testpath/snap-vK671Hr_ScuOjM40JKL1BQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-vK671Hr_ScuOjM40JKL1BQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-vK671Hr_ScuOjM40JKL1BQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-vK671Hr_ScuOjM40JKL1BQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-vK671Hr_ScuOjM40JKL1BQ.dat
        //GET /testb/testpath/snap-EB1m0HeLRHyVRVz_uUqz1g.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-EB1m0HeLRHyVRVz_uUqz1g.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-EB1m0HeLRHyVRVz_uUqz1g.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-EB1m0HeLRHyVRVz_uUqz1g.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-EB1m0HeLRHyVRVz_uUqz1g.dat
        //GET /testb/testpath/snap-f_r04g26T6mTmudby3nNvw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-f_r04g26T6mTmudby3nNvw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-f_r04g26T6mTmudby3nNvw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-f_r04g26T6mTmudby3nNvw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-f_r04g26T6mTmudby3nNvw.dat
        //GET /testb/testpath/snap-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-BZ1Nwvh6RmuRoXLY17KmJg.dat
        //GET /testb/testpath/snap-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-veQ6JtLrTYu6XLgW0sc6MQ.dat
        //GET /testb/testpath/snap-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-VgImJ9dPSe-_CP4vFrKQTQ.dat
        //GET /testb/testpath/snap-y6YzmBMHTTG21X8NXJo_pg.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-y6YzmBMHTTG21X8NXJo_pg.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-y6YzmBMHTTG21X8NXJo_pg.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-y6YzmBMHTTG21X8NXJo_pg.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-y6YzmBMHTTG21X8NXJo_pg.dat
        //GET /testb/testpath/snap-osvS9j47TWaJIpycwppW2Q.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-osvS9j47TWaJIpycwppW2Q.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-osvS9j47TWaJIpycwppW2Q.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-osvS9j47TWaJIpycwppW2Q.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-osvS9j47TWaJIpycwppW2Q.dat
        //GET /testb/testpath/snap-SHYTWtxRRuCw4pSn-viaRw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-SHYTWtxRRuCw4pSn-viaRw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-SHYTWtxRRuCw4pSn-viaRw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-SHYTWtxRRuCw4pSn-viaRw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-SHYTWtxRRuCw4pSn-viaRw.dat
        //GET /testb/testpath/snap-lNLDwg0aTSyOSc_F_Jf63g.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-lNLDwg0aTSyOSc_F_Jf63g.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-lNLDwg0aTSyOSc_F_Jf63g.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-lNLDwg0aTSyOSc_F_Jf63g.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-lNLDwg0aTSyOSc_F_Jf63g.dat
        //GET /testb/testpath/snap-2yUhQaMWRcy8mCbRCY3kKg.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-2yUhQaMWRcy8mCbRCY3kKg.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-2yUhQaMWRcy8mCbRCY3kKg.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-2yUhQaMWRcy8mCbRCY3kKg.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-2yUhQaMWRcy8mCbRCY3kKg.dat
        //GET /testb/testpath/snap-3TllCTfyT0C7X7xxndXoUA.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-3TllCTfyT0C7X7xxndXoUA.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-3TllCTfyT0C7X7xxndXoUA.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-3TllCTfyT0C7X7xxndXoUA.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-3TllCTfyT0C7X7xxndXoUA.dat
        //GET /testb/testpath/snap-Mls41mO4St-8BL7K-UjTaw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-Mls41mO4St-8BL7K-UjTaw.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-Mls41mO4St-8BL7K-UjTaw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-Mls41mO4St-8BL7K-UjTaw.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-Mls41mO4St-8BL7K-UjTaw.dat
        //GET /testb/testpath/snap-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/meta-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //GET /testb/testpath/indices/fLnoMV_3SIiNnw_i71O1MQ/0/snap-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/meta-d2EmkoAJTbqGd5W_jKmJPQ.dat
        //GET /testb/testpath/indices/kl4KeozKR-Sh13vKmu2ZSA/0/snap-d2EmkoAJTbqGd5W_jKmJPQ.dat
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
