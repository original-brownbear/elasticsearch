/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public final class LocalStateEncryptedRepositoryPlugin extends LocalStateCompositeXPackPlugin {

    final EncryptedRepositoryPlugin encryptedRepositoryPlugin;

    public LocalStateEncryptedRepositoryPlugin(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        final LocalStateEncryptedRepositoryPlugin thisVar = this;

        encryptedRepositoryPlugin = new EncryptedRepositoryPlugin() {

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }

            @Override
            protected EncryptedRepository createEncryptedRepository(
                RepositoryMetaData metaData,
                NamedXContentRegistry registry,
                ClusterService clusterService,
                BlobStoreRepository delegatedRepository,
                Supplier<XPackLicenseState> licenseStateSupplier,
                char[] repoPassword
            ) throws GeneralSecurityException {
                return new TestEncryptedRepository(
                    metaData,
                    registry,
                    clusterService,
                    delegatedRepository,
                    licenseStateSupplier,
                    repoPassword
                );
            }
        };
        plugins.add(encryptedRepositoryPlugin);
    }

    static class TestEncryptedRepository extends EncryptedRepository {
        private final Lock snapshotShardLock = new ReentrantLock();
        private final Condition snapshotShardCondition = snapshotShardLock.newCondition();
        private final AtomicBoolean snapshotShardBlock = new AtomicBoolean(false);

        TestEncryptedRepository(
            RepositoryMetaData metaData,
            NamedXContentRegistry registry,
            ClusterService clusterService,
            BlobStoreRepository delegatedRepository,
            Supplier<XPackLicenseState> licenseStateSupplier,
            char[] repoPassword
        )
            throws GeneralSecurityException {
            super(metaData, registry, clusterService, delegatedRepository, licenseStateSupplier, repoPassword);
        }

        @Override
        public void snapshotShard(
            Store store,
            MapperService mapperService,
            SnapshotId snapshotId,
            IndexId indexId,
            IndexCommit snapshotIndexCommit,
            String shardStateIdentifier,
            IndexShardSnapshotStatus snapshotStatus,
            Version repositoryMetaVersion,
            Map<String, Object> userMetadata,
            ActionListener<String> listener
        ) {
            snapshotShardLock.lock();
            try {
                while (snapshotShardBlock.get()) {
                    snapshotShardCondition.await();
                }
                super.snapshotShard(
                    store,
                    mapperService,
                    snapshotId,
                    indexId,
                    snapshotIndexCommit,
                    shardStateIdentifier,
                    snapshotStatus,
                    repositoryMetaVersion,
                    userMetadata,
                    listener
                );
            } catch (InterruptedException e) {
                listener.onFailure(e);
            } finally {
                snapshotShardLock.unlock();
            }
        }

        void blockSnapshotShard() {
            snapshotShardLock.lock();
            try {
                snapshotShardBlock.set(true);
                snapshotShardCondition.signalAll();
            } finally {
                snapshotShardLock.unlock();
            }
        }

        void unblockSnapshotShard() {
            snapshotShardLock.lock();
            try {
                snapshotShardBlock.set(false);
                snapshotShardCondition.signalAll();
            } finally {
                snapshotShardLock.unlock();
            }
        }
    }

}
