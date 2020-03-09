/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import javax.crypto.AEADBadTagException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public final class EncryptedRepository extends BlobStoreRepository {
    static final Logger logger = LogManager.getLogger(EncryptedRepository.class);
    // the following constants are fixed by definition
    static final int GCM_TAG_LENGTH_IN_BYTES = 16;
    static final int GCM_IV_LENGTH_IN_BYTES = 12;
    static final int AES_BLOCK_LENGTH_IN_BYTES = 128;
    // the following constants require careful thought before changing because they will break backwards compatibility
    static final String DATA_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final int DATA_KEY_LENGTH_IN_BYTES = 32;
    static final int ENCRYPTED_DATA_KEY_LENGTH_IN_BYTES = DATA_KEY_LENGTH_IN_BYTES + 8; // https://www.ietf.org/rfc/rfc3394.txt section 2.2
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 8 << 20; // 8MB
    // this should be smaller than {@code #MAX_PACKET_LENGTH_IN_BYTES} and it's what {@code EncryptionPacketsInputStream} uses
    // during encryption and what {@code DecryptionPacketsInputStream} expects during decryption (it is not configurable)
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB
    // The "ID" of any KEK is the key-wrap ciphertext of this fixed 32 byte wide array.
    // Key wrapping encryption is deterministic (same plaintext generates the same ciphertext)
    // and the probability that two different keys map the same plaintext to the same ciphertext is very small
    // (2^-256, much lower than the UUID collision of 2^-128), assuming AES is indistinguishable from a pseudorandom permutation
    // a collision does not break security but it may corrupt the repository
    private static final byte[] KEY_ID_PLAINTEXT = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
    // the path of the blob container holding all the DEKs
    // this is relative to the root base path holding the encrypted blobs (i.e. the repository root base path)
    private static final String DEK_ROOT_CONTAINER = ".encryption-metadata";
    private static final int DEK_ID_LENGTH_IN_BYTES = 16; // {@code org.elasticsearch.common.UUIDS} length
    private static final int DEK_ID_LENGTH_IN_CHARS = 22; // Base64 encoding without padding

    // the following constants can be changed freely
    private static final String RAND_ALGO = "SHA1PRNG";
    // each snapshot metadata contains the salted password hash of the master node that started the snapshot operation
    // this hash is then verified on each data node before the actual shard files snapshot, as well as on the
    // master node that finalizes the snapshot (could be a different master node, if a master failover
    // has occurred in the mean time)
    private static final String KEK_ID_USER_METADATA_KEY = EncryptedRepository.class.getName() + ".repositoryKEKId";

    // this is the repository instance to which all blob reads and writes are forwarded to (it stores both the encrypted blobs, as well
    // as the associated encrypted DEKs)
    private final BlobStoreRepository delegatedRepository;
    // every data blob is encrypted with its randomly generated AES key (DEK)
    private final Supplier<SecretKey> DEKSupplier;
    // license is checked before every snapshot operations
    private final Supplier<XPackLicenseState> licenseStateSupplier;
    private final SecretKey repositoryKEK;
    private final String repositoryKEKId;
    private final Cache<String, SecretKey> DEKCache;

    /**
     * Returns the byte length (i.e. the storage size) of an encrypted blob, given the length of the blob's plaintext contents.
     *
     * @see EncryptionPacketsInputStream#getEncryptionLength(long, int)
     */
    public static long getEncryptedBlobByteLength(long plaintextBlobByteLength) {
        return 16L /* UUID byte length */ + EncryptionPacketsInputStream.getEncryptionLength(plaintextBlobByteLength,
                PACKET_LENGTH_IN_BYTES);
    }

    protected EncryptedRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, ClusterService clusterService,
                                  BlobStoreRepository delegatedRepository, Supplier<XPackLicenseState> licenseStateSupplier,
                                  SecretKey repositoryKEK) throws GeneralSecurityException {
        super(metadata, namedXContentRegistry, clusterService, BlobPath.cleanPath());
        this.delegatedRepository = delegatedRepository;
        KeyGenerator dataEncryptionKeyGenerator = KeyGenerator.getInstance(DATA_ENCRYPTION_SCHEME.split("/")[0]);
        dataEncryptionKeyGenerator.init(DATA_KEY_LENGTH_IN_BYTES * Byte.SIZE, SecureRandom.getInstance(RAND_ALGO));
        this.DEKSupplier = () -> dataEncryptionKeyGenerator.generateKey();
        this.licenseStateSupplier = licenseStateSupplier;
        this.repositoryKEK = repositoryKEK;
        this.repositoryKEKId = computeKeyId(repositoryKEK);
        this.DEKCache = CacheBuilder.<String, SecretKey>builder().setMaximumWeight(1000).build();
        if (isReadOnly() != delegatedRepository.isReadOnly()) {
            throw new IllegalStateException("The encrypted repository must be read-only iff the delegate repository is read-only");
        }
    }

    /**
     * The repository hook method which populates the snapshot metadata with the salted password hash of the repository on the (master)
     * node that starts of the snapshot operation. All the other actions associated with the same snapshot operation will first verify
     * that the local repository password checks with the hash from the snapshot metadata.
     * <p>
     * In addition, if the installed license does not comply with encrypted snapshots, this throws an exception, which aborts the snapshot
     * operation.
     *
     * See {@link org.elasticsearch.repositories.Repository#adaptUserMetadata(Map)}.
     *
     * @param userMetadata the snapshot metadata as received from the calling user
     * @return the snapshot metadata containing the salted password hash of the node initializing the snapshot
     */
    @Override
    public Map<String, Object> adaptUserMetadata(Map<String, Object> userMetadata) {
        // because populating the snapshot metadata must be done before the actual snapshot is first initialized,
        // we take the opportunity to validate the license and abort if non-compliant
        if (false == licenseStateSupplier.get().isEncryptedSnapshotAllowed()) {
            throw LicenseUtils.newComplianceException("encrypted snapshots");
        }
        Map<String, Object> snapshotUserMetadata = new HashMap<>();
        if (userMetadata != null) {
            snapshotUserMetadata.putAll(userMetadata);
        }
        // set out the ID of the repository key
        // this is then checked before every snapshot operation (i.e. {@link #snapshotShard} and {@link #finalizeSnapshot})
        // to assure that all participating nodes in the snapshot operation are using the same key
        snapshotUserMetadata.put(KEK_ID_USER_METADATA_KEY, repositoryKEKId);
        return snapshotUserMetadata;
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, ShardGenerations shardGenerations, long startTime, String failure,
                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                 boolean includeGlobalState, MetaData clusterMetaData, Map<String, Object> userMetadata,
                                 Version repositoryMetaVersion, ActionListener<SnapshotInfo> listener) {
        try {
            checkRepositoryKEK(userMetadata);
            // remove the repository key id from the snapshot metadata so that the id is not displayed in the API response to the user
            userMetadata = new HashMap<>(userMetadata);
            userMetadata.remove(KEK_ID_USER_METADATA_KEY);
        } catch (RepositoryException KEKValidationException) {
            listener.onFailure(KEKValidationException);
            return;
        }
        super.finalizeSnapshot(snapshotId, shardGenerations, startTime, failure, totalShards, shardFailures, repositoryStateId,
                includeGlobalState, clusterMetaData, userMetadata, repositoryMetaVersion, listener);
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus, Version repositoryMetaVersion,
                              Map<String, Object> userMetadata, ActionListener<String> listener) {
        try {
            checkRepositoryKEK(userMetadata);
        } catch (RepositoryException KEKValidationException) {
            listener.onFailure(KEKValidationException);
            return;
        }
        super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, snapshotStatus, repositoryMetaVersion,
                userMetadata, listener);
    }

    @Override
    protected BlobStore createBlobStore() {
        return new EncryptedBlobStore(delegatedRepository, DEKSupplier, repositoryKEK, repositoryKEKId, DEKCache);
    }

    @Override
    protected void doStart() {
        this.delegatedRepository.start();
        super.doStart();
    }

    @Override
    protected void doStop() {
        super.doStop();
        this.delegatedRepository.stop();
    }

    @Override
    protected void doClose() {
        super.doClose();
        this.delegatedRepository.close();
    }

    private static class EncryptedBlobStore implements BlobStore {

        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final Supplier<SecretKey> DEKSupplier;
        private final SecretKey repositoryKEK;
        private final String repositoryKEKId;
        private final Cache<String, SecretKey> DEKCache;
        EncryptedBlobStore(BlobStoreRepository delegatedBlobStoreRepository,
                           Supplier<SecretKey> DEKSupplier,
                           SecretKey repositoryKEK,
                           String repositoryKEKId,
                           Cache<String, SecretKey> DEKCache) {
            this.delegatedBlobStore = delegatedBlobStoreRepository.blobStore();
            this.delegatedBasePath = delegatedBlobStoreRepository.basePath();
            this.DEKSupplier = DEKSupplier;
            this.repositoryKEK = repositoryKEK;
            this.repositoryKEKId = repositoryKEKId;
            this.DEKCache = DEKCache;
        }

        @Override
        public void close() throws IOException {
            delegatedBlobStore.close();
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new EncryptedBlobContainer(delegatedBlobStore, delegatedBasePath, path, DEKSupplier, repositoryKEK, repositoryKEKId,
                    DEKCache);
        }
    }

    private static class EncryptedBlobContainer extends AbstractBlobContainer {

        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final Supplier<SecretKey> DEKSupplier;
        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer DEKBlobContainer;
        private final SecretKey repositoryKEK;
        private final String repositoryKEKId;
        private final Cache<String, SecretKey> DEKCache;

        EncryptedBlobContainer(BlobStore delegatedBlobStore,
                               BlobPath delegatedBasePath,
                               BlobPath path, // this contains the {@code EncryptedRepository#basePath} but that is empty
                               Supplier<SecretKey> DEKSupplier,
                               SecretKey repositoryKEK,
                               String repositoryKEKId,
                               Cache<String, SecretKey> DEKCache) {
            super(path);
            this.delegatedBlobStore = delegatedBlobStore;
            this.delegatedBasePath = delegatedBasePath;
            this.DEKSupplier = DEKSupplier;
            this.delegatedBlobContainer = delegatedBlobStore.blobContainer(delegatedBasePath.append(path));
            this.DEKBlobContainer = delegatedBlobStore.blobContainer(delegatedBasePath.add(DEK_ROOT_CONTAINER).add(repositoryKEKId));
            this.repositoryKEK = repositoryKEK;
            this.repositoryKEKId = repositoryKEKId;
            this.DEKCache = DEKCache;
        }

        private SecretKey loadDEK(String DEKId) throws IOException, GeneralSecurityException {
            final byte[] encryptedDEKBytes = new byte[ENCRYPTED_DATA_KEY_LENGTH_IN_BYTES];
            try (InputStream encryptedDEKInputStream = DEKBlobContainer.readBlob(DEKId)) {
                int bytesRead = encryptedDEKInputStream.readNBytes(encryptedDEKBytes, 0, ENCRYPTED_DATA_KEY_LENGTH_IN_BYTES);
                if (bytesRead != ENCRYPTED_DATA_KEY_LENGTH_IN_BYTES) {
                    throw new IllegalArgumentException("Encrypted DEK with id [" + DEKId + "] has unexpected length [" + bytesRead + "]");
                }
                if (encryptedDEKInputStream.read() != -1) {
                    throw new IllegalArgumentException("Encrypted DEK with id [" + DEKId + "] is larger than expected");
                }
            }
            return EncryptedRepositoryPlugin.unwrapAESKey(repositoryKEK, encryptedDEKBytes);
        }

        private void storeDEK(String DEKId, SecretKey DEK) throws IOException, GeneralSecurityException{
            final byte[] encryptedDEKBytes = EncryptedRepositoryPlugin.wrapAESKey(repositoryKEK, DEK);
            try (InputStream encryptedDEKInputStream = new ByteArrayInputStream(encryptedDEKBytes)) {
                DEKBlobContainer.writeBlobAtomic(DEKId, encryptedDEKInputStream, encryptedDEKBytes.length, true);
            }
        }

        /**
         * Returns a new {@link InputStream} for the given {@code blobName} that can be used to read the contents of the blob.
         * The returned {@code InputStream} transparently handles the decryption of the blob contents, by first working out
         * the blob name of the associated DEK id, reading and decrypting the DEK (given the repository secret key, unless the DEK is
         * already cached because other blobs required it before), and lastly reading and decrypting the data blob,
         * in a streaming fashion, by employing the {@link DecryptionPacketsInputStream}.
         * The {@code DecryptionPacketsInputStream} does not return un-authenticated data.
         *
         * @param   blobName The name of the blob to get an {@link InputStream} for.
         */
        @Override
        public InputStream readBlob(String blobName) throws IOException {
            // This MIGHT require two concurrent readBlob connections if the DEK is not already in the cache and if the encrypted blob
            // is large enough so that the underlying network library keeps the connection open after reading the prepended DEK ID.
            // Arguably this is a problem only under lab conditions, when the storage service is saturated only by the first read
            // connection of the pair, so that the second read connection (for the DEK) can not be fulfilled.
            // In this case the second connection will time-out which will trigger the closing of the first one, therefore
            // allowing other pair connections to complete.
            // In this situation the restore process should slowly make headway, albeit under read-timeout exceptions
            final InputStream encryptedDataInputStream = delegatedBlobContainer.readBlob(blobName);
            try {
                // read the DEK Id (fixed length) which is prepended to the encrypted blob
                final byte[] DEKIdBytes = new byte[DEK_ID_LENGTH_IN_BYTES];
                int bytesRead = encryptedDataInputStream.readNBytes(DEKIdBytes, 0, DEK_ID_LENGTH_IN_BYTES);
                if (bytesRead != DEK_ID_LENGTH_IN_BYTES) {
                    throw new IllegalArgumentException("The encrypted blob is too small [" + bytesRead + "]");
                }
                final String DEKId = new String(Base64.getUrlEncoder().withoutPadding().encode(DEKIdBytes), StandardCharsets.UTF_8);
                final SecretKey DEK;
                try {
                    DEK = DEKCache.computeIfAbsent(DEKId, ignored -> loadDEK(DEKId));
                } catch (ExecutionException e) {
                    throw new IOException("Failed to load DEK", e);
                }
                // read and decrypt the data blob
                return new DecryptionPacketsInputStream(encryptedDataInputStream, DEK, PACKET_LENGTH_IN_BYTES);
            } catch (IOException e) {
                try {
                    encryptedDataInputStream.close();
                } catch (IOException closeEx) {
                    e.addSuppressed(closeEx);
                }
                throw e;
            }
        }

        /**
         * Reads the blob content from the input stream and writes it to the container in a new blob with the given name.
         * If {@code failIfAlreadyExists} is {@code true} and a blob with the same name already exists, the write operation will fail;
         * otherwise, if {@code failIfAlreadyExists} is {@code false} the blob is overwritten.
         * The contents are encrypted in a streaming fashion. The encryption key is randomly generated for each blob.
         * The encryption key is separately stored in a metadata blob, which is encrypted with another key derived from the repository
         * password. The metadata blob is stored first, before the encrypted data blob, so as to ensure that no encrypted data blobs
         * are left without the associated metadata, in any failure scenario.
         *
         * @param   blobName
         *          The name of the blob to write the contents of the input stream to.
         * @param   inputStream
         *          The input stream from which to retrieve the bytes to write to the blob.
         * @param   blobSize
         *          The size of the blob to be written, in bytes.  It is implementation dependent whether
         *          this value is used in writing the blob to the repository.
         * @param   failIfAlreadyExists
         *          whether to throw a FileAlreadyExistsException if the given blob already exists
         */
        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            final SecretKey dataEncryptionKey = DEKSupplier.get();
            final int nonce = encryptionNonceSupplier.get();
            // this is the metadata required to decrypt back the (soon to be) encrypted blob
            final BlobEncryptionMetadata metadata = new BlobEncryptionMetadata(nonce, PACKET_LENGTH_IN_BYTES, dataEncryptionKey);
            // encrypt the metadata
            final byte[] encryptedMetadata;
            try {
                encryptedMetadata = BlobEncryptionMetadata.serializeMetadata(metadata, metadataEncryption::encrypt);
            } catch (IOException e) {
                throw new IOException("Failure to encrypt metadata for blob [" + blobName + "]", e);
            }
            // the metadata identifier is a sufficiently long random byte array so as to make it practically unique
            // the goal is to avoid overwriting metadata blobs even if the encrypted data blobs are overwritten
            final MetadataIdentifier metadataIdentifier = metadataIdentifierSupplier.get();
            final String metadataBlobName = MetadataIdentifier.formMetadataBlobName(blobName, metadataIdentifier);
            // first write the encrypted metadata to a UNIQUE blob name
            try (ByteArrayInputStream encryptedMetadataInputStream = new ByteArrayInputStream(encryptedMetadata)) {
                DEKBlobContainer.writeBlob(metadataBlobName, encryptedMetadataInputStream, encryptedMetadata.length, true
                        /* fail in the exceptional case of metadata blob name conflict */);
            }
            // afterwards write the encrypted data blob
            // prepended to the encrypted data blob is the unique identifier (fixed length) of the metadata blob
            final long encryptedBlobSize = (long) MetadataIdentifier.byteLength() +
                    EncryptionPacketsInputStream.getEncryptionLength(blobSize, PACKET_LENGTH_IN_BYTES);
            try (InputStream encryptedInputStream =
                         ChainingInputStream.chain(new ByteArrayInputStream(metadataIdentifier.asByteArray()),
                                 new EncryptionPacketsInputStream(inputStream, dataEncryptionKey, nonce, PACKET_LENGTH_IN_BYTES))) {
                delegatedBlobContainer.writeBlob(blobName, encryptedInputStream, encryptedBlobSize, failIfAlreadyExists);
            }
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws IOException {
            // the encrypted repository does not offer an alternative implementation for atomic writes
            // fallback to regular write
            writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            // first delete the encrypted data blob
            DeleteResult deleteResult = delegatedBlobContainer.delete();
            // then delete metadata
            try {
                deleteResult = deleteResult.add(DEKBlobContainer.delete());
            } catch (IOException e) {
                // the encryption metadata blob container might not exist at all
                logger.warn("Failure to delete metadata blob container " + DEKBlobContainer.path(), e);
            }
            return deleteResult;
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            Objects.requireNonNull(blobNames);

            // find all the blob names that must be deleted
            Set<String> blobNamesSet = new HashSet<>(blobNames);
            Set<String> blobNamesToDelete = new HashSet<>();
            for (String existingBlobName : delegatedBlobContainer.listBlobs().keySet()) {
                if (blobNamesSet.contains(existingBlobName)) {
                    blobNamesToDelete.add(existingBlobName);
                }
            }

            // find all the metadata blob names that must be deleted
            Map<String, List<String>> blobNamesToMetadataNamesToDelete = new HashMap<>(blobNamesToDelete.size());
            Set<String> allMetadataBlobNames = new HashSet<>();
            try {
                allMetadataBlobNames = DEKBlobContainer.listBlobs().keySet();
            } catch (IOException e) {
                // the metadata blob container might not even exist
                // the encrypted data is the "anchor" for encrypted blobs, if those are removed, the encrypted blob as a whole is
                // considered removed, even if, technically, the metadata is still lingering (it should later be removed by cleanup)
                // therefore this tolerates metadata delete failures, when data deletes are successful
                logger.warn("Failure to list blobs of metadata blob container " + DEKBlobContainer.path(), e);
            }
            for (String metadataBlobName : allMetadataBlobNames) {
                final String blobNameForMetadata;
                try {
                    blobNameForMetadata = MetadataIdentifier.parseFromMetadataBlobName(metadataBlobName).v1();
                } catch (IllegalArgumentException e) {
                    // ignore invalid metadata blob names, which most likely have been created externally
                    continue;
                }
                // group metadata blob names to their associated blob name
                if (blobNamesToDelete.contains(blobNameForMetadata)) {
                    blobNamesToMetadataNamesToDelete.computeIfAbsent(blobNameForMetadata, k -> new ArrayList<>(1))
                            .add(metadataBlobName);
                }
            }
            // Metadata deletes when there are multiple for the same blob is un-safe, so don't try it now.
            // It is unsafe because metadata "appears" before the data and there could be an overwrite in progress for which only
            // the metadata, but not the encrypted data, shows up.
            List<String> metadataBlobNamesToDelete = new ArrayList<>(blobNamesToMetadataNamesToDelete.size());
            blobNamesToMetadataNamesToDelete.entrySet().forEach(entry -> {
                if (entry.getValue().size() == 1) {
                    metadataBlobNamesToDelete.add(entry.getValue().get(0));
                }
                // technically, duplicate metadata written during olden repository generations could be removed here as well,
                // but this code should not be aware of what a repository generation is, so let the metadata linger, it will
                // be garbage collected by cleanup
            });

            // then delete the encrypted data blobs
            delegatedBlobContainer.deleteBlobsIgnoringIfNotExists(new ArrayList<>(blobNamesToDelete));

            // lastly delete metadata blobs
            try {
                DEKBlobContainer.deleteBlobsIgnoringIfNotExists(metadataBlobNamesToDelete);
            } catch (IOException e) {
                logger.warn("Failure to delete metadata blobs " + metadataBlobNamesToDelete + " from blob container "
                        + DEKBlobContainer.path(), e);
            }
        }

        @Override
        public Map<String, BlobMetaData> listBlobs() throws IOException {
            // The encrypted data blobs "anchor" the metadata-data blob pair, i.e. the encrypted blob "exists" if only the data exists.
            // In all circumstances, barring an "external" access to the repository, the metadata associated to the data must exist.
            return delegatedBlobContainer.listBlobs();
        }

        @Override
        public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            // The encrypted data blobs "anchor" the metadata-data blob pair, i.e. the encrypted blob "exists" if only the data exists.
            // In all circumstances, barring an "external" access to the repository, the metadata associated to the data must exist.
            return delegatedBlobContainer.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            // the encrypted data blob container is the source-of-truth for child container operations
            // the metadata blob container mirrors its structure, but in some failure cases it might contain
            // additional orphaned metadata blobs
            Map<String, BlobContainer> childEncryptedBlobContainers = delegatedBlobContainer.children();
            Map<String, BlobContainer> result = new HashMap<>(childEncryptedBlobContainers.size());
            for (Map.Entry<String, BlobContainer> encryptedBlobContainer : childEncryptedBlobContainers.entrySet()) {
                if (encryptedBlobContainer.getValue().path().equals(DEKBlobContainer.path())) {
                    // do not descend recursively into the metadata blob container itself
                    continue;
                }
                // get an encrypted blob container for each child
                // Note that the encryption metadata blob container might be missing
                result.put(encryptedBlobContainer.getKey(), new EncryptedBlobContainer(delegatedBlobStore, delegatedBasePath,
                        path.add(encryptedBlobContainer.getKey()), DEKSupplier, metadataEncryption,
                        encryptionNonceSupplier, metadataIdentifierSupplier));
            }
            return result;
        }
    }

    private static String computeKeyId(SecretKey secretKey) throws GeneralSecurityException {
        return new String(Base64.getUrlEncoder().withoutPadding().encode(EncryptedRepositoryPlugin.wrapAESKey(secretKey,
                new SecretKeySpec(KEY_ID_PLAINTEXT,"AES"))), StandardCharsets.UTF_8);
    }

    /**
     * Called before the shard snapshot and finalize operations, on the data and master nodes. This validates that the repository
     * key on the master node that started the snapshot operation is the same with the repository key on the current node.
     *
     * @param snapshotUserMetadata the snapshot metadata containing the repository key id to verify
     * @throws RepositoryException if the repository key id on the local node mismatches or cannot be verified from the
     * master's, as seen in the {@code snapshotUserMetadata}
     */
    private void checkRepositoryKEK(Map<String, Object> snapshotUserMetadata) throws RepositoryException {
        if (snapshotUserMetadata == null) {
            throw new RepositoryException(metadata.name(), "Unexpected fatal internal error",
                    new IllegalStateException("Null snapshot metadata"));
        }
        final Object masterKEKId = snapshotUserMetadata.get(KEK_ID_USER_METADATA_KEY);
        if (false == masterKEKId instanceof String) {
            throw new RepositoryException(metadata.name(), "Unexpected fatal internal error",
                    new IllegalStateException("Snapshot metadata does not contain the repository key id as a String"));
        }
        if (false == repositoryKEKId.equals(masterKEKId)) {
            throw new RepositoryException(metadata.name(),
                    "Repository key id mismatch. The local node's repository key, the keystore secure setting [" +
                            EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.getConcreteSettingForNamespace(
                                    EncryptedRepositoryPlugin.KEK_NAME_SETTING.get(metadata.settings())).getKey() +
                            "], is different compared to the elected master node's, which started the snapshot operation");
        }
    }

}
