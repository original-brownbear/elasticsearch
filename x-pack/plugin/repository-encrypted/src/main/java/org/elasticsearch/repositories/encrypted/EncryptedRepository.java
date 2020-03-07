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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.threadpool.ThreadPool;

import javax.crypto.AEADBadTagException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class EncryptedRepository extends BlobStoreRepository {
    static final Logger logger = LogManager.getLogger(EncryptedRepository.class);
    // the following constants are fixed by definition
    static final int GCM_TAG_LENGTH_IN_BYTES = 16;
    static final int GCM_IV_LENGTH_IN_BYTES = 12;
    static final int AES_BLOCK_LENGTH_IN_BYTES = 128;
    // changing the following constants implies breaking compatibility with previous versions of encrypted snapshots
    // in this case the {@link #CURRENT_ENCRYPTION_VERSION_NUMBER} MUST be incremented
    static final String DATA_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final int DATA_KEY_LENGTH_IN_BITS = 256;
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 8 << 20; // 8MB
    // this can be changed freely (can be made a repository parameter) without adjusting
    // the {@link #CURRENT_ENCRYPTION_VERSION_NUMBER}, as long as it stays under the value
    // of {@link #MAX_PACKET_LENGTH_IN_BYTES}
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB
    static final String SALTED_PASSWORD_HASH_ALGO = "PBKDF2WithHmacSHA512";
    static final int SALTED_PASSWORD_HASH_ITER_COUNT = 10000;
    static final int SALTED_PASSWORD_HASH_KEY_LENGTH_IN_BITS = 512;
    static final int PASSWORD_HASH_SALT_LENGTH_IN_BYES = 16;
    static final String RAND_ALGO = "SHA1PRNG";
    static final String DEK_CIPHER_ALGO = "AES";

    // each snapshot metadata contains the salted password hash of the master node that started the snapshot operation
    // this hash is then verified on each data node before the actual shard files snapshot, as well as on the
    // master node that finalizes the snapshot (could be a different master node, if a master failover
    // has occurred in the mean time)
    private static final String PASSWORD_HASH_RESERVED_USER_METADATA_KEY = EncryptedRepository.class.getName() + ".saltedPasswordHash";
    // The encryption scheme version number to which the current implementation conforms to.
    // The version number MUST be incremented whenever the format of the metadata, or
    // the way the metadata is used for the actual decryption are changed.
    // Incrementing the version number signals that previous implementations cannot make sense
    // of the new scheme, so they will fail all operations on the repository.
    private static final int CURRENT_ENCRYPTION_VERSION_NUMBER = 2; // nobody trusts v1 of anything
    // the path of the blob container holding all the DEKs
    // this is relative to the root path holding the encrypted blobs (i.e. the repository root path)
    private static final String DEK_ROOT_CONTAINER = "encryption-metadata-v" + CURRENT_ENCRYPTION_VERSION_NUMBER;
    private static final String CURRENT_KEK_BLOB = "current-KEK-salt.info";

    // this is the repository instance to which all blob reads and writes are forwarded to
    private final BlobStoreRepository delegatedRepository;
    // every data blob is encrypted with its randomly generated AES key (this is the "Data Encryption Key")
    private final Supplier<SecretKey> dataEncryptionKeySupplier;
    private final Supplier<XPackLicenseState> licenseStateSupplier;
    // the salted hash of this repository's password on the local node. The password is fixed for the lifetime of the repository.
    private final String repositoryPasswordSaltedHash;
    // this is used to check that the salted hash of the repository password on the node that started the snapshot matches up with the
    // repository password on the local node
    private final HashVerifier passwordHashVerifier;

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
                                  SecretKey repositoryKEK) throws NoSuchAlgorithmException {
        super(metadata, namedXContentRegistry, clusterService, BlobPath.cleanPath());
        this.delegatedRepository = delegatedRepository;
        KeyGenerator dataEncryptionKeyGenerator = KeyGenerator.getInstance(DEK_CIPHER_ALGO);
        dataEncryptionKeyGenerator.init(DATA_KEY_LENGTH_IN_BITS, SecureRandom.getInstance(RAND_ALGO));
        this.dataEncryptionKeySupplier = () -> dataEncryptionKeyGenerator.generateKey();
        this.licenseStateSupplier = licenseStateSupplier;
        // the salted password hash for this encrypted repository password, on the local node (this is constant)
        this.repositoryPasswordSaltedHash = computeSaltedPBKDF2Hash(SecureRandom.getInstance(RAND_ALGO),
                password);
        // used to verify that the salted password hash in the snapshot metadata matches up with the repository password on the local node
        this.passwordHashVerifier = new HashVerifier(password);
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
        // pin down the salted hash of the repository password
        // this is then checked before every snapshot operation (i.e. {@link #snapshotShard} and {@link #finalizeSnapshot})
        // to assure that all participating nodes in the snapshot have the same repository password set
        snapshotUserMetadata.put(PASSWORD_HASH_RESERVED_USER_METADATA_KEY, this.repositoryPasswordSaltedHash);
        return snapshotUserMetadata;
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, ShardGenerations shardGenerations, long startTime, String failure,
                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                 boolean includeGlobalState, MetaData clusterMetaData, Map<String, Object> userMetadata,
                                 Version repositoryMetaVersion, ActionListener<SnapshotInfo> listener) {
        try {
            validateRepositoryPasswordHash(userMetadata);
            // remove the repository password hash from the snapshot metadata, after all repository password verifications
            // have completed, so that the hash is not displayed in the API response to the user
            userMetadata = new HashMap<>(userMetadata);
            userMetadata.remove(PASSWORD_HASH_RESERVED_USER_METADATA_KEY);
        } catch (Exception passValidationException) {
            listener.onFailure(passValidationException);
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
            validateRepositoryPasswordHash(userMetadata);
        } catch (Exception passValidationException) {
            listener.onFailure(passValidationException);
            return;
        }
        super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, snapshotStatus, repositoryMetaVersion,
                userMetadata, listener);
    }

    @Override
    public void cleanup(long repositoryStateId, Version repositoryMetaVersion, ActionListener<RepositoryCleanupResult> listener) {
        if (isReadOnly()) {
            listener.onFailure(new RepositoryException(metadata.name(), "cannot run cleanup on readonly repository"));
            return;
        }
        final StepListener<RepositoryCleanupResult> baseCleanupStep = new StepListener<>();
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

        super.cleanup(repositoryStateId, repositoryMetaVersion, baseCleanupStep);

        baseCleanupStep.whenComplete(baseCleanupResult -> {
            final GroupedActionListener<DeleteResult> groupedListener = new GroupedActionListener<>(ActionListener.wrap(deleteResults -> {
                DeleteResult deleteResult = new DeleteResult(baseCleanupResult.blobs(), baseCleanupResult.bytes());
                for (DeleteResult result : deleteResults) {
                    deleteResult = deleteResult.add(result);
                }
                listener.onResponse(new RepositoryCleanupResult(deleteResult));
            }, listener::onFailure), 2);

            // clean unreferenced metadata blobs on the root blob container
            executor.execute(ActionRunnable.supply(groupedListener, () -> {
                EncryptedBlobContainer encryptedBlobContainer = (EncryptedBlobContainer) blobContainer();
                return cleanUnreferencedEncryptionMetadata(encryptedBlobContainer);
            }));

            // clean indices blob containers
            executor.execute(ActionRunnable.supply(groupedListener, () -> {
                EncryptedBlobContainer indicesBlobContainer = (EncryptedBlobContainer) blobStore().blobContainer(indicesPath());
                Map<String, BlobContainer> metadataIndices = indicesBlobContainer.encryptionMetadataBlobContainer.children();
                Map<String, BlobContainer> dataIndices = indicesBlobContainer.delegatedBlobContainer.children();
                DeleteResult deleteResult = DeleteResult.ZERO;
                for (Map.Entry<String, BlobContainer> metadataIndexContainer : metadataIndices.entrySet()) {
                    if (false == dataIndices.containsKey(metadataIndexContainer.getKey())) {
                        // the index metadata blob container exists but the encrypted data blob container does not
                        Long indexGeneration = findFirstGeneration(metadataIndexContainer.getValue());
                        if (indexGeneration != null && indexGeneration < latestKnownRepoGen.get()) {
                            logger.debug("[{}] Found stale metadata index container [{}]. Cleaning it up", metadata.name(),
                                    metadataIndexContainer.getValue().path());
                            deleteResult = deleteResult.add(metadataIndexContainer.getValue().delete());
                            logger.debug("[{}] Cleaned up stale metadata index container [{}]", metadata.name(),
                                    metadataIndexContainer.getValue().path());
                        }
                    }
                }
                return deleteResult;
            }));

        }, listener::onFailure);
    }

    @Override
    protected BlobStore createBlobStore() {
        return new EncryptedBlobStore(delegatedRepository, dataEncryptionKeySupplier, metadataEncryption,
                encryptionNonceSupplier, metadataIdentifierSupplier);
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

    private DeleteResult cleanUnreferencedEncryptionMetadata(EncryptedBlobContainer blobContainer) throws IOException {
        Map<String, BlobMetaData> allMetadataBlobs = blobContainer.encryptionMetadataBlobContainer.listBlobs();
        Map<String, BlobMetaData> allDataBlobs = blobContainer.delegatedBlobContainer.listBlobs();
        // map from the data blob name to all the associated metadata
        Map<String, List<Tuple<MetadataIdentifier, String>>> metaDataByBlobName = new HashMap<>();
        List<String> metadataBlobsToDelete = new ArrayList<>();
        for (String metadataBlobName : allMetadataBlobs.keySet()) {
            final Tuple<String, MetadataIdentifier> blobNameAndMetaId;
            try {
                blobNameAndMetaId = MetadataIdentifier.parseFromMetadataBlobName(metadataBlobName);
            } catch (IllegalArgumentException e) {
                // ignore invalid metadata blob names, which most likely have been created externally
                logger.warn("Unrecognized blob name for metadata [" + metadataBlobName + "]", e);
                continue;
            }
            if (false == allDataBlobs.containsKey(blobNameAndMetaId.v1()) &&
                    blobNameAndMetaId.v2().repositoryGeneration < latestKnownRepoGen.get()) {
                // the data blob for this metadata blob is not going to appear, the repo moved to a new generation, which means that a
                // "parent" blob of it appeared
                metadataBlobsToDelete.add(blobNameAndMetaId.v1());
            }
            // group metadata blobs by their associated blob name
            metaDataByBlobName.computeIfAbsent(blobNameAndMetaId.v1(), k -> new ArrayList<>(1))
                    .add(new Tuple<>(blobNameAndMetaId.v2(), metadataBlobName));
        }
        metaDataByBlobName.entrySet().forEach(entry -> {
            if (entry.getValue().size() > 1) {
                // if there are multiple versions of the metadata, then remove ones created in olden repository generations
                // since overwrites cannot happen across repository generations
                long maxRepositoryGeneration =
                        entry.getValue().stream().map(meta -> meta.v1().repositoryGeneration).max(Long::compare).get();
                entry.getValue().forEach(meta -> {
                    if (meta.v1().repositoryGeneration < maxRepositoryGeneration) {
                        metadataBlobsToDelete.add(meta.v2());
                    }
                });
            }
        });
        logger.info("[{}] Found unreferenced metadata blobs {} at path {}. Cleaning them up", metadata.name(), metadataBlobsToDelete,
                blobContainer.encryptionMetadataBlobContainer.path());
        blobContainer().deleteBlobsIgnoringIfNotExists(metadataBlobsToDelete);
        return new DeleteResult(metadataBlobsToDelete.size(),
                metadataBlobsToDelete.stream().mapToLong(name -> allMetadataBlobs.get(name).length()).sum());
    }

    // aux "ugly" function which infers the repository generation under which an index blob container has been created
    private Long findFirstGeneration(BlobContainer metadataBlobContainer) throws IOException {
        for (String metaBlobName : metadataBlobContainer.listBlobs().keySet()) {
            try {
                return MetadataIdentifier.parseFromMetadataBlobName(metaBlobName).v2().repositoryGeneration;
            } catch (IllegalArgumentException e) {
                // ignored, let's find another meta blob name we can parse
            }
        }
        for (BlobContainer child : metadataBlobContainer.children().values()) {
            Long generation = findFirstGeneration(child);
            if (generation != null) {
                return generation;
            }
        }
        return null;
    };

    private static class EncryptedBlobStore implements BlobStore {

        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final Supplier<SecretKey> dataEncryptionKeySupplier;
        private final PasswordBasedEncryption metadataEncryption;
        private final Supplier<Integer> encryptionNonceSupplier;
        private final Supplier<MetadataIdentifier> metadataIdentifierSupplier;
        EncryptedBlobStore(BlobStoreRepository delegatedBlobStoreRepository, Supplier<SecretKey> dataEncryptionKeySupplier,
                           PasswordBasedEncryption metadataEncryption, Supplier<Integer> encryptionNonceSupplier,
                           Supplier<MetadataIdentifier> metadataIdentifierSupplier) {
            this.delegatedBlobStore = delegatedBlobStoreRepository.blobStore();
            this.delegatedBasePath = delegatedBlobStoreRepository.basePath();
            this.dataEncryptionKeySupplier = dataEncryptionKeySupplier;
            this.metadataEncryption = metadataEncryption;
            this.encryptionNonceSupplier = encryptionNonceSupplier;
            this.metadataIdentifierSupplier = metadataIdentifierSupplier;
        }

        @Override
        public void close() throws IOException {
            delegatedBlobStore.close();
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new EncryptedBlobContainer(delegatedBlobStore, delegatedBasePath, path, dataEncryptionKeySupplier, metadataEncryption,
                    encryptionNonceSupplier, metadataIdentifierSupplier);
        }
    }

    private static class EncryptedBlobContainer implements BlobContainer {

        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final BlobPath path;
        private final Supplier<SecretKey> dataEncryptionKeySupplier;
        private final PasswordBasedEncryption metadataEncryption;
        private final Supplier<Integer> encryptionNonceSupplier;
        private final Supplier<MetadataIdentifier> metadataIdentifierSupplier;
        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer encryptionMetadataBlobContainer;

        EncryptedBlobContainer(BlobStore delegatedBlobStore, BlobPath delegatedBasePath, BlobPath path,
                               Supplier<SecretKey> dataEncryptionKeySupplier, PasswordBasedEncryption metadataEncryption,
                               Supplier<Integer> encryptionNonceSupplier, Supplier<MetadataIdentifier> metadataIdentifierSupplier) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.delegatedBasePath = delegatedBasePath;
            this.path = path;
            this.dataEncryptionKeySupplier = dataEncryptionKeySupplier;
            this.metadataEncryption = metadataEncryption;
            this.encryptionNonceSupplier = encryptionNonceSupplier;
            this.metadataIdentifierSupplier = metadataIdentifierSupplier;
            this.delegatedBlobContainer = delegatedBlobStore.blobContainer(delegatedBasePath.append(path));
            this.encryptionMetadataBlobContainer =
                    delegatedBlobStore.blobContainer(delegatedBasePath.add(DEK_ROOT_CONTAINER).append(path));
        }

        /**
         * Returns the {@link BlobPath} to where the <b>encrypted</b> blobs are stored. Note that the encryption metadata is contained
         * in separate blobs which are stored under a different blob path (see
         * {@link #encryptionMetadataBlobContainer}). This blob path resembles the path of the <b>encrypted</b>
         * blobs but is rooted under a specific path component (see {@link #DEK_ROOT_CONTAINER}). The encryption is transparent
         * in the sense that the metadata is not exposed by the {@link EncryptedBlobContainer}.
         *
         * @return  the BlobPath to where the encrypted blobs are contained
         */
        @Override
        public BlobPath path() {
            return path;
        }

        /**
         * Returns a new {@link InputStream} for the given {@code blobName} that can be used to read the contents of the blob.
         * The returned {@code InputStream} transparently handles the decryption of the blob contents, by first working out
         * the blob name of the associated metadata, reading and decrypting the metadata (given the repository password and utilizing
         * the {@link PasswordBasedEncryption}) and lastly reading and decrypting the data blob, in a streaming fashion by employing the
         * {@link DecryptionPacketsInputStream}. The {@code DecryptionPacketsInputStream} does not return un-authenticated data.
         *
         * @param   blobName
         *          The name of the blob to get an {@link InputStream} for.
         */
        @Override
        public InputStream readBlob(String blobName) throws IOException {
            // this requires two concurrent readBlob connections so it's possible that, under lab conditions, the storage service
            // is saturated only by the first read connection of the pair, so that the second read connection (for the metadata) can not be
            // fulfilled. In this case the second connection will time-out which will trigger the closing of the first one, therefore
            // allowing other pair connections to complete. In this situation the restore process should slowly make headway, albeit under
            // read-timeout exceptions
            final InputStream encryptedDataInputStream = delegatedBlobContainer.readBlob(blobName);
            try {
                // read the metadata identifier (fixed length) which is prepended to the encrypted blob
                final byte[] metaId = encryptedDataInputStream.readNBytes(MetadataIdentifier.byteLength());
                if (metaId.length != MetadataIdentifier.byteLength()) {
                    throw new IOException("Failure to read encrypted blob metadata identifier");
                }
                final MetadataIdentifier metadataIdentifier = MetadataIdentifier.fromByteArray(metaId);
                // the metadata blob name is the name of the data blob followed by the base64 encoding (URL safe) of the metadata identifier
                final String metadataBlobName = MetadataIdentifier.formMetadataBlobName(blobName, metadataIdentifier);
                // read the encrypted metadata contents
                final BytesReference encryptedMetadataBytes = Streams.readFully(encryptionMetadataBlobContainer.readBlob(metadataBlobName));
                final BlobEncryptionMetadata metadata;
                try {
                    // decrypt and parse metadata
                    metadata = BlobEncryptionMetadata.deserializeMetadata(BytesReference.toBytes(encryptedMetadataBytes),
                            metadataEncryption::decrypt);
                } catch (IOException e) {
                    // friendlier exception message
                    String failureMessage = "Failure to decrypt metadata for blob [" + blobName + "]";
                    if (e.getCause() instanceof AEADBadTagException) {
                        failureMessage = failureMessage + ". The repository password is probably wrong.";
                    }
                    throw new IOException(failureMessage, e);
                }
                // read and decrypt the data blob
                return new DecryptionPacketsInputStream(encryptedDataInputStream, metadata.getDataEncryptionKey(), metadata.getNonce(),
                        metadata.getPacketLengthInBytes());
            } catch (Exception e) {
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
            final SecretKey dataEncryptionKey = dataEncryptionKeySupplier.get();
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
                encryptionMetadataBlobContainer.writeBlob(metadataBlobName, encryptedMetadataInputStream, encryptedMetadata.length, true
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
                deleteResult = deleteResult.add(encryptionMetadataBlobContainer.delete());
            } catch (IOException e) {
                // the encryption metadata blob container might not exist at all
                logger.warn("Failure to delete metadata blob container " + encryptionMetadataBlobContainer.path(), e);
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
                allMetadataBlobNames = encryptionMetadataBlobContainer.listBlobs().keySet();
            } catch (IOException e) {
                // the metadata blob container might not even exist
                // the encrypted data is the "anchor" for encrypted blobs, if those are removed, the encrypted blob as a whole is
                // considered removed, even if, technically, the metadata is still lingering (it should later be removed by cleanup)
                // therefore this tolerates metadata delete failures, when data deletes are successful
                logger.warn("Failure to list blobs of metadata blob container " + encryptionMetadataBlobContainer.path(), e);
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
                encryptionMetadataBlobContainer.deleteBlobsIgnoringIfNotExists(metadataBlobNamesToDelete);
            } catch (IOException e) {
                logger.warn("Failure to delete metadata blobs " + metadataBlobNamesToDelete + " from blob container "
                        + encryptionMetadataBlobContainer.path(), e);
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
                if (encryptedBlobContainer.getValue().path().equals(encryptionMetadataBlobContainer.path())) {
                    // do not descend recursively into the metadata blob container itself
                    continue;
                }
                // get an encrypted blob container for each child
                // Note that the encryption metadata blob container might be missing
                result.put(encryptedBlobContainer.getKey(), new EncryptedBlobContainer(delegatedBlobStore, delegatedBasePath,
                        path.add(encryptedBlobContainer.getKey()), dataEncryptionKeySupplier, metadataEncryption,
                        encryptionNonceSupplier, metadataIdentifierSupplier));
            }
            return result;
        }
    }

    private static String computeSaltedPBKDF2Hash(SecureRandom secureRandom, char[] password) {
        byte[] salt = new byte[PASSWORD_HASH_SALT_LENGTH_IN_BYES];
        secureRandom.nextBytes(salt);
        return computeSaltedPBKDF2Hash(salt, password);
    }

    private static String computeSaltedPBKDF2Hash(byte[] salt, char[] password) {
        final PBEKeySpec spec = new PBEKeySpec(password, salt, SALTED_PASSWORD_HASH_ITER_COUNT, SALTED_PASSWORD_HASH_KEY_LENGTH_IN_BITS);
        final byte[] hash;
        try {
            SecretKeyFactory pbkdf2KeyFactory = SecretKeyFactory.getInstance(SALTED_PASSWORD_HASH_ALGO);
            hash = pbkdf2KeyFactory.generateSecret(spec).getEncoded();
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Unexpected exception when computing the hash of the repository password", e);
        }
        return new String(Base64.getUrlEncoder().withoutPadding().encode(salt), StandardCharsets.UTF_8) + ":" +
                new String(Base64.getUrlEncoder().withoutPadding().encode(hash), StandardCharsets.UTF_8);
    }

    /**
     * Called before the shard snapshot and finalize operations, on the data and master nodes. This validates that the repository
     * password hash of the master node that started the snapshot operation matches with the repository password on the data nodes.
     *
     * @param snapshotUserMetadata the snapshot metadata to verify
     * @throws RepositoryException if the repository password on the local node mismatches or cannot be verified from the
     * master's password hash from {@code snapshotUserMetadata}
     */
    private void validateRepositoryPasswordHash(Map<String, Object> snapshotUserMetadata) throws RepositoryException {
        if (snapshotUserMetadata == null) {
            throw new RepositoryException(metadata.name(), "Unexpected fatal internal error",
                    new IllegalStateException("Null snapshot metadata"));
        }
        final Object repositoryPasswordHash = snapshotUserMetadata.get(PASSWORD_HASH_RESERVED_USER_METADATA_KEY);
        if (repositoryPasswordHash == null || (false == repositoryPasswordHash instanceof String)) {
            throw new RepositoryException(metadata.name(), "Unexpected fatal internal error",
                    new IllegalStateException("Snapshot metadata does not contain the repository password hash as a String"));
        }
        if (false == passwordHashVerifier.verify((String) repositoryPasswordHash)) {
            throw new RepositoryException(metadata.name(),
                    "Repository password mismatch. The local node's value of the keystore secure setting [" +
                            EncryptedRepositoryPlugin.KEY_ENCRYPTION_KEY_SETTING.getConcreteSettingForNamespace(metadata.name()).getKey() +
                            "] is different from the elected master node, which started the snapshot operation");
        }
    }

    /**
     * This is used to verify that salted hashes match up with the {@code password} from the constructor argument.
     * This also caches the last successfully verified hash, so that repeated checks for the same hash turn into a simple {@code String
     * #equals}.
     */
    private static class HashVerifier {
        // the password to which the salted hashes must match up with
        private final char[] password;
        // the last successfully matched salted hash
        private final AtomicReference<String> lastVerifiedHash;

        HashVerifier(char[] password) {
            this.password = password;
            this.lastVerifiedHash = new AtomicReference<>(null);
        }

        boolean verify(String saltedHash) {
            Objects.requireNonNull(saltedHash);
            // first check if this exact hash has been checked before
            if (saltedHash.equals(lastVerifiedHash.get())) {
                logger.debug("The repository salted password hash [" + saltedHash + "] is locally cached as VALID");
                return true;
            }
            String[] parts = saltedHash.split(":");
            // the hash has an invalid format
            if (parts == null || parts.length != 2) {
                logger.error("Unrecognized format for the repository password hash [" + saltedHash + "]");
                return false;
            }
            String salt = parts[0];
            logger.debug("Computing repository password hash");
            String computedHash = computeSaltedPBKDF2Hash(Base64.getUrlDecoder().decode(salt.getBytes(StandardCharsets.UTF_8)), password);
            if (false == computedHash.equals(saltedHash)) {
                return false;
            }
            // remember last successfully verified hash
            lastVerifiedHash.set(computedHash);
            logger.debug("Repository password hash [" + saltedHash + "] validated successfully and is now locally cached");
            return true;
        }

    }
}
