/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
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

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class EncryptedRepository extends BlobStoreRepository {
    static final Logger logger = LogManager.getLogger(EncryptedRepository.class);
    // the following constants are fixed by definition
    static final int GCM_TAG_LENGTH_IN_BYTES = 16;
    static final int GCM_IV_LENGTH_IN_BYTES = 12;
    static final int AES_BLOCK_LENGTH_IN_BYTES = 128;
    // the following constants require careful thought before changing because they will break backwards compatibility
    static final String DATA_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final int DATA_KEY_LENGTH_IN_BYTES = 32; // 256-bit AES key
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
    private static final byte[] KEK_ID_PLAINTEXT = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
    // the path of the blob container holding all the DEKs
    // this is relative to the root base path holding the encrypted blobs (i.e. the repository root base path)
    private static final String DEK_ROOT_CONTAINER = ".encryption-metadata";
    static final int DEK_ID_LENGTH_IN_BYTES = 16; // {@code org.elasticsearch.common.UUIDS} length
    private static final int DEK_ID_LENGTH_IN_CHARS = 22; // Base64 encoding without padding

    // the following constants can be changed freely
    private static final String RAND_ALGO = "SHA1PRNG";
    // each snapshot metadata contains the salted password hash of the master node that started the snapshot operation
    // this hash is then verified on each data node before the actual shard files snapshot, as well as on the
    // master node that finalizes the snapshot (could be a different master node, if a master failover
    // has occurred in the mean time)
    private static final String KEK_ID_USER_METADATA_KEY = EncryptedRepository.class.getName() + ".repositoryKEKId";
    private static final int DEK_CACHE_WEIGHT = 2048;

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
        return (long) DEK_ID_LENGTH_IN_BYTES /* UUID byte length */
                + EncryptionPacketsInputStream.getEncryptionLength(plaintextBlobByteLength, PACKET_LENGTH_IN_BYTES);
    }

    protected EncryptedRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, ClusterService clusterService,
                                  BlobStoreRepository delegatedRepository, Supplier<XPackLicenseState> licenseStateSupplier,
                                  SecretKey repositoryKEK) throws GeneralSecurityException {
        super(metadata, namedXContentRegistry, clusterService, BlobPath.cleanPath() /* the encrypted repository uses a hardcoded empty
        base blob path but the base path setting is honored for the delegated repository */);
        this.delegatedRepository = delegatedRepository;
        // DEKs are generated randomly
        KeyGenerator dataEncryptionKeyGenerator = KeyGenerator.getInstance(DATA_ENCRYPTION_SCHEME.split("/")[0]);
        dataEncryptionKeyGenerator.init(DATA_KEY_LENGTH_IN_BYTES * Byte.SIZE, SecureRandom.getInstance(RAND_ALGO));
        this.DEKSupplier = () -> dataEncryptionKeyGenerator.generateKey();
        this.licenseStateSupplier = licenseStateSupplier;
        this.repositoryKEK = repositoryKEK;
        this.repositoryKEKId = computeKEKId(repositoryKEK);
        // stores decrypted DEKs; DEKs are reused to encrypt/decrypt multiple independent blobs
        this.DEKCache = CacheBuilder.<String, SecretKey>builder().setMaximumWeight(DEK_CACHE_WEIGHT).build();
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
        Supplier<SecretKey> DEKSupplier = this.DEKSupplier;
        if (isReadOnly()) {
            // make sure that a read-only repository can't encrypt anything
            DEKSupplier = () -> {
                throw new IllegalStateException("DEKs are required for encryption but this is a read-only repository");
            };
        }
        return new EncryptedBlobStore(delegatedRepository.blobStore(), delegatedRepository.basePath(), repositoryKEK,
                repositoryKEKId, DEKSupplier, DEKCache);
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

        private static final UseOnceDEK EXPIRED_DEK = new UseOnceDEK(null, null, Integer.MAX_VALUE);

        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final BlobPath DEKBasePath;
        private final SecretKey repositoryKEK;
        private final Supplier<SecretKey> DEKSupplier;
        private final Cache<String, SecretKey> DEKCache;
        private final AtomicReference<UseOnceDEK> DEKCurrentlyInUse;

        EncryptedBlobStore(BlobStore delegatedBlobStore,
                           BlobPath delegatedBasePath,
                           SecretKey repositoryKEK,
                           String repositoryKEKId,
                           Supplier<SecretKey> DEKSupplier,
                           Cache<String, SecretKey> DEKCache) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.delegatedBasePath = delegatedBasePath;
            this.DEKBasePath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(repositoryKEKId);
            this.repositoryKEK = repositoryKEK;
            this.DEKSupplier = DEKSupplier;
            this.DEKCache = DEKCache;
            this.DEKCurrentlyInUse = new AtomicReference<>(EXPIRED_DEK);
        }

        private SecretKey loadDEK(String DEKId, BlobContainer DEKBlobContainer) throws IOException {
            final byte[] encryptedDEKBytes = new byte[ENCRYPTED_DATA_KEY_LENGTH_IN_BYTES];
            try (InputStream encryptedDEKInputStream = DEKBlobContainer.readBlob(DEKId)) {
                int bytesRead = encryptedDEKInputStream.readNBytes(encryptedDEKBytes, 0, ENCRYPTED_DATA_KEY_LENGTH_IN_BYTES);
                if (bytesRead != ENCRYPTED_DATA_KEY_LENGTH_IN_BYTES) {
                    throw new ElasticsearchException("Encrypted DEK [" + DEKId + "] has unexpected length [" + bytesRead + "]");
                }
                if (encryptedDEKInputStream.read() != -1) {
                    throw new ElasticsearchException("Encrypted DEK [" + DEKId + "] is larger than expected");
                }
            } catch (NoSuchFileException e) {
                // do NOT throw IOException when the DEK does not exist, as this is a decryption problem, and IOExceptions
                // can move the repository in the corrupted state
                throw new ElasticsearchException("Failure to read and decrypt DEK [" + DEKId + "] from " + DEKBlobContainer.path() +
                        ". Most likely the repository key is incorrect, as previous snapshots have used a different key.", e);
            }
            try {
                return EncryptedRepositoryPlugin.unwrapAESKey(repositoryKEK, encryptedDEKBytes);
            } catch (GeneralSecurityException e) {
                throw new ElasticsearchException("Failure to AES wrap the DEK", e);
            }
        }

        private void storeDEK(String DEKId, SecretKey DEK, BlobContainer DEKBlobContainer) throws IOException {
            final byte[] encryptedDEKBytes;
            try {
                encryptedDEKBytes = EncryptedRepositoryPlugin.wrapAESKey(repositoryKEK, DEK);
            } catch (GeneralSecurityException e) {
                // throw unchecked ElasticsearchException; IOExceptions are interpreted differently and can move the repository in the
                // corrupted state
                throw new ElasticsearchException("Failure to AES wrap the DEK with id [" + DEKId + "]", e);
            }
            try (InputStream encryptedDEKInputStream = new ByteArrayInputStream(encryptedDEKBytes)) {
                DEKBlobContainer.writeBlobAtomic(DEKId, encryptedDEKInputStream, encryptedDEKBytes.length, true);
            }
        }

        private CheckedSupplier<UseOnceDEK, IOException> createNonceAndDEKSupplier(Supplier<SecretKey> DEKSupplier,
                                                                                   AtomicReference<UseOnceDEK> DEKCurrentlyInUse,
                                                                                   BlobContainer DEKBlobContainer) {
            return () -> {
                while (true) {
                    final UseOnceDEK nonceAndDEK = DEKCurrentlyInUse.getAndUpdate(prev -> prev.nonce < Integer.MAX_VALUE ?
                            new UseOnceDEK(prev.DEK, prev.DEKId, prev.nonce + 1) : EXPIRED_DEK);
                    if (nonceAndDEK.nonce < Integer.MAX_VALUE) {
                        logger.trace(() -> new ParameterizedMessage("DEK with id [{}] reused with nonce [{}]", nonceAndDEK.DEKId,
                                nonceAndDEK.nonce));
                        return nonceAndDEK;
                    } else {
                        logger.trace(() -> new ParameterizedMessage("Regenerating a new DEK to replace id [{}]", nonceAndDEK.DEKId));
                        synchronized (this) {
                            if (DEKCurrentlyInUse.get().nonce == Integer.MAX_VALUE) {
                                final SecretKey newDEK = DEKSupplier.get();
                                final String newDEKId = UUIDs.randomBase64UUID();
                                if (newDEKId.length() != DEK_ID_LENGTH_IN_CHARS) {
                                    throw new IllegalStateException("");
                                }
                                logger.debug(() -> new ParameterizedMessage("A new DEK with id [{}] has been generated", newDEKId));
                                storeDEK(newDEKId, newDEK, DEKBlobContainer);
                                logger.debug(() -> new ParameterizedMessage("DEK with id [{}] has been stored under [{}]", newDEKId,
                                        DEKBlobContainer.path()));
                                DEKCurrentlyInUse.set(new UseOnceDEK(newDEK, newDEKId, Integer.MIN_VALUE));
                            }
                        }
                    }
                }
            };
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            BlobContainer delegatedBlobContainer = delegatedBlobStore.blobContainer(delegatedBasePath.append(path));
            BlobContainer DEKBlobContainer = delegatedBlobStore.blobContainer(DEKBasePath);
            return new EncryptedBlobContainer(path, delegatedBlobContainer,
                    createNonceAndDEKSupplier(DEKSupplier, DEKCurrentlyInUse, DEKBlobContainer),
                    DEKId -> {
                        try {
                            return DEKCache.computeIfAbsent(DEKId, ignored -> loadDEK(DEKId, DEKBlobContainer));
                        } catch (ExecutionException e) {
                            // some exception types are to be expected
                            if (e.getCause() instanceof IOException) {
                                throw (IOException) e.getCause();
                            } else if (e.getCause() instanceof ElasticsearchException) {
                                throw (ElasticsearchException) e.getCause();
                            } else {
                                throw new ElasticsearchException("Unexpected exception retrieving DEK for id [" + DEKId + "]", e);
                            }
                        }
                    });
        }

        @Override
        public void close() {
            // do NOT close delegatedBlobStore; it will be closed when the inner delegatedRepository is closed
        }
    }

    private static class EncryptedBlobContainer extends AbstractBlobContainer {

        private final BlobContainer delegatedBlobContainer;
        private final CheckedSupplier<UseOnceDEK, IOException> nonceAndDEKSupplier;
        private final CheckedFunction<String, SecretKey, IOException> getDEKById;

        EncryptedBlobContainer(BlobPath path, // this path contains the {@code EncryptedRepository#basePath} which, importantly, is empty
                               BlobContainer delegatedBlobContainer,
                               CheckedSupplier<UseOnceDEK, IOException> nonceAndDEKSupplier,
                               CheckedFunction<String, SecretKey, IOException> getDEKById) {
            super(path);
            if (DEK_ROOT_CONTAINER.equals(path.getRootPath())) {
                throw new IllegalArgumentException("Cannot descend into the DEK blob container");
            }
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.nonceAndDEKSupplier = nonceAndDEKSupplier;
            this.getDEKById = getDEKById;
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
                    throw new ElasticsearchException("The encrypted blob [" + blobName + "] is too small [" + bytesRead + "]");
                }
                final String DEKId = new String(Base64.getUrlEncoder().withoutPadding().encode(DEKIdBytes), StandardCharsets.UTF_8);
                // might open a connection to read and decrypt the DEK, but most likely it will be served from cache
                final SecretKey DEK = getDEKById.apply(DEKId);
                // read and decrypt the rest of the blob
                return new DecryptionPacketsInputStream(encryptedDataInputStream, DEK, PACKET_LENGTH_IN_BYTES);
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
         * The contents are encrypted in a streaming fashion. The DEK (encryption key) is randomly generated and reused for encrypting
         * subsequent blobs such that the same IV is not reused together with the same key.
         * The DEK encryption key is separately stored in a different blob, which is encrypted with the repository key.
         *
         * @param   blobName
         *          The name of the blob to write the contents of the input stream to.
         * @param   inputStream
         *          The input stream from which to retrieve the bytes to write to the blob.
         * @param   blobSize
         *          The size of the blob to be written, in bytes. The actual number of bytes written to the storage service is larger
         *          because of encryption and authentication overhead. It is implementation dependent whether this value is used
         *          in writing the blob to the repository.
         * @param   failIfAlreadyExists
         *          whether to throw a FileAlreadyExistsException if the given blob already exists
         */
        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            // reuse, but possibly generate and store a new DEK
            final UseOnceDEK nonceAndDEK = nonceAndDEKSupplier.get();
            final byte[] DEKIdBytes = Base64.getUrlDecoder().decode(nonceAndDEK.DEKId.getBytes(StandardCharsets.UTF_8));
            if (DEKIdBytes.length != DEK_ID_LENGTH_IN_BYTES) {
                throw new IllegalStateException("Unexpected DEK Id length [" + DEKIdBytes.length + "]");
            }
            final long encryptedBlobSize = getEncryptedBlobByteLength(blobSize);
            try (InputStream encryptedInputStream = ChainingInputStream.chain(new ByteArrayInputStream(DEKIdBytes),
                    new EncryptionPacketsInputStream(inputStream, nonceAndDEK.DEK, nonceAndDEK.nonce, PACKET_LENGTH_IN_BYTES))) {
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
            return delegatedBlobContainer.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            delegatedBlobContainer.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetaData> listBlobs() throws IOException {
            return delegatedBlobContainer.listBlobs();
        }

        @Override
        public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return delegatedBlobContainer.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            final Map<String, BlobContainer> childEncryptedBlobContainers = delegatedBlobContainer.children();
            final Map<String, BlobContainer> resultBuilder = new HashMap<>(childEncryptedBlobContainers.size());
            for (Map.Entry<String, BlobContainer> childBlobContainer : childEncryptedBlobContainers.entrySet()) {
                if (childBlobContainer.getKey().equals(DEK_ROOT_CONTAINER) && path().isEmpty()) {
                    // do not descend into the DEK blob container
                    continue;
                }
                // get an encrypted blob container for each child
                // Note that the encryption metadata blob container might be missing
                resultBuilder.put(childBlobContainer.getKey(), new EncryptedBlobContainer(path().add(childBlobContainer.getKey()),
                        childBlobContainer.getValue(), nonceAndDEKSupplier, getDEKById));
            }
            return Map.copyOf(resultBuilder);
        }
    }

    /**
     * The ID of the repository key (KEK) is the ciphertext of a known plaintext, using the AES Wrap algorithm.
     * AES Wrap algorithm is deterministic, i.e. encryption using the same key, of the same plaintext, generates the same ciphertext.
     * Moreover, the ciphertext reveals no information on the key, and the probability of collision of ciphertexts given different
     * keys is statistically negligible.
     */
    private static String computeKEKId(SecretKey repositoryKEK) throws GeneralSecurityException {
        return new String(Base64.getUrlEncoder().withoutPadding().encode(EncryptedRepositoryPlugin.wrapAESKey(repositoryKEK,
                new SecretKeySpec(KEK_ID_PLAINTEXT,"AES"))), StandardCharsets.UTF_8);
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

    private static class UseOnceDEK {
        final SecretKey DEK;
        final String DEKId;
        final Integer nonce;

        UseOnceDEK(SecretKey DEK, String DEKId, Integer nonce) {
            this.DEK = DEK;
            this.DEKId = DEKId;
            this.nonce = nonce;
        }
    }

}
