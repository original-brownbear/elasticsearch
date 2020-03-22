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
import org.elasticsearch.common.collect.Tuple;
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
import java.util.function.Function;
import java.util.function.Supplier;

public final class EncryptedRepository extends BlobStoreRepository {
    static final Logger logger = LogManager.getLogger(EncryptedRepository.class);
    // the following constants are fixed by definition
    static final int GCM_TAG_LENGTH_IN_BYTES = 16;
    static final int GCM_IV_LENGTH_IN_BYTES = 12;
    static final int AES_BLOCK_LENGTH_IN_BYTES = 128;
    // the following constants require careful thought before changing because they will break backwards compatibility
    static final String DATA_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 8 << 20; // 8MB
    // this should be smaller than {@code #MAX_PACKET_LENGTH_IN_BYTES} and it's what {@code EncryptionPacketsInputStream} uses
    // during encryption and what {@code DecryptionPacketsInputStream} expects during decryption (it is not configurable)
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB
    // the path of the blob container holding all the DEKs
    // this is relative to the root base path holding the encrypted blobs (i.e. the repository root base path)
    static final String DEK_ROOT_CONTAINER = ".encryption-metadata"; // package private for tests
    static final int DEK_ID_LENGTH_IN_BYTES = 16; // {@code org.elasticsearch.common.UUIDS} length
    private static final int DEK_ID_LENGTH_IN_CHARS = 22; // Base64 encoding without padding

    // the following constants can be changed freely
    private static final String RAND_ALGO = "SHA1PRNG";
    // each snapshot metadata contains an unforgeable identifier of the repository password of the master node that started the snapshot
    // this hash is then verified on each data node before the actual shard files snapshot, as well as on the
    // master node that finalizes the snapshot (could be a different master node, if a master failover occurred during the snapshot)
    private static final String PASSWORD_ID_USER_METADATA_KEY = EncryptedRepository.class.getName() + ".repositoryPasswordId";
    private static final String PASSWORD_ID_SALT_USER_METADATA_KEY = EncryptedRepository.class.getName() + ".repositoryPasswordIdSalt";
    private static final int DEK_CACHE_WEIGHT = 2048;

    // this is the repository instance to which all blob reads and writes are forwarded to (it stores both the encrypted blobs, as well
    // as the associated encrypted DEKs)
    private final BlobStoreRepository delegatedRepository;
    // every data blob is encrypted with its randomly generated AES key (DEK)
    private final Supplier<Tuple<SecretKey, String>> DEKGenerator;
    // license is checked before every snapshot operations; protected non-final for tests
    protected Supplier<XPackLicenseState> licenseStateSupplier;
    private final char[] repositoryPassword;
    private final String localRepositoryPasswordId;
    private final String localRepositoryPasswordIdSalt;
    private final AtomicReference<String> validatedRepositoryPasswordId;
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
                                  char[] repositoryPassword) throws GeneralSecurityException {
        super(metadata, namedXContentRegistry, clusterService, BlobPath.cleanPath() /* the encrypted repository uses a hardcoded empty
        base blob path but the base path setting is honored for the delegated repository */);
        this.delegatedRepository = delegatedRepository;
        this.DEKGenerator = createDEKGenerator();
        this.licenseStateSupplier = licenseStateSupplier;
        this.repositoryPassword = repositoryPassword;
        // the password "id" and validated
        this.localRepositoryPasswordIdSalt = UUIDs.randomBase64UUID();
        this.localRepositoryPasswordId = AESKeyUtils.computeId(AESKeyUtils.generatePasswordBasedKey(repositoryPassword,
                Base64.getUrlDecoder().decode(this.localRepositoryPasswordIdSalt.getBytes(StandardCharsets.UTF_8))));
        this.validatedRepositoryPasswordId = new AtomicReference<>(this.localRepositoryPasswordId);
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
        // set out the ID of the repository secret
        // this is then checked before every snapshot operation (i.e. {@link #snapshotShard} and {@link #finalizeSnapshot})
        // to assure that all participating nodes in the snapshot operation are using the same repository secret
        snapshotUserMetadata.put(PASSWORD_ID_SALT_USER_METADATA_KEY, localRepositoryPasswordIdSalt);
        snapshotUserMetadata.put(PASSWORD_ID_USER_METADATA_KEY, localRepositoryPasswordId);
        return snapshotUserMetadata;
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, ShardGenerations shardGenerations, long startTime, String failure,
                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                 boolean includeGlobalState, MetaData clusterMetaData, Map<String, Object> userMetadata,
                                 Version repositoryMetaVersion, ActionListener<SnapshotInfo> listener) {
        try {
            validateLocalRepositorySecret(userMetadata);
            // remove the repository key id from the snapshot metadata so that the id is not displayed in the API response to the user
            userMetadata = new HashMap<>(userMetadata);
            userMetadata.remove(PASSWORD_ID_USER_METADATA_KEY);
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
            validateLocalRepositorySecret(userMetadata);
        } catch (RepositoryException KEKValidationException) {
            listener.onFailure(KEKValidationException);
            return;
        }
        super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, snapshotStatus, repositoryMetaVersion,
                userMetadata, listener);
    }

    @Override
    protected BlobStore createBlobStore() {
        final Supplier<Tuple<SecretKey, String>> DEKGenerator;
        if (isReadOnly()) {
            // make sure that a read-only repository can't encrypt anything
            DEKGenerator = () -> {
                throw new IllegalStateException("DEKs are required for encryption but this is a read-only repository");
            };
        } else {
            DEKGenerator = this.DEKGenerator;
        }
        return new EncryptedBlobStore(delegatedRepository.blobStore(), delegatedRepository.basePath(), repositoryPassword, DEKGenerator,
                DEKCache);
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

    private static Supplier<Tuple<SecretKey, String>> createDEKGenerator() throws GeneralSecurityException {
        // DEK and DEK Ids are generated randomly
        final SecureRandom DEKSecureRandom = SecureRandom.getInstance(RAND_ALGO);
        final SecureRandom DEKIdSecureRandom = SecureRandom.getInstance(RAND_ALGO);
        final KeyGenerator dataEncryptionKeyGenerator = KeyGenerator.getInstance(DATA_ENCRYPTION_SCHEME.split("/")[0]);
        dataEncryptionKeyGenerator.init(AESKeyUtils.KEY_LENGTH_IN_BYTES * Byte.SIZE, DEKSecureRandom);
        return () -> new Tuple<>(dataEncryptionKeyGenerator.generateKey(), UUIDs.randomBase64UUID(DEKIdSecureRandom));
    }

    static class EncryptedBlobStore implements BlobStore {
        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final Function<String, Tuple<String, SecretKey>> getKEKforDEK;
        private final CheckedSupplier<SingleUseDEK, IOException> singleUseDEKSupplier;
        private final CheckedFunction<String, SecretKey, IOException> getDEKById;

        EncryptedBlobStore(BlobStore delegatedBlobStore,
                           BlobPath delegatedBasePath,
                           char[] repositoryPassword,
                           Supplier<Tuple<SecretKey, String>> DEKGenerator,
                           Cache<String, SecretKey> DEKCache) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.delegatedBasePath = delegatedBasePath;
            this.getKEKforDEK = DEKId -> {
                try {
                    SecretKey KEK = AESKeyUtils.generatePasswordBasedKey(repositoryPassword,
                            Base64.getUrlDecoder().decode(DEKId.getBytes(StandardCharsets.UTF_8)));
                    String KEKId = AESKeyUtils.computeId(KEK);
                    return new Tuple<>(KEKId, KEK);
                } catch (GeneralSecurityException e) {
                    throw new ElasticsearchException("Failure to generate KEK to wrap the DEK [" + DEKId + "]", e);
                }
            };
            this.singleUseDEKSupplier = SingleUseDEK.createSingleUseDEKSupplier(() -> {
                Tuple<SecretKey, String> newDEK = DEKGenerator.get();
                // store newly generated DEK before making it available
                storeDEK(newDEK.v2(), newDEK.v1());
                return newDEK;
            });
            this.getDEKById = DEKId -> {
                try {
                    return DEKCache.computeIfAbsent(DEKId, ignored -> loadDEK(DEKId));
                } catch (ExecutionException e) {
                    // some exception types are to be expected
                    if (e.getCause() instanceof IOException) {
                        throw (IOException) e.getCause();
                    } else if (e.getCause() instanceof ElasticsearchException) {
                        throw (ElasticsearchException) e.getCause();
                    } else {
                        throw new ElasticsearchException("Unexpected exception retrieving DEK [" + DEKId + "]", e);
                    }
                }
            };
        }

        private SecretKey loadDEK(String DEKId) throws IOException {
            final BlobContainer DEKBlobContainer = delegatedBlobStore.blobContainer(delegatedBasePath.add(DEK_ROOT_CONTAINER).add(DEKId));
            final Tuple<String, SecretKey> KEK = getKEKforDEK.apply(DEKId);
            final byte[] encryptedDEKBytes = new byte[AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES];
            try (InputStream encryptedDEKInputStream = DEKBlobContainer.readBlob(KEK.v1())) {
                int bytesRead = encryptedDEKInputStream.readNBytes(encryptedDEKBytes, 0, AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES);
                if (bytesRead != AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES) {
                    throw new ElasticsearchException("Encrypted DEK [" + DEKId + "] has unexpected length [" + bytesRead + "]");
                }
                if (encryptedDEKInputStream.read() != -1) {
                    throw new ElasticsearchException("Encrypted DEK [" + DEKId + "] is larger than expected");
                }
            } catch (NoSuchFileException e) {
                // do NOT throw IOException when the DEK does not exist, as this is a decryption problem, and IOExceptions
                // can move the repository in the corrupted state
                throw new ElasticsearchException("Failure to read and decrypt DEK [" + DEKId + "] from " + DEKBlobContainer.path() +
                        ". Most likely the repository password is incorrect, as previous snapshots have used a different key.", e);
            }
            try {
                return AESKeyUtils.unwrap(KEK.v2(), encryptedDEKBytes);
            } catch (GeneralSecurityException e) {
                throw new ElasticsearchException("Failure to AES wrap the DEK", e);
            }
        }

        private void storeDEK(String DEKId, SecretKey DEK) throws IOException {
            final BlobContainer DEKBlobContainer = delegatedBlobStore.blobContainer(delegatedBasePath.add(DEK_ROOT_CONTAINER).add(DEKId));
            final Tuple<String, SecretKey> KEK = getKEKforDEK.apply(DEKId);
            final byte[] encryptedDEKBytes;
            try {
                encryptedDEKBytes = AESKeyUtils.wrap(KEK.v2(), DEK);
            } catch (GeneralSecurityException e) {
                // throw unchecked ElasticsearchException; IOExceptions are interpreted differently and can move the repository in the
                // corrupted state
                throw new ElasticsearchException("Failure to AES wrap the DEK with id [" + DEKId + "]", e);
            }
            try (InputStream encryptedDEKInputStream = new ByteArrayInputStream(encryptedDEKBytes)) {
                DEKBlobContainer.writeBlobAtomic(KEK.v1(), encryptedDEKInputStream, encryptedDEKBytes.length, true);
            }
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final BlobContainer delegatedBlobContainer = delegatedBlobStore.blobContainer(delegatedBasePath.append(path));
            return new EncryptedBlobContainer(path, delegatedBlobContainer, singleUseDEKSupplier, getDEKById);
        }

        @Override
        public void close() {
            // do NOT close delegatedBlobStore; it will be closed when the inner delegatedRepository is closed
        }
    }

    private static class EncryptedBlobContainer extends AbstractBlobContainer {
        private final BlobContainer delegatedBlobContainer;
        // supplier for the DEK used for encryption (snapshot)
        private final CheckedSupplier<SingleUseDEK, IOException> singleUseDEKSupplier;
        // retrieves the DEK required for decryption (restore)
        private final CheckedFunction<String, SecretKey, IOException> getDEKById;

        EncryptedBlobContainer(BlobPath path, // this path contains the {@code EncryptedRepository#basePath} which, importantly, is empty
                               BlobContainer delegatedBlobContainer,
                               CheckedSupplier<SingleUseDEK, IOException> singleUseDEKSupplier,
                               CheckedFunction<String, SecretKey, IOException> getDEKById) {
            super(path);
            if (DEK_ROOT_CONTAINER.equals(path.getRootPath())) {
                throw new IllegalArgumentException("Cannot descend into the DEK blob container");
            }
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.singleUseDEKSupplier = singleUseDEKSupplier;
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
            final SingleUseDEK singleUseNonceAndDEK = singleUseDEKSupplier.get();
            final byte[] DEKIdBytes = Base64.getUrlDecoder().decode(singleUseNonceAndDEK.DEKId.getBytes(StandardCharsets.UTF_8));
            if (DEKIdBytes.length != DEK_ID_LENGTH_IN_BYTES) {
                throw new IllegalStateException("Unexpected DEK Id length [" + DEKIdBytes.length + "]");
            }
            final long encryptedBlobSize = getEncryptedBlobByteLength(blobSize);
            try (InputStream encryptedInputStream = ChainingInputStream.chain(new ByteArrayInputStream(DEKIdBytes),
                    new EncryptionPacketsInputStream(inputStream, singleUseNonceAndDEK.DEK,
                            singleUseNonceAndDEK.nonce, PACKET_LENGTH_IN_BYTES))) {
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
                        childBlobContainer.getValue(), singleUseDEKSupplier, getDEKById));
            }
            return Map.copyOf(resultBuilder);
        }
    }

    /**
     * Called before the shard snapshot and finalize operations, on the data and master nodes. This validates that the repository
     * secret on the master node that started the snapshot operation is identical to the repository secret on the local node.
     *
     * @param snapshotUserMetadata the snapshot metadata containing the repository secret id to verify
     * @throws RepositoryException if the repository secret id on the local node mismatches the master's
     */
    private void validateLocalRepositorySecret(Map<String, Object> snapshotUserMetadata) throws RepositoryException {
        if (snapshotUserMetadata == null) {
            throw new RepositoryException(metadata.name(), "Unexpected fatal internal error",
                    new IllegalStateException("Null snapshot metadata"));
        }
        final Object masterRepositoryPasswordId = snapshotUserMetadata.get(PASSWORD_ID_USER_METADATA_KEY);
        if (false == masterRepositoryPasswordId instanceof String) {
            throw new RepositoryException(metadata.name(), "Unexpected fatal internal error",
                    new IllegalStateException("Snapshot metadata does not contain the repository key id as a String"));
        }
        if (false == validatedRepositoryPasswordId.get().equals(masterRepositoryPasswordId)) {
            final Object masterRepositoryPasswordIdSalt = snapshotUserMetadata.get(PASSWORD_ID_SALT_USER_METADATA_KEY);
            if (false == masterRepositoryPasswordIdSalt instanceof String) {
                throw new RepositoryException(metadata.name(), "Unexpected fatal internal error",
                        new IllegalStateException("Snapshot metadata does not contain the repository key id as a String"));
            }
            final String computedRepositoryPasswordId;
            try {
                computedRepositoryPasswordId = AESKeyUtils.computeId(AESKeyUtils.generatePasswordBasedKey(repositoryPassword,
                        Base64.getUrlDecoder().decode(((String) masterRepositoryPasswordIdSalt).getBytes(StandardCharsets.UTF_8))));
            } catch (Exception e) {
                throw new RepositoryException(metadata.name(), "Unexpected fatal internal error", e);
            }
            if (computedRepositoryPasswordId.equals(masterRepositoryPasswordId)) {
                this.validatedRepositoryPasswordId.set(computedRepositoryPasswordId);
            } else {
                throw new RepositoryException(metadata.name(),
                        "Repository secret id mismatch. The local node's repository secret, the keystore setting [" +
                                EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(
                                        EncryptedRepositoryPlugin.PASSWORD_NAME_SETTING.get(metadata.settings())).getKey() +
                                "], is different compared to the elected master node's which started the snapshot operation");
            }
        }
    }

    private static class SingleUseDEK {
        private static final SingleUseDEK EXPIRED_DEK = new SingleUseDEK(null, null, Integer.MAX_VALUE);

        final SecretKey DEK;
        final String DEKId;
        final Integer nonce;

        private SingleUseDEK(SecretKey DEK, String DEKId, Integer nonce) {
            this.DEK = DEK;
            this.DEKId = DEKId;
            this.nonce = nonce;
        }

        static CheckedSupplier<SingleUseDEK, IOException> createSingleUseDEKSupplier(
                CheckedSupplier<Tuple<SecretKey, String>, IOException> DEKGenerator) {
            final AtomicReference<SingleUseDEK> DEKCurrentlyInUse = new AtomicReference<>(EXPIRED_DEK);
            final Object lock = new Object();
            return () -> {
                while (true) {
                    final SingleUseDEK nonceAndDEK = DEKCurrentlyInUse.getAndUpdate(prev -> prev.nonce < Integer.MAX_VALUE ?
                            new SingleUseDEK(prev.DEK, prev.DEKId, prev.nonce + 1) : EXPIRED_DEK);
                    if (nonceAndDEK.nonce < Integer.MAX_VALUE) {
                        logger.trace(() -> new ParameterizedMessage("DEK with id [{}] reused with nonce [{}]", nonceAndDEK.DEKId,
                                nonceAndDEK.nonce));
                        return nonceAndDEK;
                    } else {
                        logger.trace(() -> new ParameterizedMessage("Regenerating a new DEK to replace id [{}]", nonceAndDEK.DEKId));
                        synchronized (lock) {
                            if (DEKCurrentlyInUse.get().nonce == Integer.MAX_VALUE) {
                                final Tuple<SecretKey, String> newDEK = DEKGenerator.get();
                                logger.debug(() -> new ParameterizedMessage("A new DEK with id [{}] has been generated", newDEK.v2()));
                                if (newDEK.v2().length() != DEK_ID_LENGTH_IN_CHARS) {
                                    throw new IllegalStateException("Unexpected DEK Id length [" + newDEK.v2().length() + "]");
                                }
                                DEKCurrentlyInUse.set(new SingleUseDEK(newDEK.v1(), newDEK.v2(), Integer.MIN_VALUE));
                            }
                        }
                    }
                }
            };
        }
    }

}
