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

package org.elasticsearch.repositories.azure;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AzureBlobContainer extends AbstractBlobContainer {

    private final Logger logger = LogManager.getLogger(AzureBlobContainer.class);
    private final AzureBlobStore blobStore;
    private final ThreadPool threadPool;
    private final String keyPath;

    AzureBlobContainer(BlobPath path, AzureBlobStore blobStore, ThreadPool threadPool) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
        this.threadPool = threadPool;
    }

    public boolean blobExists(String blobName) {
        logger.trace("blobExists({})", blobName);
        try {
            final Tuple<CloudBlobClient, AzureStorageSettings> client = getClient();
            final CloudBlobContainer blobContainer = client.v1().getContainerReference(blobStore.container);
            return SocketAccess.doPrivilegedException(() -> {
                final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(buildKey(blobName));
                return azureBlob.exists(null, null, buildOperationContext(client.v2()));
            });
        } catch (URISyntaxException | StorageException e) {
            logger.warn("can not access [{}] in container {{}}: {}", blobName, blobStore, e.getMessage());
        }
        return false;
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        logger.trace("readBlob({})", blobName);

        if (blobStore.locationModeSecondaryOnly() && !blobExists(blobName)) {
            // On Azure, if the location path is a secondary location, and the blob does not
            // exist, instead of returning immediately from the getInputStream call below
            // with a 404 StorageException, Azure keeps trying and trying for a long timeout
            // before throwing a storage exception.  This can cause long delays in retrieving
            // snapshots, so we first check if the blob exists before trying to open an input
            // stream to it.
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }

        try {
            final Tuple<CloudBlobClient, AzureStorageSettings> client = getClient();
            final String blob = buildKey(blobName);
            final CloudBlockBlob blockBlobReference = client.v1().getContainerReference(blobStore.container).getBlockBlobReference(blob);
            logger.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", blobStore.container, blob));
            final BlobInputStream is = SocketAccess.doPrivilegedException(() ->
                blockBlobReference.openInputStream(null, null, buildOperationContext(client.v2())));
            return AzureStorageService.giveSocketPermissionsToStream(is);
        } catch (StorageException e) {
            if (e.getHttpStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                throw new NoSuchFileException(e.getMessage());
            }
            throw new IOException(e);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        final String blobKey = buildKey(blobName);
        logger.trace("writeBlob({}, stream, {})", blobKey, blobSize);
        try {
            assert inputStream.markSupported()
                : "Should not be used with non-mark supporting streams as their retry handling in the SDK is broken";
            logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobKey, blobSize));
            final Tuple<CloudBlobClient, AzureStorageSettings> client = getClient();
            final CloudBlobContainer blobContainer = client.v1().getContainerReference(blobStore.container);
            final CloudBlockBlob blob = blobContainer.getBlockBlobReference(blobKey);
            try {
                final AccessCondition accessCondition =
                    failIfAlreadyExists ? AccessCondition.generateIfNotExistsCondition() : AccessCondition.generateEmptyCondition();
                SocketAccess.doPrivilegedVoidException(() ->
                    blob.upload(inputStream, blobSize, accessCondition, blobStore.getService().getBlobRequestOptionsForWriteBlob(),
                        buildOperationContext(client.v2())));
            } catch (final StorageException se) {
                if (failIfAlreadyExists && se.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
                    StorageErrorCodeStrings.BLOB_ALREADY_EXISTS.equals(se.getErrorCode())) {
                    throw new FileAlreadyExistsException(blobKey, null, se.getMessage());
                }
                throw se;
            }
            logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobKey, blobSize));
        } catch (URISyntaxException | StorageException e) {
            throw new IOException("Can not write blob " + blobName, e);
        }
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        try {
            final CloudBlobContainer blobContainer = getClient().v1().getContainerReference(blobStore.container);
            final Collection<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
            final AtomicLong outstanding = new AtomicLong(1L);
            final PlainActionFuture<Void> result = PlainActionFuture.newFuture();
            final AtomicLong blobsDeleted = new AtomicLong();
            final AtomicLong bytesDeleted = new AtomicLong();
            SocketAccess.doPrivilegedVoidException(() -> {
                for (final ListBlobItem blobItem : blobContainer.listBlobs(keyPath, true)) {
                    // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                    // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
                    final String blobPath = blobItem.getUri().getPath().substring(1 + blobStore.container.length() + 1);
                    outstanding.incrementAndGet();
                    threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() throws Exception {
                            final long len;
                            if (blobItem instanceof CloudBlob) {
                                len = ((CloudBlob) blobItem).getProperties().getLength();
                            } else {
                                len = -1L;
                            }
                            deleteBlob(blobPath);
                            blobsDeleted.incrementAndGet();
                            if (len >= 0) {
                                bytesDeleted.addAndGet(len);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            exceptions.add(e);
                        }

                        @Override
                        public void onAfter() {
                            if (outstanding.decrementAndGet() == 0) {
                                result.onResponse(null);
                            }
                        }
                    });
                }
            });
            if (outstanding.decrementAndGet() == 0) {
                result.onResponse(null);
            }
            result.actionGet();
            if (exceptions.isEmpty() == false) {
                final IOException ex = new IOException("Deleting directory [" + keyPath + "] failed");
                exceptions.forEach(ex::addSuppressed);
                throw ex;
            }
            return new DeleteResult(blobsDeleted.get(), bytesDeleted.get());
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        final PlainActionFuture<Void> result = PlainActionFuture.newFuture();
        if (blobNames.isEmpty()) {
            result.onResponse(null);
        } else {
            final GroupedActionListener<Void> listener =
                new GroupedActionListener<>(ActionListener.map(result, v -> null), blobNames.size());
            final ExecutorService executor = threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME);
            // Executing deletes in parallel since Azure SDK 8 is using blocking IO while Azure does not provide a bulk delete API endpoint
            // TODO: Upgrade to newer non-blocking Azure SDK 11 and execute delete requests in parallel that way.
            for (String blobName : blobNames) {
                executor.execute(ActionRunnable.run(listener, () -> {
                    logger.trace("deleteBlob({})", blobName);
                    try {
                        deleteBlob(buildKey(blobName));
                    } catch (StorageException e) {
                        if (e.getHttpStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                            throw new IOException(e);
                        }
                    } catch (URISyntaxException e) {
                        throw new IOException(e);
                    }
                }));
            }
        }
        try {
            result.actionGet();
        } catch (Exception e) {
            throw new IOException("Exception during bulk delete", e);
        }
    }

    private void deleteBlob(String blob) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, AzureStorageSettings> client = getClient();
        // Container name must be lower case.
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(blobStore.container);
        logger.trace(() -> new ParameterizedMessage("delete blob for container [{}], blob [{}]", blobStore.container, blob));
        SocketAccess.doPrivilegedVoidException(() -> {
            final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            logger.trace(() -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", blobStore.container, blob));
            azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, buildOperationContext(client.v2()));
        });
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable String prefix) throws IOException {
        logger.trace("listBlobsByPrefix({})", prefix);

        try {
            // NOTE: this should be here: if (prefix == null) prefix = "";
            // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
            // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!
            final var blobsBuilder = new HashMap<String, BlobMetaData>();
            final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);
            final Tuple<CloudBlobClient, AzureStorageSettings> client = getClient();
            final CloudBlobContainer blobContainer = client.v1().getContainerReference(blobStore.container);
            logger.trace(() -> new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]",
                blobStore.container, keyPath, prefix));
            SocketAccess.doPrivilegedVoidException(() -> {
                for (final ListBlobItem blobItem : blobContainer.listBlobs(keyPath + (prefix == null ? "" : prefix), false,
                    enumBlobListingDetails, null, buildOperationContext(client.v2()))) {
                    final URI uri = blobItem.getUri();
                    logger.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
                    // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                    // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
                    final String blobPath = uri.getPath().substring(1 + blobStore.container.length() + 1);
                    if (blobItem instanceof CloudBlob) {
                        final BlobProperties properties = ((CloudBlob) blobItem).getProperties();
                        final String name = blobPath.substring(keyPath.length());
                        logger.trace(() -> new ParameterizedMessage("blob url [{}], name [{}], size [{}]", uri, name,
                            properties.getLength()));
                        blobsBuilder.put(name, new PlainBlobMetaData(name, properties.getLength()));
                    }
                }
            });
            return Map.copyOf(blobsBuilder);
        } catch (URISyntaxException | StorageException e) {
            logger.warn("can not access [{}] in container {{}}: {}", prefix, blobStore, e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        logger.trace("listBlobs()");
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        final BlobPath path = path();
        try {
            final var blobsBuilder = new HashSet<String>();
            final Tuple<CloudBlobClient, AzureStorageSettings> client = getClient();
            final CloudBlobContainer blobContainer = client.v1().getContainerReference(blobStore.container);
            final String keyPath = path.buildAsString();
            final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);

            SocketAccess.doPrivilegedVoidException(() -> {
                for (ListBlobItem blobItem :
                    blobContainer.listBlobs(keyPath, false, enumBlobListingDetails, null, buildOperationContext(client.v2()))) {
                    if (blobItem instanceof CloudBlobDirectory) {
                        final URI uri = blobItem.getUri();
                        logger.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
                        // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                        // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /.
                        // Lastly, we add the length of keyPath to the offset to strip this container's path.
                        final String uriPath = uri.getPath();
                        blobsBuilder.add(uriPath.substring(1 + blobStore.container.length() + 1 + keyPath.length(), uriPath.length() - 1));
                    }
                }
            });
            return Collections.unmodifiableMap(blobsBuilder.stream()
                .collect(Collectors.toMap(Function.identity(), name -> new AzureBlobContainer(path.add(name), blobStore, threadPool))));
        } catch (URISyntaxException | StorageException e) {
            throw new IOException("Failed to list children in path [" + path.buildAsString() + "].", e);
        }
    }

    private static OperationContext buildOperationContext(AzureStorageSettings azureStorageSettings) {
        final OperationContext context = new OperationContext();
        context.setProxy(azureStorageSettings.getProxy());
        return context;
    }

    private Tuple<CloudBlobClient, AzureStorageSettings> getClient() {
        return blobStore.getService().client(blobStore.clientName);
    }

    protected String buildKey(String blobName) {
        return keyPath + (blobName == null ? "" : blobName);
    }
}
