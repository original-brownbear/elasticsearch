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

import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.ListContainersOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ReliableDownloadOptions;
import com.microsoft.azure.storage.blob.RequestRetryOptions;
import com.microsoft.azure.storage.blob.RetryPolicyType;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.models.BlobDeleteResponse;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ListBlobsHierarchySegmentResponse;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpRequest;
import com.microsoft.rest.v2.http.HttpResponse;
import io.reactivex.Flowable;
import io.reactivex.Single;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class AzureStorageService {
    
    private static final Logger logger = LogManager.getLogger(AzureStorageService.class);

    public static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
    /**
     * TODO: Docs
     */
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(256, ByteSizeUnit.MB);

    // 'package' for testing
    volatile Map<String, AzureStorageSettings> storageSettings = emptyMap();

    public AzureStorageService(Settings settings) {
        // eagerly load client settings so that secure settings are read
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        refreshAndClearCache(clientsSettings);
    }

    /**
     * Creates a {@code CloudBlobClient} on each invocation using the current client
     * settings. CloudBlobClient is not thread safe and the settings can change,
     * therefore the instance is not cache-able and should only be reused inside a
     * thread for logically coupled ops. The {@code OperationContext} is used to
     * specify the proxy, but a new context is *required* for each call.
     */
    public ServiceURL client(String clientName) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }
        try {
            return buildClient(azureStorageSettings);
        } catch (IllegalArgumentException e) {
            throw new SettingsException("Invalid azure client settings with name [" + clientName + "]", e);
        }
    }

    private static ServiceURL buildClient(AzureStorageSettings azureStorageSettings) {
        final ServiceURL client = createClient(azureStorageSettings);
        // Set timeout option if the user sets cloud.azure.storage.timeout or
        // cloud.azure.storage.xxx.timeout (it's negative by default)
        final long timeout = azureStorageSettings.getTimeout().getMillis();
        if (timeout > 0) {
            if (timeout > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Timeout [" + azureStorageSettings.getTimeout() + "] exceeds 2,147,483,647ms.");
            }
            // TODO: Configure timeouts
        }
        // We define a default exponential retry policy
        // TODO: Configure retries
        return client;
    }

    private static ServiceURL createClient(AzureStorageSettings azureStorageSettings) {
        return SocketAccess.doPrivilegedException(() -> {
            final HttpClient client = HttpClient.createDefault();
            final PipelineOptions options = new PipelineOptions().withClient(new HttpClient() {
                @Override
                public Single<HttpResponse> sendRequestAsync(HttpRequest request) {
                    return client.sendRequestAsync(new HttpRequest(request.callerMethod(), request.httpMethod(),
                        request.url(), request.responseDecoder()));
                }
            });
            final SharedKeyCredentials creds = new SharedKeyCredentials(azureStorageSettings.getAccount(), azureStorageSettings.getKey());
            final String endpointOverride = azureStorageSettings.endpointOverride();
            options.withRequestRetryOptions(
                new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL, 3, 10, 500L, 1000L,
                    Strings.hasText(endpointOverride) ? endpointOverride :
                        azureStorageSettings.getAccount() + "-secondary.blob." + azureStorageSettings.getEndpointSuffix()));
            return new ServiceURL(
                new URL(Strings.hasText(endpointOverride) ? endpointOverride :
                    "https://" + azureStorageSettings.getAccount() + ".blob." + azureStorageSettings.getEndpointSuffix()),
                StorageURL.createPipeline(creds, options));
        });
    }

    /**
     * Updates settings for building clients. Any client cache is cleared. Future
     * client requests will use the new refreshed settings.
     *
     * @param clientsSettings the settings for new clients
     * @return the old settings
     */
    public Map<String, AzureStorageSettings> refreshAndClearCache(Map<String, AzureStorageSettings> clientsSettings) {
        final Map<String, AzureStorageSettings> prevSettings = this.storageSettings;
        this.storageSettings = Map.copyOf(clientsSettings);
        // clients are built lazily by {@link client(String)}
        return prevSettings;
    }

    public boolean doesContainerExist(String account, String container) {
        return SocketAccess.doPrivilegedException(
            () -> client(account).listContainersSegment(null, new ListContainersOptions().withPrefix(container))
                .blockingGet().body().containerItems().stream().anyMatch(c -> container.equals(c.name())));
    }

    /**
     * Extract the blob name from a URI like https://myservice.azure.net/container/path/to/myfile
     * It should remove the container part (first part of the path) and gives path/to/myfile
     * @param uri URI to parse
     * @return The blob name relative to the container
     */
    static String blobNameFromUri(URI uri) {
        final String path = uri.getPath();
        // We remove the container name from the path
        // The 3 magic number cames from the fact if path is /container/path/to/myfile
        // First occurrence is empty "/"
        // Second occurrence is "container
        // Last part contains "path/to/myfile" which is what we want to get
        final String[] splits = path.split("/", 3);
        // We return the remaining end of the string
        return splits[2];
    }

    public boolean blobExists(String account, String container, String blob) {
        // Container name must be lower case.
        return SocketAccess.doPrivilegedException(() ->
            client(account).createContainerURL(container).listBlobsFlatSegment(null, new ListBlobsOptions()
                .withPrefix(blob)).blockingGet().body().segment().blobItems().stream().anyMatch(b -> b.name().equals(blob)));
    }

    public void deleteBlob(String account, String container, String blob) throws IOException {
        final BlobDeleteResponse response = SocketAccess.doPrivilegedException(
            () -> client(account).createContainerURL(container).createBlobURL(blob).delete().blockingGet());
        assert response != null;
    }

    public InputStream getInputStream(String account, String container, String blob) throws IOException {
        return SocketAccess.doPrivilegedException(() -> {
            final Iterator<ByteBuffer> buffers =
                client(account).createContainerURL(container).createBlobURL(blob)
                    .download().blockingGet().body(new ReliableDownloadOptions()).blockingIterable().iterator();
            return new InputStream() {

                private ByteBuffer current = buffers.hasNext() ? buffers.next() : ByteBuffer.allocate(0);

                @Override
                public int read() {
                    if (current.hasRemaining()) {
                        return current.get() & 0xFF;
                    } else if (buffers.hasNext()) {
                        current = buffers.next();
                        assert current.hasRemaining();
                        return current.get();
                    }
                    return -1;
                }
            };
        });
    }

    public Map<String, BlobMetaData> listBlobsByPrefix(String account, String container, String keyPath, String prefix) {
        return SocketAccess.doPrivilegedException(() -> {
            final Map<String, BlobMetaData> blobsBuilder = new HashMap<>();
            ListBlobsHierarchySegmentResponse response = client(account).createContainerURL(container)
                .listBlobsHierarchySegment(null, "/", new ListBlobsOptions().withPrefix(prefix)).blockingGet().body();
            for (BlobItem blobItem : response.segment().blobItems()) {
                final long length = blobItem.properties().contentLength();
                final String name = blobItem.name();
                blobsBuilder.put(name, new PlainBlobMetaData(name, length));
            }
            while (response.nextMarker() != null) {
                response = client(account).createContainerURL(container)
                    .listBlobsHierarchySegment(response.nextMarker(), "/",
                        new ListBlobsOptions().withPrefix(prefix)).blockingGet().body();
                for (BlobItem blobItem : response.segment().blobItems()) {
                    final long length = blobItem.properties().contentLength();
                    final String name = blobItem.name();
                    blobsBuilder.put(name, new PlainBlobMetaData(name, length));
                }
            }
            return Map.copyOf(blobsBuilder);
        });
    }

    public void writeBlob(String account, String container, String blobName, InputStream inputStream, long blobSize,
                          boolean failIfAlreadyExists) throws IOException {
        final int bufferSize = 2 << 15;
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        SocketAccess.doPrivilegedException(() -> {
            long left = blobSize;
            while (left > 0L) {
                final int read = inputStream.read(buffer.array());
                buffer.position(0).limit(read);
                if (read > 0) {
                    left -= read;
                    client(account).createContainerURL(container).createBlockBlobURL(blobName).upload(Flowable.just(buffer), blobSize)
                        .blockingGet();
                } else {
                    break;
                }
            }
            return null;
        });
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    static InputStream giveSocketPermissionsToStream(final InputStream stream) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                return SocketAccess.doPrivilegedIOException(stream::read);
            }

            @Override
            public int read(byte[] b) throws IOException {
                return SocketAccess.doPrivilegedIOException(() -> stream.read(b));
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return SocketAccess.doPrivilegedIOException(() -> stream.read(b, off, len));
            }
        };
    }
}
