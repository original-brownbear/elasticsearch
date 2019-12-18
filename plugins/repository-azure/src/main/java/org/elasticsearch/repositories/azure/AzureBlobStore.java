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

import com.microsoft.azure.storage.LocationMode;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static java.util.Collections.emptyMap;

public class AzureBlobStore implements BlobStore {

    private final AzureStorageService service;
    private final ThreadPool threadPool;

    final String clientName;
    final String container;
    private final LocationMode locationMode;

    public AzureBlobStore(RepositoryMetaData metadata, AzureStorageService service, ThreadPool threadPool) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());
        this.service = service;
        this.threadPool = threadPool;
        // locationMode is set per repository, not per client
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        final Map<String, AzureStorageSettings> prevSettings = this.service.refreshAndClearCache(emptyMap());
        final Map<String, AzureStorageSettings> newSettings = AzureStorageSettings.overrideLocationMode(prevSettings, this.locationMode);
        this.service.refreshAndClearCache(newSettings);
    }

    @Override
    public String toString() {
        return container;
    }

    public AzureStorageService getService() {
        return service;
    }

    /**
     * Returns {@code true} iff {@link LocationMode} for the Azure storage requests is {@link LocationMode#SECONDARY_ONLY}.
     */
    public boolean locationModeSecondaryOnly() {
        return locationMode == LocationMode.SECONDARY_ONLY;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(path, this, threadPool);
    }

    @Override
    public void close() {
    }
}
