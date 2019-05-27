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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.microsoft.rest.v2.RestProxy;
import com.microsoft.rest.v2.protocol.SerializerEncoding;
import com.microsoft.rest.v2.serializer.JacksonAdapter;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class AzureRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    static {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                Class.forName("org.elasticsearch.repositories.azure.AzureRepositoryPlugin");
                Class.forName("com.microsoft.rest.v2.serializer.AdditionalPropertiesSerializer");
                Class.forName("com.microsoft.rest.v2.http.HttpResponse");
                Class.forName("com.microsoft.rest.v2.protocol.HttpResponseDecoder");
                final ObjectMapper mapper = new JacksonAdapter().serializer();
                mapper.setSerializerProvider(new DefaultSerializerProvider.Impl());
                mapper.writeValue(new ByteArrayOutputStream(), Collections.emptyMap());
                RestProxy.createDefaultSerializer().serialize(Collections.emptyMap(), SerializerEncoding.JSON);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    // protected for testing
    final AzureStorageService azureStoreService;

    public AzureRepositoryPlugin(Settings settings) {
        // eagerly load client settings so that secure settings are read
        this.azureStoreService = new AzureStorageService(settings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ThreadPool threadPool) {
        return Collections.singletonMap(AzureRepository.TYPE,
                (metadata) -> new AzureRepository(metadata, env, namedXContentRegistry, azureStoreService, threadPool));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            AzureStorageSettings.ACCOUNT_SETTING,
            AzureStorageSettings.KEY_SETTING,
            AzureStorageSettings.ENDPOINT_SUFFIX_SETTING,
            AzureStorageSettings.ENDPOINT_OVERRIDE_SETTING,
            AzureStorageSettings.TIMEOUT_SETTING,
            AzureStorageSettings.MAX_RETRIES_SETTING,
            AzureStorageSettings.PROXY_TYPE_SETTING,
            AzureStorageSettings.PROXY_HOST_SETTING,
            AzureStorageSettings.PROXY_PORT_SETTING
        );
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        if (clientsSettings.isEmpty()) {
            throw new SettingsException("If you want to use an azure repository, you need to define a client configuration.");
        }
        azureStoreService.refreshAndClearCache(clientsSettings);
    }
}
