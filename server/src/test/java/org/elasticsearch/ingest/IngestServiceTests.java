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

package org.elasticsearch.ingest;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestServiceTests extends ESTestCase {
    private final IngestPlugin DUMMY_PLUGIN = new IngestPlugin() {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Collections.singletonMap("foo", (factories, tag, config) -> null);
        }
    };

    public void testIngestPlugin() {
        ThreadPool tp = mock(ThreadPool.class);
        IngestService ingestService = new IngestService(mock(ClusterService.class), tp, null, null,
            null, Collections.singletonList(DUMMY_PLUGIN));
        Map<String, Processor.Factory> factories = ingestService.getProcessorFactories();
        assertTrue(factories.containsKey("foo"));
        assertEquals(1, factories.size());
    }

    public void testIngestPluginDuplicate() {
        ThreadPool tp = mock(ThreadPool.class);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new IngestService(mock(ClusterService.class), tp, null, null,
            null, Arrays.asList(DUMMY_PLUGIN, DUMMY_PLUGIN)));
        assertTrue(e.getMessage(), e.getMessage().contains("already registered"));
    }

    public void testExecuteIndexPipelineDoesNotExist() {
        ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);
        IngestService ingestService = new IngestService(mock(ClusterService.class), threadPool, null, null,
            null, Collections.singletonList(DUMMY_PLUGIN));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("_id");

        final SetOnce<Boolean> failure = new SetOnce<>();
        final BiConsumer<IndexRequest, Exception> failureHandler = (request, e) -> {
            failure.set(true);
            assertThat(request, sameInstance(indexRequest));
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("pipeline with id [_id] does not exist"));
        };

        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);

        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(null);
    }
}
