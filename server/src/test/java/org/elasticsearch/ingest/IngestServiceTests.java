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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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

    public void testUpdatePipelines() {
        IngestService ingestService = createWithProcessors();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.pipelines().size(), is(0));

        PipelineConfiguration pipeline = new PipelineConfiguration(
            "_id",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", pipeline));
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.pipelines().size(), is(1));
        assertThat(ingestService.pipelines().get("_id").getId(), equalTo("_id"));
        assertThat(ingestService.pipelines().get("_id").getDescription(), nullValue());
        assertThat(ingestService.pipelines().get("_id").getProcessors().size(), equalTo(1));
        assertThat(ingestService.pipelines().get("_id").getProcessors().get(0).getType(), equalTo("set"));
    }

    public void testDelete() {
        IngestService ingestService = createWithProcessors();
        PipelineConfiguration config = new PipelineConfiguration(
            "_id",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", config));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), notNullValue());

        // Delete pipeline:
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("_id");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), nullValue());

        // Delete existing pipeline:
        try {
            IngestService.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [_id] is missing"));
        }
    }

    public void testValidateNoIngestInfo() throws Exception {
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON);
        Exception e = expectThrows(IllegalStateException.class, () -> ingestService.validatePipeline(emptyMap(), putRequest));
        assertEquals("Ingest info is empty", e.getMessage());

        DiscoveryNode discoveryNode = new DiscoveryNode("_node_id", buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT);
        IngestInfo ingestInfo = new IngestInfo(Collections.singletonList(new ProcessorInfo("set")));
        ingestService.validatePipeline(Collections.singletonMap(discoveryNode, ingestInfo), putRequest);
    }

    public void testCrud() throws Exception {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest(id,
            new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("set"));

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest(id);
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
    }

    public void testPut() {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        // add a new pipeline:
        PutPipelineRequest putRequest = new PutPipelineRequest(id, new BytesArray("{\"processors\": []}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(0));

        // overwrite existing pipeline:
        putRequest =
            new PutPipelineRequest(id, new BytesArray("{\"processors\": [], \"description\": \"_description\"}"), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(0));
    }

    public void testPutWithErrorResponse() {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        PutPipelineRequest putRequest =
            new PutPipelineRequest(id, new BytesArray("{\"description\": \"empty processors\"}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = PipelineStore.innerPut(putRequest, clusterState);
        try {
            ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
            fail("should fail");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[processors] required property is missing"));
        }
        pipeline = ingestService.getPipeline(id);
        assertNotNull(pipeline);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("this is a place holder pipeline, because pipeline with" +
            " id [_id] could not be loaded"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertNull(pipeline.getProcessors().get(0).getTag());
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("unknown"));
    }

    public void testDeleteUsingWildcard() {
        IngestService ingestService = createWithProcessors();
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"
        );
        pipelines.put("p1", new PipelineConfiguration("p1", definition, XContentType.JSON));
        pipelines.put("p2", new PipelineConfiguration("p2", definition, XContentType.JSON));
        pipelines.put("q1", new PipelineConfiguration("q1", definition, XContentType.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), notNullValue());
        assertThat(ingestService.getPipeline("p2"), notNullValue());
        assertThat(ingestService.getPipeline("q1"), notNullValue());

        // Delete pipeline matching wildcard
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("p*");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), nullValue());
        assertThat(ingestService.getPipeline("p2"), nullValue());
        assertThat(ingestService.getPipeline("q1"), notNullValue());

        // Exception if we used name which does not exist
        try {
            IngestService.innerDelete(new DeletePipelineRequest("unknown"), clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [unknown] is missing"));
        }

        // match all wildcard works on last remaining pipeline
        DeletePipelineRequest matchAllDeleteRequest = new DeletePipelineRequest("*");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(matchAllDeleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), nullValue());
        assertThat(ingestService.getPipeline("p2"), nullValue());
        assertThat(ingestService.getPipeline("q1"), nullValue());

        // match all wildcard does not throw exception if none match
        IngestService.innerDelete(matchAllDeleteRequest, clusterState);
    }

    public void testDeleteWithExistingUnmatchedPipelines() {
        IngestService ingestService = createWithProcessors();
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"
        );
        pipelines.put("p1", new PipelineConfiguration("p1", definition, XContentType.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), notNullValue());

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("z*");
        try {
            IngestService.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [z*] is missing"));
        }
    }

    public void testGetPipelines() {
        Map<String, PipelineConfiguration> configs = new HashMap<>();
        configs.put("_id1", new PipelineConfiguration(
            "_id1", new BytesArray("{\"processors\": []}"), XContentType.JSON
        ));
        configs.put("_id2", new PipelineConfiguration(
            "_id2", new BytesArray("{\"processors\": []}"), XContentType.JSON
        ));

        assertThat(IngestService.innerGetPipelines(null, "_id1").isEmpty(), is(true));

        IngestMetadata ingestMetadata = new IngestMetadata(configs);
        List<PipelineConfiguration> pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id1");
        assertThat(pipelines.size(), equalTo(1));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id1", "_id2");
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id*");
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        // get all variants: (no IDs or '*')
        pipelines = IngestService.innerGetPipelines(ingestMetadata);
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "*");
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));
    }

    public void testValidate() throws Exception {
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\", \"tag\": \"tag1\"}}," +
                "{\"remove\" : {\"field\": \"_field\", \"tag\": \"tag2\"}}]}"),
            XContentType.JSON);

        DiscoveryNode node1 = new DiscoveryNode("_node_id1", buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("_node_id2", buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT);
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"))));

        ElasticsearchParseException e =
            expectThrows(ElasticsearchParseException.class, () -> ingestService.validatePipeline(ingestInfos, putRequest));
        assertEquals("Processor type [remove] is not installed on node [" + node2 + "]", e.getMessage());
        assertEquals("remove", e.getHeader("processor_type").get(0));
        assertEquals("tag2", e.getHeader("processor_tag").get(0));

        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestService.validatePipeline(ingestInfos, putRequest);
    }

    private static IngestService createWithProcessors() {
        ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);
        return new IngestService(mock(ClusterService.class), threadPool, null, null,
            null, Collections.singletonList(new IngestPlugin() {
            @Override
            public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
                Map<String, Processor.Factory> processors = new HashMap<>();
                processors.put("set", (factories, tag, config) -> {
                    String field = (String) config.remove("field");
                    String value = (String) config.remove("value");
                    return new Processor() {
                        @Override
                        public void execute(IngestDocument ingestDocument) {
                            ingestDocument.setFieldValue(field, value);
                        }

                        @Override
                        public String getType() {
                            return "set";
                        }

                        @Override
                        public String getTag() {
                            return tag;
                        }
                    };
                });
                processors.put("remove", (factories, tag, config) -> {
                    String field = (String) config.remove("field");
                    return new Processor() {
                        @Override
                        public void execute(IngestDocument ingestDocument) {
                            ingestDocument.removeField(field);
                        }

                        @Override
                        public String getType() {
                            return "remove";
                        }

                        @Override
                        public String getTag() {
                            return tag;
                        }
                    };
                });
                return processors;
            }
        }));
    }
}
