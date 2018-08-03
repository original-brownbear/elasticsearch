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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.WritePipelineResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.gateway.GatewayService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipelineStore {

    private final Map<String, Processor.Factory> processorFactories;

    // Ideally this should be in IngestMetadata class, but we don't have the processor factories around there.
    // We know of all the processor factories when a node with all its plugin have been initialized. Also some
    // processor factories rely on other node services. Custom metadata is statically registered when classes
    // are loaded, so in the cluster state we just save the pipeline config and here we keep the actual pipelines around.
    volatile Map<String, Pipeline> pipelines = new HashMap<>();

    public PipelineStore(Map<String, Processor.Factory> processorFactories) {
        this.processorFactories = processorFactories;
    }

    void innerUpdatePipelines(ClusterState previousState, ClusterState state) {
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        IngestMetadata previousIngestMetadata = previousState.getMetaData().custom(IngestMetadata.TYPE);
        if (Objects.equals(ingestMetadata, previousIngestMetadata)) {
            return;
        }

        Map<String, Pipeline> pipelines = new HashMap<>();
        List<ElasticsearchParseException> exceptions = new ArrayList<>();
        for (PipelineConfiguration pipeline : ingestMetadata.getPipelines().values()) {
            try {
                pipelines.put(pipeline.getId(), Pipeline.create(pipeline.getId(), pipeline.getConfigAsMap(), processorFactories));
            } catch (ElasticsearchParseException e) {
                pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), e));
                exceptions.add(e);
            } catch (Exception e) {
                ElasticsearchParseException parseException = new ElasticsearchParseException(
                    "Error updating pipeline with id [" + pipeline.getId() + "]", e);
                pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), parseException));
                exceptions.add(parseException);
            }
        }
        this.pipelines = Collections.unmodifiableMap(pipelines);
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    private static Pipeline substitutePipeline(String id, ElasticsearchParseException e) {
        String tag = e.getHeaderKeys().contains("processor_tag") ? e.getHeader("processor_tag").get(0) : null;
        String type = e.getHeaderKeys().contains("processor_type") ? e.getHeader("processor_type").get(0) : "unknown";
        String errorMessage = "pipeline with id [" + id + "] could not be loaded, caused by [" + e.getDetailedMessage() + "]";
        Processor failureProcessor = new AbstractProcessor(tag) {
            @Override
            public void execute(IngestDocument ingestDocument) {
                throw new IllegalStateException(errorMessage);
            }

            @Override
            public String getType() {
                return type;
            }
        };
        String description = "this is a place holder pipeline, because pipeline with id [" +  id + "] could not be loaded";
        return new Pipeline(id, description, null, new CompoundProcessor(failureProcessor));
    }

    /**
     * Stores the specified pipeline definition in the request.
     */
    public void put(ClusterService clusterService, Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request,
                    ActionListener<WritePipelineResponse> listener) throws Exception {
        // validates the pipeline and processor configuration before submitting a cluster update task:
        validatePipeline(ingestInfos, request);
        clusterService.submitStateUpdateTask("put-pipeline-" + request.getId(),
                new AckedClusterStateUpdateTask<WritePipelineResponse>(request, listener) {

            @Override
            protected WritePipelineResponse newResponse(boolean acknowledged) {
                return new WritePipelineResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return innerPut(request, currentState);
            }
        });
    }

    void validatePipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request) throws Exception {
        if (ingestInfos.isEmpty()) {
            throw new IllegalStateException("Ingest info is empty");
        }

        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        Pipeline pipeline = Pipeline.create(request.getId(), pipelineConfig, processorFactories);
        List<Exception> exceptions = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                if (entry.getValue().containsProcessor(processor.getType()) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message));
                }
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    static ClusterState innerPut(PutPipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        Map<String, PipelineConfiguration> pipelines;
        if (currentIngestMetadata != null) {
            pipelines = new HashMap<>(currentIngestMetadata.getPipelines());
        } else {
            pipelines = new HashMap<>();
        }

        pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), request.getSource(), request.getXContentType()));
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
            .putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelines))
            .build());
        return newState.build();
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline get(String id) {
        return pipelines.get(id);
    }

    public Map<String, Processor.Factory> getProcessorFactories() {
        return processorFactories;
    }
}
