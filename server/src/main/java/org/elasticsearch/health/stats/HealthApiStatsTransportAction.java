/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.health.stats;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Performs the health api stats operation.
 */
public class HealthApiStatsTransportAction extends TransportNodesAction<
    HealthApiStatsTransportAction.Request,
    HealthApiStatsTransportAction.Response,
    HealthApiStatsTransportAction.Request.Node,
    HealthApiStatsTransportAction.Response.Node> {

    public static final ActionType<Response> ACTION = new ActionType<>("cluster:monitor/health_api/stats", Response::new);
    private final HealthApiStats healthApiStats;

    @Inject
    public HealthApiStatsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        HealthApiStats healthApiStats
    ) {
        super(
            ACTION.name(),
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            Request.Node::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.healthApiStats = healthApiStats;
    }

    @Override
    protected Response newResponse(Request request, List<Response.Node> nodes, List<FailedNodeException> failures) {
        return new Response(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected Request.Node newNodeRequest(Request request) {
        return new Request.Node(request);
    }

    @Override
    protected Response.Node newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new Response.Node(in);
    }

    @Override
    protected Response.Node nodeOperation(Request.Node request, Task task) {
        Response.Node statsResponse = new Response.Node(clusterService.localNode());
        if (healthApiStats.hasCounters()) {
            statsResponse.setStats(healthApiStats.getStats());
        }
        return statsResponse;
    }

    public static class Request extends BaseNodesRequest<Request> {

        public Request() {
            super((String[]) null);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public String toString() {
            return "health_api_stats";
        }

        public static class Node extends TransportRequest {

            public Node(StreamInput in) throws IOException {
                super(in);
            }

            public Node(Request ignored) {}

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }
    }

    public static class Response extends BaseNodesResponse<Response.Node> {

        private Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<Node> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        protected List<Node> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(Node::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<Node> nodes) throws IOException {
            out.writeList(nodes);
        }

        public Counters getStats() {
            List<Counters> counters = getNodes().stream().map(Node::getStats).filter(Objects::nonNull).toList();
            return Counters.merge(counters);
        }

        public static class Node extends BaseNodeResponse {
            @Nullable
            private Counters stats;

            Node(StreamInput in) throws IOException {
                super(in);
                stats = in.readOptionalWriteable(Counters::new);
            }

            Node(DiscoveryNode node) {
                super(node);
            }

            public Counters getStats() {
                return stats;
            }

            public void setStats(Counters stats) {
                this.stats = stats;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeOptionalWriteable(stats);
            }
        }
    }
}
