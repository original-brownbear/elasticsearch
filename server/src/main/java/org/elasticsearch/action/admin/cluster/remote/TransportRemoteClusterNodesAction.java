/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterServerInfo;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransportRemoteClusterNodesAction extends HandledTransportAction<
    TransportRemoteClusterNodesAction.Request,
    TransportRemoteClusterNodesAction.Response> {

    public static final String ACTION_NAME = "cluster:internal/remote_cluster/nodes";
    public static final ActionType<Response> ACTION = new ActionType<>(ACTION_NAME, Response::new);
    private final TransportService transportService;

    @Inject
    public TransportRemoteClusterNodesAction(TransportService transportService, ActionFilters actionFilters) {
        super(ACTION.name(), transportService, actionFilters, Request::new);
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear();
        nodesInfoRequest.addMetrics(NodesInfoRequest.Metric.REMOTE_CLUSTER_SERVER.metricName());
        final ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        try (var ignore = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            transportService.sendRequest(
                transportService.getLocalNode(),
                TransportNodesInfoAction.ACTION.name(),
                nodesInfoRequest,
                new ActionListenerResponseHandler<>(listener.delegateFailureAndWrap((l, response) -> {
                    final List<DiscoveryNode> remoteClusterNodes = response.getNodes().stream().map(nodeInfo -> {
                        final RemoteClusterServerInfo remoteClusterServerInfo = nodeInfo.getInfo(RemoteClusterServerInfo.class);
                        if (remoteClusterServerInfo == null) {
                            return null;
                        }
                        return nodeInfo.getNode().withTransportAddress(remoteClusterServerInfo.getAddress().publishAddress());
                    }).filter(Objects::nonNull).toList();
                    l.onResponse(new Response(remoteClusterNodes));
                }), TransportNodesInfoAction.ACTION.getResponseReader())
            );
        }
    }

    public static class Request extends ActionRequest {

        public static final Request INSTANCE = new Request();

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {

        private final List<DiscoveryNode> nodes;

        public Response(List<DiscoveryNode> nodes) {
            this.nodes = nodes;
        }

        private Response(StreamInput in) throws IOException {
            super(in);
            this.nodes = in.readList(DiscoveryNode::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(nodes);
        }

        public List<DiscoveryNode> getNodes() {
            return nodes;
        }
    }
}
