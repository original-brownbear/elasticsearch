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

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

/**
 * ForceMerge index/indices action.
 */
public class TransportForceMergeActionV2 extends TransportAction<ForceMergeRequest, ForceMergeResponse> {

    private final TransportService transportService;

    @Inject
    public TransportForceMergeActionV2(TransportService transportService, ActionFilters actionFilters) {
        super(ForceMergeActionV2.NAME, actionFilters, transportService.getTaskManager());
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, ForceMergeRequest request, ActionListener<ForceMergeResponse> listener) {
        // TODO: Run this on primary
        // TODO: retention lease sync request -> call the below
        //indexServicePrimary.getShard(0).syncRetentionLeases();
        transportService.sendChildRequest(transportService.getLocalNode(), ForceMergeAction.NAME, request, task,
                TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, ForceMergeResponse::new));
    }
}
