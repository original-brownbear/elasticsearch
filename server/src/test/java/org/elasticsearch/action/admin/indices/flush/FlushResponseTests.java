/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.test.AbstractBroadcastResponseTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Arrays;
import java.util.List;

public class FlushResponseTests extends AbstractBroadcastResponseTestCase<FlushResponse> {

    private static final ConstructingObjectParser<FlushResponse, Void> PARSER = new ConstructingObjectParser<>("flush", true, arg -> {
        BaseBroadcastResponse response = (BaseBroadcastResponse) arg[0];
        return new FlushResponse(
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getFailedShards(),
            Arrays.asList(response.getShardFailures())
        );
    });

    static {
        BaseBroadcastResponse.declareBroadcastFields(PARSER);
    }

    @Override
    protected FlushResponse createTestInstance(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> failures
    ) {
        return new FlushResponse(totalShards, successfulShards, failedShards, failures);
    }

    @Override
    protected FlushResponse doParseInstance(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
