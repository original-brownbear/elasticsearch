/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Base class for responses to action requests.
 */
public abstract class ActionResponse extends TransportResponse {

    public ActionResponse() {}

    public ActionResponse(StreamInput in) throws IOException {
        super(in);
    }

    public static final class Empty extends ActionResponse implements ToXContentObject {
        public static final ActionResponse.Empty INSTANCE = new ActionResponse.Empty();

        public static <T, E extends Exception> CheckedFunction<T, ActionResponse.Empty, E> map() {
            return ignored -> INSTANCE;
        }

        public static Writeable.Reader<ActionResponse.Empty> reader() {
            return ignored -> INSTANCE;
        }

        @Override
        public String toString() {
            return "EmptyActionResponse{}";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) {
            return builder;
        }
    }
}
