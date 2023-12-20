/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Iterator;

public class MultiSearchTemplateResponse extends ActionResponse implements Iterable<MultiSearchTemplateResponse.Item>, ChunkedToXContent {

    /**
     * A search template response item, holding the actual search template response, or an error message if it failed.
     */
    public static class Item implements Writeable {
        private final SearchTemplateResponse response;
        private final Exception exception;

        private Item(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                this.response = new SearchTemplateResponse(in);
                this.exception = null;
            } else {
                exception = in.readException();
                this.response = null;
            }
        }

        public Item(SearchTemplateResponse response, Exception exception) {
            this.response = response;
            this.exception = exception;
        }

        /**
         * Is it a failed search?
         */
        public boolean isFailure() {
            return exception != null;
        }

        /**
         * The actual failure message, null if its not a failure.
         */
        @Nullable
        public String getFailureMessage() {
            return exception == null ? null : exception.getMessage();
        }

        /**
         * The actual search response, null if its a failure.
         */
        @Nullable
        public SearchTemplateResponse getResponse() {
            return this.response;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (response != null) {
                out.writeBoolean(true);
                response.writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeException(exception);
            }
        }

        public Exception getFailure() {
            return exception;
        }

        @Override
        public String toString() {
            return "Item [response=" + response + ", exception=" + exception + "]";
        }
    }

    private final Item[] items;
    private final long tookInMillis;

    MultiSearchTemplateResponse(StreamInput in) throws IOException {
        super(in);
        items = in.readArray(Item::new, Item[]::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_0_0)) {
            tookInMillis = in.readVLong();
        } else {
            tookInMillis = -1L;
        }
    }

    MultiSearchTemplateResponse(Item[] items, long tookInMillis) {
        this.items = items;
        this.tookInMillis = tookInMillis;
    }

    @Override
    public Iterator<Item> iterator() {
        return Iterators.forArray(items);
    }

    /**
     * The list of responses, the order is the same as the one provided in the request.
     */
    public Item[] getResponses() {
        return this.items;
    }

    /**
     * How long the msearch_template took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(items);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_0_0)) {
            out.writeVLong(tookInMillis);
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            Iterators.single((b, p) -> b.field("took", tookInMillis).startArray(Fields.RESPONSES)),
            Iterators.flatMap(Iterators.forArray(items), item -> {
                if (item.isFailure()) {
                    return Iterators.single((b, p) -> {
                        b.startObject();
                        ElasticsearchException.generateFailureXContent(b, params, item.getFailure(), true);
                        return b.field(Fields.STATUS, ExceptionsHelper.status(item.getFailure()).getStatus()).endObject();
                    });
                } else {
                    return Iterators.concat(
                        ChunkedToXContentHelper.startObject(),
                        item.getResponse().innerToXContentChunked(params),
                        Iterators.single((b, p) -> b.field(Fields.STATUS, item.getResponse().status().getStatus())),
                        ChunkedToXContentHelper.endObject()
                    );
                }
            }),
            Iterators.single((b, p) -> b.endArray()),
            ChunkedToXContentHelper.endObject()
        );
    }

    static final class Fields {
        static final String RESPONSES = "responses";
        static final String STATUS = "status";
    }

    public static MultiSearchTemplateResponse fromXContext(XContentParser parser) {
        // The MultiSearchTemplateResponse is identical to the multi search response so we reuse the parsing logic in multi search response
        MultiSearchResponse mSearchResponse = MultiSearchResponse.fromXContext(parser);
        try {
            org.elasticsearch.action.search.MultiSearchResponse.Item[] responses = mSearchResponse.getResponses();
            Item[] templateResponses = new Item[responses.length];
            int i = 0;
            for (org.elasticsearch.action.search.MultiSearchResponse.Item item : responses) {
                SearchTemplateResponse stResponse = null;
                if (item.getResponse() != null) {
                    stResponse = new SearchTemplateResponse();
                    stResponse.setResponse(item.getResponse());
                }
                templateResponses[i++] = new Item(stResponse, item.getFailure());
            }
            return new MultiSearchTemplateResponse(templateResponses, mSearchResponse.getTook().millis());
        } finally {
            mSearchResponse.decRef();
        }
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
