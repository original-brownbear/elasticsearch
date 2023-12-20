/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

public class SearchTemplateResponse extends ActionResponse implements ChunkedToXContent {
    public static ParseField TEMPLATE_OUTPUT_FIELD = new ParseField("template_output");

    /** Contains the source of the rendered template **/
    private BytesReference source;

    /** Contains the search response, if any **/
    private SearchResponse response;

    SearchTemplateResponse() {}

    SearchTemplateResponse(StreamInput in) throws IOException {
        super(in);
        source = in.readOptionalBytesReference();
        response = in.readOptionalWriteable(SearchResponse::new);
    }

    public BytesReference getSource() {
        return source;
    }

    public void setSource(BytesReference source) {
        this.source = source;
    }

    public SearchResponse getResponse() {
        return response;
    }

    public void setResponse(SearchResponse searchResponse) {
        this.response = searchResponse;
    }

    public boolean hasResponse() {
        return response != null;
    }

    @Override
    public String toString() {
        return "SearchTemplateResponse [source=" + source + ", response=" + response + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBytesReference(source);
        out.writeOptionalWriteable(response);
    }

    public static SearchTemplateResponse fromXContent(XContentParser parser) throws IOException {
        SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
        Map<String, Object> contentAsMap = parser.map();

        if (contentAsMap.containsKey(TEMPLATE_OUTPUT_FIELD.getPreferredName())) {
            Object source = contentAsMap.get(TEMPLATE_OUTPUT_FIELD.getPreferredName());
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).value(source);
            searchTemplateResponse.setSource(BytesReference.bytes(builder));
        } else {
            XContentType contentType = parser.contentType();
            XContentBuilder builder = XContentFactory.contentBuilder(contentType).map(contentAsMap);
            try (
                XContentParser searchResponseParser = contentType.xContent()
                    .createParser(parser.getXContentRegistry(), parser.getDeprecationHandler(), BytesReference.bytes(builder).streamInput())
            ) {
                searchTemplateResponse.setResponse(SearchResponse.fromXContent(searchResponseParser));
            }
        }
        return searchTemplateResponse;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(ChunkedToXContentHelper.startObject(), innerToXContentChunked(params), ChunkedToXContentHelper.endObject());
    }

    Iterator<? extends ToXContent> innerToXContentChunked(ToXContent.Params params) {
        if (hasResponse()) {
            return response.innerToXContentChunked(params);
        }

        return Iterators.single((b, p) -> {
            b.startObject();
            try (InputStream stream = source.streamInput()) {
                b.rawField(TEMPLATE_OUTPUT_FIELD.getPreferredName(), stream, XContentType.JSON);
            }
            return b.endObject();
        });
    }

    public RestStatus status() {
        if (hasResponse()) {
            return response.status();
        } else {
            return RestStatus.OK;
        }
    }
}
