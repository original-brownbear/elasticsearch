/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.segments;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IndicesSegmentResponse extends BroadcastResponse {

    private final List<ShardSegments> shards;

    private Map<String, IndexSegments> indicesSegments;

    IndicesSegmentResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readList(ShardSegments::new);
    }

    IndicesSegmentResponse(List<ShardSegments> shards, int totalShards, int successfulShards, int failedShards,
                           List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = List.copyOf(shards);
    }

    public Map<String, IndexSegments> getIndices() {
        if (indicesSegments != null) {
            return indicesSegments;
        }
        final Map<String, List<ShardSegments>> indicesSegmentsTmp = new HashMap<>();

        for (ShardSegments shard : shards) {
            indicesSegmentsTmp.computeIfAbsent(shard.getShardRouting().getIndexName(), k -> new ArrayList<>()).add(shard);
        }
        final Map<String, IndexSegments> tmpSegments = new HashMap<>(indicesSegmentsTmp.size());
        for (Map.Entry<String, List<ShardSegments>> entry : indicesSegmentsTmp.entrySet()) {
            tmpSegments.put(entry.getKey(), new IndexSegments(entry.getKey(), entry.getValue()));
        }

        this.indicesSegments = Map.copyOf(tmpSegments);
        return indicesSegments;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(shards);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        assert Transports.assertNotTransportThread("segments are very numerous, too expensive to serialize on a transport thread");

        builder.startObject(Fields.INDICES);

        for (IndexSegments indexSegments : getIndices().values()) {
            builder.startObject(indexSegments.getIndex());

            builder.startObject(Fields.SHARDS);
            for (IndexShardSegments indexSegment : indexSegments) {
                builder.startArray(Integer.toString(indexSegment.getShardId().id()));
                for (ShardSegments shardSegments : indexSegment) {
                    builder.startObject();

                    builder.startObject(Fields.ROUTING);
                    builder.field(Fields.STATE, shardSegments.getShardRouting().state());
                    builder.field(Fields.PRIMARY, shardSegments.getShardRouting().primary());
                    builder.field(Fields.NODE, shardSegments.getShardRouting().currentNodeId());
                    if (shardSegments.getShardRouting().relocatingNodeId() != null) {
                        builder.field(Fields.RELOCATING_NODE, shardSegments.getShardRouting().relocatingNodeId());
                    }
                    builder.endObject();

                    builder.field(Fields.NUM_COMMITTED_SEGMENTS, shardSegments.getNumberOfCommitted());
                    builder.field(Fields.NUM_SEARCH_SEGMENTS, shardSegments.getNumberOfSearch());

                    builder.startObject(Fields.SEGMENTS);
                    for (Segment segment : shardSegments) {
                        builder.startObject(segment.getName());
                        builder.field(Fields.GENERATION, segment.getGeneration());
                        builder.field(Fields.NUM_DOCS, segment.getNumDocs());
                        builder.field(Fields.DELETED_DOCS, segment.getDeletedDocs());
                        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, segment.getSize());
                        builder.humanReadableField(Fields.MEMORY_IN_BYTES, Fields.MEMORY, new ByteSizeValue(0));
                        builder.field(Fields.COMMITTED, segment.isCommitted());
                        builder.field(Fields.SEARCH, segment.isSearch());
                        if (segment.getVersion() != null) {
                            builder.field(Fields.VERSION, segment.getVersion());
                        }
                        if (segment.isCompound() != null) {
                            builder.field(Fields.COMPOUND, segment.isCompound());
                        }
                        if (segment.getMergeId() != null) {
                            builder.field(Fields.MERGE_ID, segment.getMergeId());
                        }
                        if (segment.getSegmentSort() != null) {
                            toXContent(builder, segment.getSegmentSort());
                        }
                        if (segment.attributes != null && segment.attributes.isEmpty() == false) {
                            builder.field("attributes", segment.attributes);
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }

        builder.endObject();
    }

    private static void toXContent(XContentBuilder builder, Sort sort) throws IOException {
        builder.startArray("sort");
        for (SortField field : sort.getSort()) {
            builder.startObject();
            builder.field("field", field.getField());
            if (field instanceof SortedNumericSortField) {
                builder.field("mode", ((SortedNumericSortField) field).getSelector()
                    .toString().toLowerCase(Locale.ROOT));
            } else if (field instanceof SortedSetSortField) {
                builder.field("mode", ((SortedSetSortField) field).getSelector()
                    .toString().toLowerCase(Locale.ROOT));
            }
            if (field.getMissingValue() != null) {
                builder.field("missing", field.getMissingValue().toString());
            }
            builder.field("reverse", field.getReverse());
            builder.endObject();
        }
        builder.endArray();
    }

    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
        static final String ROUTING = "routing";
        static final String STATE = "state";
        static final String PRIMARY = "primary";
        static final String NODE = "node";
        static final String RELOCATING_NODE = "relocating_node";

        static final String SEGMENTS = "segments";
        static final String GENERATION = "generation";
        static final String NUM_COMMITTED_SEGMENTS = "num_committed_segments";
        static final String NUM_SEARCH_SEGMENTS = "num_search_segments";
        static final String NUM_DOCS = "num_docs";
        static final String DELETED_DOCS = "deleted_docs";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String COMMITTED = "committed";
        static final String SEARCH = "search";
        static final String VERSION = "version";
        static final String COMPOUND = "compound";
        static final String MERGE_ID = "merge_id";
        static final String MEMORY = "memory";
        static final String MEMORY_IN_BYTES = "memory_in_bytes";
    }
}
