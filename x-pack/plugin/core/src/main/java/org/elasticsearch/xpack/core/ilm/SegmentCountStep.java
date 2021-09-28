/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;

/**
 * This {@link Step} evaluates whether force_merge was successful by checking the segment count.
 */
public class SegmentCountStep extends AsyncWaitStep {

    private static final Logger logger = LogManager.getLogger(SegmentCountStep.class);
    public static final String NAME = "segment-count";

    private final int maxNumSegments;

    public SegmentCountStep(StepKey key, StepKey nextStepKey, Client client, int maxNumSegments) {
        super(key, nextStepKey, client);
        this.maxNumSegments = maxNumSegments;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    @Override
    public void evaluateCondition(Metadata metadata, Index index, Listener listener, TimeValue masterTimeout) {
        getClient().admin().indices().segments(new IndicesSegmentsRequest(index.getName()),
            ActionListener.wrap(response -> {
                IndexSegments idxSegments = response.getIndices().get(index.getName());
                if (idxSegments == null || (response.getShardFailures() != null && response.getShardFailures().length > 0)) {
                    final DefaultShardOperationFailedException[] failures = response.getShardFailures();
                    for (DefaultShardOperationFailedException failure : failures) {
                        logger.warn(
                            new ParameterizedMessage(
                                "[{}][{}] retrieval of segment counts after force merge did not succeed", index, failure.shardId()),
                                failure
                        );
                    }
                    listener.onResponse(true, new Info(-1));
                } else {
                    int unmergedCount = 0;
                    for (IndexShardSegments iss : idxSegments.getShards().values()) {
                        for (ShardSegments shardSegments : iss.getShards()) {
                            final int segmentCount = shardSegments.getSegments().size();
                            if (segmentCount > maxNumSegments) {
                                logger.info("[{}] best effort force merge to [{}] segments did not succeed for {}: {}",
                                        index.getName(), maxNumSegments, shardSegments.getShardRouting(), segmentCount);
                                unmergedCount++;
                            }
                        }
                    }
                    // Force merging is best effort, so always return true that the condition has been met.
                    listener.onResponse(true, new Info(unmergedCount));
                }
            }, listener::onFailure));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxNumSegments);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SegmentCountStep other = (SegmentCountStep) obj;
        return super.equals(obj)
            && Objects.equals(maxNumSegments, other.maxNumSegments);
    }

    public static class Info implements ToXContentObject {

        private final long numberShardsLeftToMerge;

        static final ParseField SHARDS_TO_MERGE = new ParseField("shards_left_to_merge");
        static final ParseField MESSAGE = new ParseField("message");
        static final ConstructingObjectParser<Info, Void> PARSER = new ConstructingObjectParser<>("segment_count_step_info",
                a -> new Info((long) a[0]));
        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SHARDS_TO_MERGE);
            PARSER.declareString((i, s) -> {}, MESSAGE);
        }

        public Info(long numberShardsLeftToMerge) {
            this.numberShardsLeftToMerge = numberShardsLeftToMerge;
        }

        public long getNumberShardsLeftToMerge() {
            return numberShardsLeftToMerge;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (numberShardsLeftToMerge == 0) {
                builder.field(MESSAGE.getPreferredName(), "all shards force merged successfully");
            } else {
                builder.field(MESSAGE.getPreferredName(),
                    "[" + numberShardsLeftToMerge + "] shards did not successfully force merge");
            }
            builder.field(SHARDS_TO_MERGE.getPreferredName(), numberShardsLeftToMerge);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(numberShardsLeftToMerge);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Info other = (Info) obj;
            return Objects.equals(numberShardsLeftToMerge, other.numberShardsLeftToMerge);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
