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

package org.elasticsearch.index.snapshots.blobstore;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shard snapshot metadata
 */
public class BlobStoreIndexShardSnapshot implements ToXContentFragment {

    private final String snapshot;

    private final long indexVersion;

    private final long startTime;

    private final long time;

    private final int incrementalFileCount;

    private final long incrementalSize;

    private final List<FileInfo> indexFiles;

    /**
     * Constructs new shard snapshot metadata from snapshot metadata
     *
     * @param snapshot              snapshot id
     * @param indexVersion          index version
     * @param indexFiles            list of files in the shard
     * @param startTime             snapshot start time
     * @param time                  snapshot running time
     * @param incrementalFileCount  incremental of files that were snapshotted
     * @param incrementalSize       incremental size of snapshot
     */
    public BlobStoreIndexShardSnapshot(String snapshot, long indexVersion, List<FileInfo> indexFiles,
                                       long startTime, long time,
                                       int incrementalFileCount,
                                       long incrementalSize
    ) {
        assert snapshot != null;
        assert indexVersion >= 0;
        this.snapshot = snapshot;
        this.indexVersion = indexVersion;
        this.indexFiles = List.copyOf(indexFiles);
        this.startTime = startTime;
        this.time = time;
        this.incrementalFileCount = incrementalFileCount;
        this.incrementalSize = incrementalSize;
    }

    /**
     * Returns snapshot id
     *
     * @return snapshot id
     */
    public String snapshot() {
        return snapshot;
    }

    /**
     * Returns snapshot start time
     */
    public long startTime() {
        return startTime;
    }

    /**
     * Returns snapshot running time
     */
    public long time() {
        return time;
    }

    /**
     * Returns incremental of files that were snapshotted
     */
    public int incrementalFileCount() {
        return incrementalFileCount;
    }

    /**
     * Returns total number of files that are referenced by this snapshot
     */
    public int totalFileCount() {
        return indexFiles.size();
    }

    /**
     * Returns incremental of files size that were snapshotted
     */
    public long incrementalSize() {
        return incrementalSize;
    }

    /**
     * Returns total size of all files that where snapshotted
     */
    public long totalSize() {
        return indexFiles.stream().mapToLong(fi -> fi.metadata().length()).sum();
    }

    private static final String NAME = "name";
    private static final String INDEX_VERSION = "index_version";
    private static final String START_TIME = "start_time";
    private static final String TIME = "time";
    private static final String FILES = "files";
    // for the sake of BWC keep the actual property names as in 6.x
    // + there is a constraint in #fromXContent() that leads to ElasticsearchParseException("unknown parameter [incremental_file_count]");
    private static final String INCREMENTAL_FILE_COUNT = "number_of_files";
    private static final String INCREMENTAL_SIZE = "total_size";


    private static final ParseField PARSE_NAME = new ParseField(NAME);
    private static final ParseField PARSE_INDEX_VERSION = new ParseField(INDEX_VERSION, "index-version");
    private static final ParseField PARSE_START_TIME = new ParseField(START_TIME);
    private static final ParseField PARSE_TIME = new ParseField(TIME);
    private static final ParseField PARSE_INCREMENTAL_FILE_COUNT = new ParseField(INCREMENTAL_FILE_COUNT);
    private static final ParseField PARSE_INCREMENTAL_SIZE = new ParseField(INCREMENTAL_SIZE);
    private static final ParseField PARSE_FILES = new ParseField(FILES);

    /**
     * Serializes shard snapshot metadata info into JSON
     *
     * @param builder  XContent builder
     * @param params   parameters
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, snapshot);
        builder.field(INDEX_VERSION, indexVersion);
        builder.field(START_TIME, startTime);
        builder.field(TIME, time);
        builder.field(INCREMENTAL_FILE_COUNT, incrementalFileCount);
        builder.field(INCREMENTAL_SIZE, incrementalSize);
        builder.startArray(FILES);
        for (FileInfo fileInfo : indexFiles) {
            FileInfo.toXContent(fileInfo, builder);
        }
        builder.endArray();
        return builder;
    }

    /**
     * Parses shard snapshot metadata
     *
     * @param parser parser
     * @return shard snapshot metadata
     */
    public static BlobStoreIndexShardSnapshot fromXContent(XContentParser parser) throws IOException {
        String snapshot = null;
        long indexVersion = -1;
        long startTime = 0;
        long time = 0;
        int incrementalFileCount = 0;
        long incrementalSize = 0;

        List<FileInfo> indexFiles = new ArrayList<>();
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if (PARSE_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                            snapshot = parser.text();
                        } else if (PARSE_INDEX_VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                            // The index-version is needed for backward compatibility with v 1.0
                            indexVersion = parser.longValue();
                        } else if (PARSE_START_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                            startTime = parser.longValue();
                        } else if (PARSE_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                            time = parser.longValue();
                        } else if (PARSE_INCREMENTAL_FILE_COUNT.match(currentFieldName, parser.getDeprecationHandler())) {
                            incrementalFileCount = parser.intValue();
                        } else if (PARSE_INCREMENTAL_SIZE.match(currentFieldName, parser.getDeprecationHandler())) {
                            incrementalSize = parser.longValue();
                        } else {
                            throw new ElasticsearchParseException("unknown parameter [{}]", currentFieldName);
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (PARSE_FILES.match(currentFieldName, parser.getDeprecationHandler())) {
                            while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                indexFiles.add(FileInfo.fromXContent(parser));
                            }
                        } else {
                            throw new ElasticsearchParseException("unknown parameter [{}]", currentFieldName);
                        }
                    } else {
                        throw new ElasticsearchParseException("unexpected token  [{}]", token);
                    }
                } else {
                    throw new ElasticsearchParseException("unexpected token [{}]", token);
                }
            }
        }

        return new BlobStoreIndexShardSnapshot(snapshot, indexVersion, Collections.unmodifiableList(indexFiles),
                                               startTime, time, incrementalFileCount, incrementalSize);
    }
}
