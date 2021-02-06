/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexLongFieldRange;

import java.util.Map;

/**
 * Tracks the mapping of the {@code @timestamp} field of immutable indices that expose their timestamp range in their index metadata.
 * Coordinating nodes do not have (easy) access to mappings for all indices, so we extract the type of this one field from the mapping here.
 */
public class TimestampFieldMapperService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(TimestampFieldMapperService.class);

    private final IndicesService indicesService;

    private static final DateFieldMapper.DateFieldType NULL_TYPE = new DateFieldMapper.DateFieldType("NULL");

    /**
     * The type of the {@code @timestamp} field keyed by index.
     */
    private final Map<Index, DateFieldMapper.DateFieldType> fieldTypesByIndex = ConcurrentCollections.newConcurrentMap();

    public TimestampFieldMapperService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        final Metadata metadata = event.state().metadata();

        // clear out mappers for indices that no longer exist or whose timestamp range is no longer known
        fieldTypesByIndex.keySet().removeIf(index -> hasUsefulTimestampField(metadata.index(index)) == false);

        // capture mappers for indices that do exist
        for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
            final IndexMetadata indexMetadata = cursor.value;
            final Index index = indexMetadata.getIndex();

            if (hasUsefulTimestampField(indexMetadata) && fieldTypesByIndex.containsKey(index) == false) {
                logger.trace("computing timestamp mapping for {}", index);
                final IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    logger.trace("computing timestamp mapping for {} async", index);
                    try (MapperService mapperService = indicesService.createIndexMapperService(indexMetadata)) {
                        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
                        fieldTypesByIndex.put(index, fromMapperService(mapperService));
                    } catch (Exception e) {
                        logger.debug(new ParameterizedMessage("failed to compute mapping for {}", index), e);
                        fieldTypesByIndex.put(index, NULL_TYPE); // no need to propagate a failure to create the mapper service to searches
                    }
                } else {
                    logger.trace("computing timestamp mapping for {} using existing index service", index);
                    try {
                        fieldTypesByIndex.put(index, fromMapperService(indexService.mapperService()));
                    } catch (Exception e) {
                        assert false : e;
                        fieldTypesByIndex.put(index, NULL_TYPE);
                    }
                }
            }
        }
    }

    private static boolean hasUsefulTimestampField(IndexMetadata indexMetadata) {
        if (indexMetadata == null) {
            return false;
        }
        final IndexLongFieldRange timestampRange = indexMetadata.getTimestampRange();
        return timestampRange.isComplete() && timestampRange != IndexLongFieldRange.UNKNOWN;
    }

    private static DateFieldMapper.DateFieldType fromMapperService(MapperService mapperService) {
        final MappedFieldType mappedFieldType = mapperService.fieldType(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD);
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType) {
            return (DateFieldMapper.DateFieldType) mappedFieldType;
        } else {
            return null;
        }
    }

    /**
     * @return the field type of the {@code @timestamp} field of the given index, or {@code null} if:
     * - the index is not found,
     * - the field is not found,
     * - the mapping is not known yet, or
     * - the field is not a timestamp field.
     */
    @Nullable
    public DateFieldMapper.DateFieldType getTimestampFieldType(Index index) {
        DateFieldMapper.DateFieldType type = fieldTypesByIndex.get(index);
        return type == NULL_TYPE ? null : type;
    }

}
