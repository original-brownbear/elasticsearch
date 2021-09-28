/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.index.shard.ShardId;

import java.util.Iterator;
import java.util.List;

public class IndexShardSegments implements Iterable<ShardSegments> {

    private final ShardId shardId;

    private final List<ShardSegments> shards;

    IndexShardSegments(ShardId shardId, List<ShardSegments> shards) {
        this.shardId = shardId;
        this.shards = List.copyOf(shards);
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public ShardSegments getAt(int i) {
        return shards.get(i);
    }

    public List<ShardSegments> getShards() {
        return this.shards;
    }

    @Override
    public Iterator<ShardSegments> iterator() {
        return shards.iterator();
    }
}
