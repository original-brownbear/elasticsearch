/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Base class for all individual search phases like collecting distributed frequencies, fetching documents, querying shards.
 */
abstract class SearchPhase implements CheckedRunnable<IOException> {
    private final String name;

    protected SearchPhase(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    /**
     * Returns the phases name.
     */
    public String getName() {
        return name;
    }

    public void start() {
        try {
            run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected final void checkNoMissingShards(
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardsIts
    ) {
        assert request.allowPartialSearchResults() != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (request.allowPartialSearchResults() == false) {
            final StringBuilder missingShards = new StringBuilder();
            // Fail-fast verification of all shards being available
            for (int index = 0; index < shardsIts.size(); index++) {
                final SearchShardIterator shardRoutings = shardsIts.get(index);
                if (shardRoutings.size() == 0) {
                    if (missingShards.length() > 0) {
                        missingShards.append(", ");
                    }
                    missingShards.append(shardRoutings.shardId());
                }
            }
            if (missingShards.length() > 0) {
                // Status red - shard is missing all copies and would produce partial results for an index search
                final String msg = "Search rejected due to missing shards ["
                    + missingShards
                    + "]. Consider using `allow_partial_search_results` setting to bypass this error.";
                throw new SearchPhaseExecutionException(name, msg, null, ShardSearchFailure.EMPTY_ARRAY);
            }
        }
        var version = request.minCompatibleShardNode();
        if (version != null && Version.CURRENT.minimumCompatibilityVersion().equals(version) == false) {
            if (checkMinimumVersion(nodeIdToConnection, request, shardsIts) == false) {
                throw new VersionMismatchException(
                    "One of the shards is incompatible with the required minimum version [{}]",
                    request.minCompatibleShardNode()
                );
            }
        }
    }

    protected static Transport.Connection getConnection(
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        SearchRequest request,
        String clusterAlias,
        String nodeId
    ) {
        Transport.Connection conn = nodeIdToConnection.apply(clusterAlias, nodeId);
        Version minVersion = request.minCompatibleShardNode();
        if (minVersion != null && conn != null && conn.getVersion().before(minVersion)) {
            throw new VersionMismatchException("One of the shards is incompatible with the required minimum version [{}]", minVersion);
        }
        return conn;
    }

    private static boolean checkMinimumVersion(
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardsIts
    ) {
        for (SearchShardIterator it : shardsIts) {
            if (it.getTargetNodeIds().isEmpty() == false) {
                boolean isCompatible = it.getTargetNodeIds().stream().anyMatch(nodeId -> {
                    Transport.Connection conn = getConnection(nodeIdToConnection, request, it.getClusterAlias(), nodeId);
                    return conn == null || conn.getVersion().onOrAfter(request.minCompatibleShardNode());
                });
                if (isCompatible == false) {
                    return false;
                }
            }
        }
        return true;
    }
}
