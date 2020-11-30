/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.function.BooleanSupplier;

public class SearchableSnapshotAllocationDecider extends AllocationDecider {

    static final String NAME = "searchable_snapshots";

    private final BooleanSupplier hasValidLicenseSupplier;

    public SearchableSnapshotAllocationDecider(BooleanSupplier hasValidLicenseSupplier) {
        this.hasValidLicenseSupplier = hasValidLicenseSupplier;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return allowAllocation(allocation.metadata().getIndexSafe(shardRouting.index()));
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return allowAllocation(allocation.metadata().getIndexSafe(shardRouting.index()));
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return allowAllocation(indexMetadata);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return allowAllocation(allocation.metadata().getIndexSafe(shardRouting.index()));
    }

    private static final Decision YES_VALID_LICENSE = Decision.single(Decision.Type.YES, NAME, "valid license for searchable snapshots");
    private static final Decision NO_INVALID_LICENSE = Decision.single(Decision.Type.NO, NAME, "invalid license for searchable snapshots");
    private static final Decision YES_NOT_SNAPSHOT = Decision.single(
        Decision.Type.YES,
        NAME,
        "decider only applicable for indices backed by searchable snapshots"
    );

    private Decision allowAllocation(IndexMetadata indexMetadata) {
        if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexMetadata.getSettings())) {
            return hasValidLicenseSupplier.getAsBoolean() ? YES_VALID_LICENSE : NO_INVALID_LICENSE;
        } else {
            return YES_NOT_SNAPSHOT;
        }
    }
}
