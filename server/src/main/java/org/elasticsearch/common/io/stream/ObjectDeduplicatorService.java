package org.elasticsearch.common.io.stream;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ObjectDeduplicatorService extends AbstractLifecycleComponent implements ClusterStateListener {

    private volatile Map<Object, Object> knownInstances = Collections.emptyMap();

    private final ClusterService clusterService;

    public ObjectDeduplicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.routingTableChanged() || event.nodesDelta().hasChanges()) {
            final Map<Object, Object> updated = new HashMap<>();
            final ClusterState newState = event.state();
            for (ShardRouting routing : newState.getRoutingTable().allShards()) {
                final ShardId shardId = routing.shardId();
                updated.put(shardId, shardId);
                updated.put(shardId.getIndex(), shardId.getIndex());
            }
            for (DiscoveryNode node : newState.nodes()) {
                updated.put(node, node);
            }
            knownInstances = updated;
        }
    }

    public <T> T read(Writeable.Reader<T> reader, StreamInput in) throws IOException {
        return deduplicate(reader.read(in));
    }

    @SuppressWarnings("unchecked")
    private <T> T deduplicate(T instance) {
        return (T) knownInstances.getOrDefault(instance, instance);
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
        knownInstances = Collections.emptyMap();
    }

    @Override
    protected void doClose() {
    }
}
