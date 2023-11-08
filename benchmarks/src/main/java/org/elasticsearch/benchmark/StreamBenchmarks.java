/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;

@Fork(value = 1, jvmArgsAppend = { "-XX:+UnlockDiagnosticVMOptions" })
@Threads(1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class StreamBenchmarks {

    @Param({ "1", "3", "10", "25" })
    public int nodes;
    public DiscoveryNodes discoveryNodes;

    @Setup
    public void setup() {
        var builder = DiscoveryNodes.builder();
        for (int n = 0; n < nodes; n++) {
            builder.add(
                new DiscoveryNode(
                    "node-" + n,
                    "node-" + n,
                    new TransportAddress(TransportAddress.META_ADDRESS, 10_000 + n),
                    Map.of(),
                    Set.of(DiscoveryNodeRole.DATA_ROLE),
                    VersionInformation.CURRENT
                )
            );
        }
        discoveryNodes = builder.build();
    }

    @Benchmark
    public Set<String> nodeIdsFromIterator() {
        var nodeIds = new HashSet<String>();
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            nodeIds.add(discoveryNode.getId());
        }
        return nodeIds;
    }

    @Benchmark
    public Set<String> nodeIdsFromIteratorWithKnownSize() {
        var nodeIds = Sets.<String>newHashSetWithExpectedSize(discoveryNodes.size());
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            nodeIds.add(discoveryNode.getId());
        }
        return nodeIds;
    }

    @Benchmark
    public Set<String> nodeIdsFromStream() {
        return discoveryNodes.stream().map(DiscoveryNode::getId).collect(toSet());
    }

    @Benchmark
    public Set<String> nodeIdsFromStaticHelper() {
        return collect(discoveryNodes, DiscoveryNode::getId, () -> Sets.newHashSetWithExpectedSize(discoveryNodes.size()));
    }

    private static <V, T, C extends Set<T>> Set<T> collect(Iterable<V> iterable, Function<V, T> mapper, Supplier<C> collection) {
        var values = collection.get();
        for (V value : iterable) {
            values.add(mapper.apply(value));
        }
        return values;
    }
}
