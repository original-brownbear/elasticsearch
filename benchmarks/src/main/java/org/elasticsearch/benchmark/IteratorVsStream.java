/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Fork(value = 1, jvmArgsAppend = { "-XX:+UnlockDiagnosticVMOptions" })
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Threads(1)
public class IteratorVsStream {

    private Set<DiscoveryNode> discoveryNodes;
    private List<DiscoveryNode> discoveryNodes2;
    private SortedSet<DiscoveryNode> discoveryNodes3;

    @Setup
    public void init() {
        this.discoveryNodes = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            this.discoveryNodes.add(
                new DiscoveryNode(
                    "node-" + i,
                    UUIDs.randomBase64UUID(),
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9200),
                    Map.of(),
                    Set.of(),
                    null
                )
            );
        }
        this.discoveryNodes2 = new ArrayList<>(discoveryNodes);
        this.discoveryNodes3 = new TreeSet<>(Comparator.comparing(DiscoveryNode::getName));
        discoveryNodes3.addAll(discoveryNodes);
    }

    @Benchmark
    public Object iterator() {
        Set<String> result = Sets.newHashSetWithExpectedSize(discoveryNodes.size());
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            result.add(discoveryNode.getName());
        }
        Set<String> result2 = Sets.newLinkedHashSetWithExpectedSize(discoveryNodes2.size());
        for (DiscoveryNode discoveryNode : discoveryNodes2) {
            result2.add(discoveryNode.getId());
        }
        List<TransportAddress> result3 = new ArrayList<>(discoveryNodes3.size());
        for (DiscoveryNode discoveryNode : discoveryNodes3) {
            result3.add(discoveryNode.getAddress());
        }
        return Tuple.tuple(Tuple.tuple(result, result2), result3);
    }

    @Benchmark
    public Object stream() {
        Set<String> result = discoveryNodes.stream().map(DiscoveryNode::getName).collect(Collectors.toSet());
        Set<String> result2 = discoveryNodes2.stream().map(DiscoveryNode::getId).collect(Collectors.toCollection(LinkedHashSet::new));
        List<TransportAddress> result3 = discoveryNodes3.stream().map(DiscoveryNode::getAddress).collect(Collectors.toList());
        return Tuple.tuple(Tuple.tuple(result, result2), result3);
    }
}
