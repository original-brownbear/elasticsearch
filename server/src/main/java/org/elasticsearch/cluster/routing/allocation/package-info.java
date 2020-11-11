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

/**
 * This package contains the code for the shard allocation logic that determines the assignment of shards to data nodes.
 * The shard allocation logic mechanism works by the master node putting the shard allocation into the cluster state and the data nodes
 * then starting shards on themselves according to the contents of the cluster state. The specific mechanism for starting shards on data
 * nodes is the subject of the recovery mechanism found in the {@link org.elasticsearch.indices.recovery} package. This package only
 * covers the logic for computing the shard allocation on the master node itself.
 *
 * The allocation of shards at a given point in time is modeled via an instance of {@link org.elasticsearch.cluster.routing.RoutingTable}
 * found in {@link org.elasticsearch.cluster.ClusterState#routingTable()}.
 *
 *
 */
package org.elasticsearch.cluster.routing.allocation;