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
 * <p>This package contains the logic for the recovery functionality.</p>
 *
 * <h2>Preliminaries</h2>
 *
 * Recoveries are started on data nodes through cluster state changes. The master node sets up a shard allocation in the cluster state
 * that is then inspected by data nodes. If data nodes find shard allocations that require recovery on their end they will execute the
 * required recoveries starting in the code starting at
 * {@link org.elasticsearch.indices.cluster.IndicesClusterStateService#createOrUpdateShards}.
 * As the data nodes execute the steps of the recovery state machine they report back success or failure to do so to the current master
 * node via the transport actions in {@link org.elasticsearch.cluster.action.shard.ShardStateAction} which will then update the shard
 * routing in the cluster state accordingly.
 *
 *
 */
package org.elasticsearch.indices.recovery;