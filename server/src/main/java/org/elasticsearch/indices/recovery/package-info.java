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
 * Recoveries are started on data nodes through cluster state changes. The master node sets up shard allocations in the cluster state
 * (see {@link org.elasticsearch.cluster.routing.ShardRouting}) that are then inspected by data nodes. If data nodes find shard allocations
 * that require recovery on their end they will execute the required recoveries, triggered by the logic starting at
 * {@link org.elasticsearch.indices.cluster.IndicesClusterStateService#createOrUpdateShards}.
 * As the data nodes execute the steps of the recovery state machine they report back success or failure to do so to the current master
 * node via the transport actions in {@link org.elasticsearch.cluster.action.shard.ShardStateAction} which will then update the shard
 * routing in the cluster state accordingly.
 * Recoveries can have various kinds of sources that are modeled via the {@link org.elasticsearch.cluster.routing.RecoverySource}
 * returned by {@link org.elasticsearch.cluster.routing.ShardRouting#recoverySource()} for each shard routing. These sources and their
 * state machines will be described below. The actual recovery for all of them is started by invoking
 * {@link org.elasticsearch.index.shard.IndexShard#startRecovery)}
 *
 * <h2>Peer Recovery</h2>
 *
 * Peer recoveries are modeled via a {@link org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource}.
 * They start by moving the shard's state to {@link org.elasticsearch.index.shard.IndexShardState#RECOVERING} and then triggering the
 * peer recovery through a call to {@link org.elasticsearch.indices.recovery.PeerRecoveryTargetService#startRecovery} which results
 * in the following steps being executed.
 *
 * <li>
 *     <ul>
 *         A {@link  org.elasticsearch.indices.recovery.StartRecoveryRequest} is sent to the primary node of the shard to recover by the
 *         data node that is recovering the shard. This triggers {@link org.elasticsearch.indices.recovery.PeerRecoverySourceService#recover}
 *         on the primary node that receives the request. The {@code StartRecoveryRequest} contains information about the local state
*          of the recovery target, based on which the recovery source will determine the recovery mechanism to use.</ul>
 *     <ul>
 *         In the simplest case, there is no shared existing data on the recovery target node:
 *         In this case, the recovery source node will execute phase 1 of the recovery by invoking
 *         {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#phase1}. Using the information about the files on the target node
 *         found in the {@code StartRecoveryRequest}, phase 1 will determine what segment files must be copied to the recovery target.
 *         The information about these files will then be sent to the recovery target via a
 *         {@link org.elasticsearch.indices.recovery.RecoveryFilesInfoRequest}. Once the recovery target has received the list of files
 *         that will be copied to it, {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#sendFiles} is invoked which
 *         will send the segment files over to the recovery target via a series of
 *         {@link org.elasticsearch.indices.recovery.RecoveryFileChunkRequest}.</ul>
 *     <ul>
 *         TODO: describe retention lease things
 *     <ul/>
 *     <ul>
 *         After the segment files have been copied from the source to the target, the translog based recovery step is executed by
 *         invoking {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#prepareTargetForTranslog} on the recovery source.
 *         This sends a {@link org.elasticsearch.indices.recovery.RecoveryPrepareForTranslogOperationsRequest} to the recovery target
 *         which contains the estimated number of translog operations that have to be copied to the target.
 *         On the target this request is handled and triggers a call to
 *         {@link org.elasticsearch.index.shard.IndexShard#openEngineAndSkipTranslogRecovery()} which opens a new engine and translog
 *         and then responds back to the recovery source.
 *         Once the recovery source receives that response, it invokes
 *         {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#phase2} to replay outstanding translog operations on the target.
 *         This is done by sending a series of {@link org.elasticsearch.indices.recovery.RecoveryTranslogOperationsRequest} to the target
 *         which will respond with {@link org.elasticsearch.indices.recovery.RecoveryTranslogOperationsResponse} (TODO: why does this contain local checkpoint?).
 *     </ul>
 * </li>
 *
 *
 */
package org.elasticsearch.indices.recovery;