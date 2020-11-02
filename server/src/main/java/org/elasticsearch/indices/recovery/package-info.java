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
 * {@link org.elasticsearch.index.shard.IndexShard#startRecovery)}.
 *
 * <h3>Checkpoints</h3>
 *
 * Many aspects of the recovery logic are based on the notion of local and global checkpoints. Each operation on a shard is tracked by a
 * sequence number and the primary term during which it was applied to the index. The sequence number up to which operations have been
 * processed on a shard is that shard's local checkpoint. The sequence number up to which operations on all replicas for a shard have been
 * fully processed is referred to as the global checkpoint. Comparing the local of shard copies on a high level allows determining
 * which operations have to be applied to a shard copy to bring it in-sync with the primary. By retaining operations in the
 * {@link org.elasticsearch.indices.recovery.RecoveryState.Translog} they are available for replay to bring a shard from a certain local
 * checkpoint to a higher local checkpoint.
 * The global checkpoint allows for determining which operations are safely processed on all shards and thus don't have to be retained on
 * the primary node for replaying them to replicas.
 *
 * The primary node tracks the global checkpoint via {@link org.elasticsearch.index.seqno.ReplicationTracker}. The primary term is tracked
 * via the master node and in the cluster state and incremented each time the primary node for a shard changes.
 *
 * <h2>Peer Recovery</h2>
 *
 * Peer recovery is the process of bringing a shard copy on one node, referred to as the target node below, in-sync with the shard copy on
 * another node, referred to as the source node below. It is always the primary node of a shard that serves as the source of the recovery.
 * On a high level, recovery happens by a combination of comparing and subsequently synchronizing files and applied operations on the
 * target with the source.
 * Synchronizing the on disk file structure on the target with those on the source node is often referred to as file based recovery in
 * documentation. Synchronizing operations based on comparing checkpoints is commonly referred to as ops based recovery.
 * As primaries and replicas are independent Lucene indices that will execute their Lucene level merges independently the concrete on disk
 * file structure on a pair of primary and replica nodes for a given shard will diverge over time even if both copies of the shard
 * hold the same set of documents. Peer recovery will therefore try to avoid file based recovery where possible in order to reduce the
 * amount of data that has to be transferred and prefer replaying just those operations missing on the target relative to the source.
 * Replaying operations is possible as long as the primary node retains the missing operations in its
 * {@link org.elasticsearch.indices.recovery.RecoveryState.Translog}.
 *
 * <h3>Retention Leases</h3>
 *
 * The duration for which a shard retains individual operations alongside
 *
 * <h3>State Machine</h3>
 *
 * Peer recoveries are modeled via a {@link org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource}. They start by moving the
 * shard's state to {@link org.elasticsearch.index.shard.IndexShardState#RECOVERING} and then triggering the peer recovery through a call
 * to {@link org.elasticsearch.indices.recovery.PeerRecoveryTargetService#startRecovery} which results in the following steps being
 * executed.
 *
 * TODO: spell out exact target shard states in the CS below
 * <li>
 *     <ul>
 *         The target shard starts out with a {@link org.elasticsearch.indices.recovery.RecoveryState} at stage
 *         {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#INIT}. At the start of the peer recovery process the target node
 *         will try to recover from its local translog as far as if there are any operations to recover from it. It will first move to
 *         stage {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#INDEX} and then try to recover as far as possible from
 *         existing files and the existing translog. After that it moves to
 *         {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#VERIFY_INDEX}. //TODO: continue here and explain verify index
 *         // TODO: talk about org.elasticsearch.index.shard.IndexShard#resetRecoveryStage()
 *         A {@link  org.elasticsearch.indices.recovery.StartRecoveryRequest} is then sent to the primary node of the shard to recover by
 *         the target node for the recovery. This triggers
 *         {@link org.elasticsearch.indices.recovery.PeerRecoverySourceService#recover} on the primary node that receives the request. The
 *         {@code StartRecoveryRequest} contains information about the local state of the recovery target, based on which the recovery
 *         source will determine the recovery mechanism (file based or ops based) to use.
 *     </ul>
 *     <ul>
 *        When determining whether to use ops based recovery the recovery source will check the following conditions
 *        that must all be true simultaneously for ops based recovery to be executed:
 *        <li>
 *            <ul>
 *                Target shard and source shard must share the same
 *                {@link org.elasticsearch.index.engine.Engine#HISTORY_UUID_KEY} in their latest Lucene commit.
 *            </ul>
 *            <ul>
 *                The source must have retained all operations between the latest sequence number present on the target.
 *                See {@link org.elasticsearch.index.shard.IndexShard#hasCompleteHistoryOperations} for details.
 *            </ul>
 *            <ul>
 *                TODO: When does this retention lease get instantiated?
 *                A peer recovery retention lease must exist for the target shard and it must retain a sequence number below or equal
 *                to the starting sequence number in {@link org.elasticsearch.indices.recovery.StartRecoveryRequest#startingSeqNo()}.
 *            </ul>
 *        </li>
 *     </ul>
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
 *         // TODO: explain retention lease logic in details
 *         // TODO: soft deletes in detail maybe?
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
 *         which will respond with {@link org.elasticsearch.indices.recovery.RecoveryTranslogOperationsResponse}s which contain the
 *         maximum persisted local checkpoint for the target. Tracking the maximum of the received local checkpoint values is necessary
 *         for the next step, finalizing the recovery.
 *     </ul>
 *     <ul>
 *         After having replayed the translog operations, the recovery is finalized by a call to
 *         {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#finalizeRecovery} on the source. With the knowledge that the
 *         target has received all operations up to the maximum local checkpoint from tracked in the previous step, the source
 *         (which is also the primary) can now update its in-sync checkpoint state by a call to
 *         {@link org.elasticsearch.index.seqno.ReplicationTracker#markAllocationIdAsInSync}.
 *         Once the in-sync sequence number information has been persisted successfully, the source sends a
 *         {@link org.elasticsearch.indices.recovery.RecoveryFinalizeRecoveryRequest} to the target which contains the global checkpoint
 *         as well as a sequence number above which the target can trim all operations from its translog since all operations above this
 *         number have just been replayed in the previous step.
 *     </ul>
 * </li>
 *
 *
 */
package org.elasticsearch.indices.recovery;