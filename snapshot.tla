----------------------------- MODULE blobstore -----------------------------

\* Simplified specification of the snapshot create and delete blob store interaction.
\* The specification assumes just a single metadata file per snapshot that references
\* all segment files belonging to the snapshot. The real repository structure by comparison
\* contains another two layers of hierarchy by organizing segments into shards and shards into indices.

EXTENDS Naturals, FiniteSets, Sequences, TLC

Snapshots == {"snapshot-A", "snapshot-B"}

AllSegments == {"path/to/shard/segment-X", "path/to/shard/segment-Y", "path/to/shard/segment-Z"}

\* We test all possible mappings of snapshot names to segment files.
Segments == [Snapshots -> SUBSET(AllSegments)]

IndexNBlobs == <<"index-1", "index-2", "index-3", "index-4">>

StateNBlobs == {"blobmeta-1", "blobmeta-2", "blobmeta-3", "blobmeta-4"}

IndexBlobSet == {IndexNBlobs[i]: i \in 1..Len(IndexNBlobs)}

Blobs == IndexBlobSet \union Snapshots \union AllSegments

VARIABLES clusterState, repositoryMeta, outstandingSnapshots, physicalBlobs, segmentMap, indexBlobContent

----
\* Utilities

Max(s) == CHOOSE x \in s : \A y \in s : x >= y

----

Init == \E segs \in Segments:
            /\ clusterState = [
                    repoStateId |-> 0,
                    height |-> 0,
                    snapshotInProgress |-> "NULL",
                    snapshotDeletionInProgress |-> "NULL",
                    wasMaster |-> FALSE
                ]
            /\ repositoryMeta = [repoStateId |-> 1, blobs |-> [b \in Blobs |-> [height |-> 0, state |-> "NULL"]]]
            /\ outstandingSnapshots = Snapshots
            /\ physicalBlobs = [b \in Blobs |-> FALSE]
            /\ segmentMap = segs
            /\ indexBlobContent = [i \in IndexBlobSet |-> {}]

vars == <<clusterState, repositoryMeta, outstandingSnapshots, physicalBlobs, segmentMap, indexBlobContent>>

----

\* Get the next N for writing a new Index-N blob
NextIndex == IF repositoryMeta.blobs[IndexNBlobs[1]].state = "NULL" THEN
                1
             ELSE
                CHOOSE i \in 1..Len(IndexNBlobs): \E j \in 1..Len(IndexNBlobs):
                    /\ i = j + 1
                    /\ repositoryMeta.blobs[IndexNBlobs[j]].state = "DONE"
                    /\ repositoryMeta.blobs[IndexNBlobs[i]].state = "NULL"

\* Mark all files resulting from the snapshot as "UPLOADING" in the metadata
MarkUploads(snapshotBlob) == /\ repositoryMeta' = [repositoryMeta EXCEPT
                                                       !.repoStateId = @ + 1,
                                                       !.blobs = [
                                                            blob \in Blobs |->
                                                                IF blob \in segmentMap[snapshotBlob] \/ blob = snapshotBlob \/ blob = IndexNBlobs[NextIndex] THEN
                                                                    [state |-> "UPLOADING", height |-> clusterState.height]
                                                                ELSE
                                                                    repositoryMeta.blobs[blob]
                                                              ]]

RollbackLastStep == /\ repositoryMeta' = [repositoryMeta EXCEPT
                                                       !.repoStateId = @ + 1,
                                                       !.blobs = [
                                                            blob \in Blobs |->
                                                                IF \/ repositoryMeta.blobs[blob].state = "UPLOADING"
                                                                   \/ repositoryMeta.blobs[blob].state = "DONE"
                                                                        /\ repositoryMeta.blobs[blob].height = clusterState.height
                                                                THEN
                                                                    [state |-> "DELETED", height |-> clusterState.height]
                                                                ELSE
                                                                    repositoryMeta.blobs[blob]
                                                              ]]


\* Are there any pending uploads in the repository meta?
UploadsInProgress == Cardinality({bl \in Blobs: repositoryMeta.blobs[bl].state = "UPLOADING"}) > 0

DeletesInProgress == Cardinality({bl \in Blobs: repositoryMeta.blobs[bl].state = "DELETED"}) > 0

----

StartUploading == /\ clusterState.wasMaster = TRUE
                  /\ UploadsInProgress = FALSE
                  /\ \A b \in Blobs : repositoryMeta.blobs[b].state \in {"DONE", "NULL"}
                  /\ clusterState.snapshotInProgress \in Snapshots
                     /\ repositoryMeta.blobs[clusterState.snapshotInProgress].state = "NULL"
                     /\ MarkUploads(clusterState.snapshotInProgress)
                  /\ clusterState' = [clusterState EXCEPT !.repoStateId = repositoryMeta.repoStateId + 1]
                  /\ indexBlobContent' = [indexBlobContent EXCEPT ![IndexNBlobs[NextIndex]] =
                        (IF NextIndex - 1 = 0 THEN {} ELSE indexBlobContent[IndexNBlobs[NextIndex - 1]]) \union {clusterState.snapshotInProgress}]
                  /\ UNCHANGED <<outstandingSnapshots, physicalBlobs, segmentMap>>

\* Starting the snapshot process by adding the snapshot to the cluster state.
StartSnapshot == /\ clusterState.wasMaster = TRUE
                 /\ UploadsInProgress = FALSE
                 /\ clusterState.snapshotInProgress = "NULL"
                 /\ clusterState.snapshotDeletionInProgress = "NULL"
                 /\ \E s \in outstandingSnapshots:
                    /\ outstandingSnapshots' = outstandingSnapshots \ {s}
                    /\  clusterState' = [clusterState EXCEPT !.snapshotInProgress = s, !.height = @ + 1]
                /\ UNCHANGED <<repositoryMeta, physicalBlobs, segmentMap, indexBlobContent>>

\* Finish a single upload. Modeled as a single step of updating the repository metablob and writing the file.
FinishOneUpload == \E b \in Blobs:
                        /\ clusterState.wasMaster = TRUE
                        /\ repositoryMeta.blobs[b].state = "UPLOADING"
                        /\ repositoryMeta' = [
                                     repositoryMeta EXCEPT !.repoStateId = @ + 1,
                                     !.blobs = [
                                                blob \in Blobs |->
                                                    IF blob = b THEN
                                                        [state |-> "DONE", height |-> repositoryMeta.blobs[blob].height]
                                                    ELSE
                                                        repositoryMeta.blobs[blob]
                                                    ]]
                        /\ physicalBlobs' = [physicalBlobs EXCEPT ![b] = TRUE]
                  /\ UNCHANGED <<clusterState, outstandingSnapshots, segmentMap, indexBlobContent>>

FinishSnapshot == /\ clusterState.wasMaster = TRUE
                  /\ clusterState.snapshotInProgress /= "NULL"
                  /\ \A b \in Blobs : repositoryMeta.blobs[b].state \in {"DONE", "NULL"}
                  /\ repositoryMeta.blobs[clusterState.snapshotInProgress].state = "DONE"
                  /\ clusterState' = [clusterState EXCEPT
                                     !.snapshotInProgress = "NULL",
                                     !.repoStateId = repositoryMeta.repoStateId]
                  /\ UNCHANGED <<repositoryMeta, outstandingSnapshots, physicalBlobs, segmentMap, indexBlobContent>>

\* Losing the cluster state (i.e. restoring from scratch) by moving all CS entries back to defaults
LoseClusterState == /\ clusterState.wasMaster = TRUE
                    /\ clusterState' = [
                            repoStateId |-> 0,
                            height |-> 0,
                            snapshotInProgress |-> "NULL",
                            snapshotDeletionInProgress |-> "NULL",
                            wasMaster |-> FALSE
                        ]
                    /\ UNCHANGED <<repositoryMeta, outstandingSnapshots, physicalBlobs, segmentMap, indexBlobContent>>

RecoverStateAndHeight == /\ clusterState.wasMaster = FALSE
                         /\ clusterState' = [clusterState EXCEPT
                                        !.height = Max({repositoryMeta.blobs[b].height :b \in Blobs}) + (IF UploadsInProgress THEN 0 ELSE 1),
                                        !.repoStateId = repositoryMeta.repoStateId,
                                        !.wasMaster = TRUE]
                         /\ UNCHANGED <<repositoryMeta, outstandingSnapshots, physicalBlobs, segmentMap, indexBlobContent>>

CleanupDanglingRepositoryState == /\ UploadsInProgress
                                  /\ clusterState.snapshotInProgress = "NULL"
                                  /\ clusterState.wasMaster
                                  /\ RollbackLastStep
                                  /\ UNCHANGED <<clusterState, outstandingSnapshots, physicalBlobs, segmentMap, indexBlobContent>>

ExecuteOneDelete == /\ clusterState.wasMaster
                    /\ UploadsInProgress = FALSE \* Physical deletes may only run after all uploads finished
                    /\ DeletesInProgress
                    /\ \E b \in { bl \in Blobs: repositoryMeta.blobs[bl].state = "DELETED"}:
                            /\ physicalBlobs' = [physicalBlobs EXCEPT ![b] = FALSE]
                            /\ repositoryMeta' = [repositoryMeta EXCEPT
                                                           !.repoStateId = @ + 1,
                                                           !.blobs = [
                                                                blob \in Blobs |->
                                                                    IF blob = b THEN
                                                                        [state |-> "NULL", height |-> 0]
                                                                    ELSE
                                                                        repositoryMeta.blobs[blob]
                                                                  ]]
                    /\ UNCHANGED <<clusterState, outstandingSnapshots, segmentMap, indexBlobContent>>


-----

TypeOK == \A b \in Blobs: repositoryMeta.blobs[b].state \in {"NULL", "UPLOADING", "DONE", "DELETED"}

\* All segments must be referenced by snapshots, i.e. the set of all uploading or existing blobs
\* must be the same as the union of all snapshot's segments, snapshot-meta blobs and the root level
\* index blobs.
NoStaleBlobs == /\ {b \in Blobs: repositoryMeta.blobs[b].state \in {"DONE", "UPLOADING"}} =
                                    (UNION {
                                        segmentMap[s]: s \in {
                                            sn \in Snapshots: repositoryMeta.blobs[sn].state \in {"DONE", "UPLOADING"}}})
                                                \union {sn \in Snapshots: repositoryMeta.blobs[sn].state \in {"DONE", "UPLOADING"}}
                                                    \union {ib \in IndexBlobSet: repositoryMeta.blobs[ib].state \in {"DONE", "UPLOADING"}}
                /\ \A b \in Blobs:
                        repositoryMeta.blobs[b].height < clusterState.height => repositoryMeta.blobs[b].state \in {"DONE", "NULL", "DELETED"}

\* TODO: check that the heights are consistent
BlobMetaOK == /\ \/ Max({repositoryMeta.blobs[b].height: b \in Blobs}) <= clusterState.height
                    /\ repositoryMeta.repoStateId >= clusterState.repoStateId
                    \* Either we have a state that is in sync with the repo or the pointers in the state are zeroed out
                 \/ clusterState.wasMaster = FALSE
              /\ Cardinality({bl \in IndexBlobSet: repositoryMeta.blobs[bl].state = "UPLOADING"}) <= 1
              \* There should not be pending uploads from different heights, all uploads at a certain height must
              \* fail or complete before incrementing the height.
              /\ Cardinality({repositoryMeta.blobs[b].height: b \in {bl \in Blobs: repositoryMeta.blobs[bl].state = "UPLOADING"}}) <= 1
              \* All blobs marked as existing in the metadata exist
              /\ \A b \in Blobs: repositoryMeta.blobs[b].state = "DONE" => physicalBlobs[b] = TRUE
              \* No blobs exist that aren't tracked by the metadata
              /\ \A b \in Blobs: repositoryMeta.blobs[b].state = "NULL" => physicalBlobs[b] = FALSE


AllOK == TypeOK /\ BlobMetaOK /\ NoStaleBlobs

-----

AttemptAllSnapshots == <>[](outstandingSnapshots = {})

AllBlobsReachFinalState == <>[] (\A b \in Blobs:
                                    /\ repositoryMeta.blobs[b].state = "NULL"
                                        \/ repositoryMeta.blobs[b].state = "DONE")


-----

Next == \/ StartSnapshot
        \/ StartUploading
        \/ FinishOneUpload
        \/ FinishSnapshot
        \/ LoseClusterState
        \/ CleanupDanglingRepositoryState
        \/ RecoverStateAndHeight
        \/ ExecuteOneDelete

Spec == /\ Init
        /\ [][Next]_vars
        /\ SF_vars(StartSnapshot)
        /\ SF_vars(StartUploading)
        /\ SF_vars(FinishOneUpload)
        /\ SF_vars(ExecuteOneDelete)
        /\ SF_vars(FinishSnapshot)
        /\ SF_vars(CleanupDanglingRepositoryState)
        /\ WF_vars(RecoverStateAndHeight)
