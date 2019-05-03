----------------------------- MODULE blobstore -----------------------------

\* Simplified specification of the snapshot create and delete blob store interaction.
\* The specification assumes just a single metadata file per snapshot that references
\* all segment files belonging to the snapshot. The real repository structure by comparison
\* contains another two layers of hierarchy by organizing segments into shards and shards into indices.

EXTENDS Naturals, FiniteSets, Sequences, TLC

\* Set of all segment files that can be part of a snapshot (e.g. {"X", "Y", "Z"})
\* TODO: Model segments becoming unavailable due to deletes
CONSTANT AllSegments

CONSTANT Snapshots

\* We test all possible mappings of snapshot names to segment files.
Segments == [Snapshots -> SUBSET(AllSegments)]

\* TODO: Just use a different structure here in the blob meta so we don't have to manually set any index-N blob names.
IndexNBlobs == <<"index-1", "index-2", "index-3", "index-4">>

IndexBlobSet == {IndexNBlobs[i]: i \in 1..Len(IndexNBlobs)}

Blobs == IndexBlobSet \union Snapshots \union AllSegments

VARIABLES clusterState,
          repositoryMeta,
          outstandingSnapshots,
          physicalBlobs,
          segmentMap,
          indexBlobContent,
          master,
          dataNode

ExistStates == {"DONE", "UPLOADING"}
FinalStates == {"DONE", "NULL"}

----
\* Utilities

Max(s) == CHOOSE x \in s : \A y \in s : x >= y

----

EmptySnapshotInProgress == [name |-> "NULL", state |-> "NULL"]

EmptyClusterState == [
                    repoStateId |-> 0,
                    height |-> 0,
                    snapshotInProgress |-> EmptySnapshotInProgress,
                    snapshotDeletionInProgress |-> "NULL",
                    wasMaster |-> FALSE
                ]

EmptyMasterMemory == [unpublished |-> {}, uploaded |-> {}, pendingUpload |-> {}, nextSnapshotState |-> "NULL"]

NoBlob == [height |-> 0, state |-> "NULL"]

Init == \E segs \in Segments:
            /\ clusterState = EmptyClusterState
            /\ repositoryMeta = [
                repoStateId |-> 1,
                blobs |-> [b \in Blobs |-> NoBlob]
               ]
            /\ outstandingSnapshots = Snapshots
            /\ physicalBlobs = [b \in Blobs |-> FALSE]
            /\ segmentMap = segs
            /\ indexBlobContent = [i \in IndexBlobSet |-> {}]
            /\ master = EmptyMasterMemory
            /\ dataNode = [pendingUploads |-> {}, uploaded |-> {}]

vars == <<clusterState,
          repositoryMeta,
          outstandingSnapshots,
          physicalBlobs,
          segmentMap,
          indexBlobContent,
          master,
          dataNode>>

----

\* Get the next N for writing a new Index-N blob
NextIndex == IF repositoryMeta.blobs[IndexNBlobs[1]].state = "NULL" THEN
                1
             ELSE
                CHOOSE i \in 1..Len(IndexNBlobs): \E j \in 1..Len(IndexNBlobs):
                    /\ i = j + 1
                    /\ repositoryMeta.blobs[IndexNBlobs[j]].state = "DONE"
                    /\ repositoryMeta.blobs[IndexNBlobs[i]].state = "NULL"

UpdateRepoMeta(newBlobs, startedSnapshot) ==
                            LET nextRepoState == clusterState.repoStateId + 1
                            IN
                                /\ repositoryMeta' = [
                                                repositoryMeta EXCEPT !.repoStateId = nextRepoState,
                                                !.blobs = newBlobs]
                                /\ master' = [master EXCEPT
                                    !.unpublished = @ \union {nextRepoState},
                                    !.uploaded = @ \ {b \in DOMAIN newBlobs: newBlobs[b].state = "DONE"},
                                    !.nextSnapshotState = IF startedSnapshot THEN "STARTED" ELSE master.nextSnapshotState]

\* Mark all files resulting from the snapshot as "UPLOADING" in the metadata
MarkUploads(snapshotBlob) ==
    UpdateRepoMeta([blob \in Blobs |->
                    IF  /\ repositoryMeta.blobs[blob].state /= "DONE"
                            /\ \/ blob \in segmentMap[snapshotBlob]
                               \/ blob = snapshotBlob
                               \/ blob = IndexNBlobs[NextIndex]
                    THEN
                        [state |-> "UPLOADING", height |-> clusterState.height]
                    ELSE
                        repositoryMeta.blobs[blob]
                  ],
                  TRUE)

RollbackLastStep == UpdateRepoMeta([blob \in Blobs |->
                                    IF repositoryMeta.blobs[blob].state \in ExistStates /\ repositoryMeta.blobs[blob].height = clusterState.height
                                    THEN
                                        [state |-> "DELETED", height |-> clusterState.height]
                                    ELSE
                                        repositoryMeta.blobs[blob]
                                  ], FALSE)

CanUpdateRepoMeta == clusterState.wasMaster /\ master.unpublished = {}

UploadingBlobs == {b \in Blobs: repositoryMeta.blobs[b].state = "UPLOADING"}

\* Are there any pending uploads in the repository meta?
UploadsInProgress == UploadingBlobs /= {}

\* Are there any pending deletes/tombstones in the repository meta?
DeletesInProgress == \E bl \in Blobs: repositoryMeta.blobs[bl].state = "DELETED"

SnapshotInProgress == clusterState.snapshotInProgress /= EmptySnapshotInProgress

AbortSnapshot == /\ SnapshotInProgress
                 /\ clusterState' = [clusterState EXCEPT
                                        !.snapshotInProgress =
                                            [clusterState.snapshotInProgress EXCEPT
                                                !.state = "ABORTED"]]

----

StartUploading == /\ CanUpdateRepoMeta
                  /\ UploadsInProgress = FALSE
                  /\ \A b \in Blobs : repositoryMeta.blobs[b].state \in FinalStates
                  /\ clusterState.snapshotInProgress.name \in Snapshots
                     /\ clusterState.snapshotInProgress.state = "INIT"
                     /\ MarkUploads(clusterState.snapshotInProgress.name)
                  /\ indexBlobContent' = [indexBlobContent EXCEPT ![IndexNBlobs[NextIndex]] =
                        (IF NextIndex - 1 = 0 THEN {} ELSE indexBlobContent[IndexNBlobs[NextIndex - 1]]) \union {clusterState.snapshotInProgress.name}]
                  /\ UNCHANGED <<clusterState, outstandingSnapshots, physicalBlobs, segmentMap, dataNode>>

\* Starting the snapshot process by adding the snapshot to the cluster state.
StartSnapshot == /\ clusterState.wasMaster
                 /\ UploadsInProgress = FALSE
                 /\ DeletesInProgress = FALSE
                 /\ SnapshotInProgress = FALSE
                 /\ clusterState.snapshotDeletionInProgress = "NULL"
                 /\ \E s \in outstandingSnapshots:
                    /\ outstandingSnapshots' = outstandingSnapshots \ {s}
                    /\  clusterState' = [clusterState EXCEPT !.snapshotInProgress = [name |-> s, state |-> "INIT"], !.height = @ + 1]
                /\ UNCHANGED <<repositoryMeta, physicalBlobs, segmentMap, indexBlobContent, master, dataNode>>

MasterUploadsOneBlob == /\ clusterState.wasMaster
                        /\ clusterState.snapshotInProgress.state = "STARTED"
                        /\ \E b \in master.pendingUpload:
                            /\ master' = [master EXCEPT !.pendingUpload = @ \ {b}, !.uploaded = @ \union {b}]
                            /\ physicalBlobs' = [physicalBlobs EXCEPT ![b] = TRUE]
                        /\ UNCHANGED <<clusterState, repositoryMeta, segmentMap, indexBlobContent, dataNode, outstandingSnapshots>>

MasterPublishesNextSnapshotState == /\ master.nextSnapshotState /= "NULL"
                                    /\ master' = [master EXCEPT !.nextSnapshotState = "NULL"]
                                    /\ clusterState' = [clusterState EXCEPT
                                                                    !.snapshotInProgress = [clusterState.snapshotInProgress EXCEPT
                                                                                                !.state = master.nextSnapshotState]]
                                    /\ UNCHANGED <<repositoryMeta,
                                                   outstandingSnapshots,
                                                   physicalBlobs,
                                                   segmentMap,
                                                   indexBlobContent,
                                                   dataNode>>

\* Finish a single upload. Modeled as a single step of updating the repository metablob and writing the file.
FinishOneUpload == /\ CanUpdateRepoMeta
                   /\ clusterState.snapshotInProgress.state = "STARTED"
                   /\ \E b \in master.uploaded:
                        /\ UpdateRepoMeta([blob \in Blobs |->
                                           IF blob = b THEN
                                                [state |-> "DONE", height |-> repositoryMeta.blobs[blob].height]
                                           ELSE
                                                repositoryMeta.blobs[blob]
                                           ], FALSE)
                  /\ UNCHANGED <<clusterState, outstandingSnapshots, segmentMap, indexBlobContent, dataNode, physicalBlobs>>

FinishSnapshot == /\ clusterState.wasMaster
                  /\ clusterState.snapshotInProgress.state = "STARTED"
                  /\ \A b \in Blobs : repositoryMeta.blobs[b].state \in FinalStates
                  /\ repositoryMeta.blobs[clusterState.snapshotInProgress.name].state = "DONE"
                  /\ clusterState' = [clusterState EXCEPT !.snapshotInProgress = "NULL"]
                  /\ UNCHANGED <<repositoryMeta,
                                 outstandingSnapshots,
                                 physicalBlobs,
                                 segmentMap,
                                 indexBlobContent,
                                 master,
                                 dataNode>>

\* Losing the cluster state (i.e. restoring from scratch) by moving all CS entries back to defaults
LoseClusterState == /\ clusterState.wasMaster
                    /\ clusterState' = EmptyClusterState
                    /\ master' = EmptyMasterMemory
                    /\ UNCHANGED <<repositoryMeta,
                                   outstandingSnapshots,
                                   physicalBlobs,
                                   segmentMap,
                                   indexBlobContent,
                                   dataNode>>

MasterFailOver == /\ master' = EmptyMasterMemory
                  /\ AbortSnapshot
                  /\ UNCHANGED <<outstandingSnapshots,
                                 repositoryMeta,
                                 physicalBlobs,
                                 segmentMap,
                                 indexBlobContent,
                                 dataNode>>

HandleAbortedSnapshot == /\ clusterState.wasMaster
                         /\ clusterState.snapshotInProgress.state = "ABORTED"
                         /\ \/ UploadsInProgress
                                /\ RollbackLastStep
                                /\ UNCHANGED clusterState
                            \/ UploadsInProgress = FALSE
                                /\ clusterState' = [
                                    clusterState EXCEPT !.snapshotInProgress = EmptySnapshotInProgress]
                                /\ UNCHANGED <<repositoryMeta, master>>
                        /\ UNCHANGED <<outstandingSnapshots,
                                       physicalBlobs,
                                       segmentMap,
                                       indexBlobContent,
                                       dataNode>>

RecoverStateAndHeight == /\ clusterState.wasMaster = FALSE
                         /\ clusterState' = [clusterState EXCEPT
                                        !.height = Max({repositoryMeta.blobs[b].height :b \in Blobs}),
                                        !.repoStateId = repositoryMeta.repoStateId,
                                        !.wasMaster = TRUE]
                         /\ UNCHANGED <<repositoryMeta,
                                        outstandingSnapshots,
                                        physicalBlobs,
                                        segmentMap,
                                        indexBlobContent,
                                        master,
                                        dataNode>>

CleanupDanglingRepositoryState == /\ UploadsInProgress
                                  /\ SnapshotInProgress = FALSE
                                  /\ clusterState.wasMaster
                                  /\ RollbackLastStep
                                  /\ UNCHANGED <<clusterState,
                                                 outstandingSnapshots,
                                                 physicalBlobs,
                                                 segmentMap,
                                                 indexBlobContent,
                                                 dataNode>>

\* TODO: This is needlessly careful and models the worst case of having to delete one by one and trying to save delete calls.
\* In most repos the delete operation is (effectively) free and done in bulk.
ExecuteOneDelete == /\ CanUpdateRepoMeta
                    /\ DeletesInProgress
                    /\ UploadsInProgress = FALSE \* Physical deletes may only run after all uploads finished
                    /\ \E b \in {bl \in Blobs: repositoryMeta.blobs[bl].state = "DELETED"}:
                        /\ UpdateRepoMeta([blob \in Blobs |->
                                           IF blob = b THEN
                                                [state |-> "NULL", height |-> clusterState.height]
                                           ELSE
                                                repositoryMeta.blobs[blob]
                                          ], FALSE)
                        /\ physicalBlobs' = [physicalBlobs EXCEPT ![b] = FALSE]
                    /\ UNCHANGED <<clusterState,
                                   outstandingSnapshots,
                                   segmentMap,
                                   indexBlobContent,
                                   dataNode>>

PublishNextRepoStateId == /\ \E s \in master.unpublished:
                            /\ clusterState' = [clusterState EXCEPT !.repoStateId = s]
                            /\ master' = [master EXCEPT
                                            !.unpublished = @ \ {s},
                                            !.pendingUpload = @ \union {b \in Blobs: repositoryMeta.blobs[b].state = "UPLOADING"}]
                          /\ UNCHANGED <<repositoryMeta,
                                         outstandingSnapshots,
                                         physicalBlobs,
                                         segmentMap,
                                         indexBlobContent,
                                         dataNode>>

-----

TypeOK == \A b \in Blobs: repositoryMeta.blobs[b].state \in {"NULL", "UPLOADING", "DONE", "DELETED"}

\* All segments must be referenced by snapshots, i.e. the set of all uploading or existing blobs
\* must be the same as the union of all snapshot's segments, snapshot-meta blobs and the root level
\* index blobs.
NoStaleBlobs == /\ {b \in Blobs: repositoryMeta.blobs[b].state \in ExistStates } =
                                    (UNION {
                                        segmentMap[s]: s \in {
                                            sn \in Snapshots: repositoryMeta.blobs[sn].state \in ExistStates}})
                                                \union {sn \in Snapshots: repositoryMeta.blobs[sn].state \in ExistStates}
                                                    \union {ib \in IndexBlobSet: repositoryMeta.blobs[ib].state \in ExistStates}
                /\ \A b \in Blobs:
                        repositoryMeta.blobs[b].height < clusterState.height => repositoryMeta.blobs[b].state /= "UPLOADING"

BlobMetaOK == /\ \/ Max({repositoryMeta.blobs[b].height: b \in Blobs}) <= clusterState.height
                    /\ repositoryMeta.repoStateId >= clusterState.repoStateId
                    \* Either we have a state that is in sync with the repo or the pointers in the state are zeroed out
                 \/ clusterState.wasMaster = FALSE
              /\ Cardinality(UploadingBlobs \intersect IndexBlobSet) <= 1
              \* There should not be pending uploads from different heights, all uploads at a certain height must
              \* fail or complete before incrementing the height.
              /\ \A x,y \in UploadingBlobs: repositoryMeta.blobs[x].height = repositoryMeta.blobs[y].height
              \* All blobs marked as existing in the metadata exist
              /\ \A b \in Blobs: repositoryMeta.blobs[b].state = "DONE" => physicalBlobs[b]
              \* No blobs exist that aren't tracked by the metadata
              /\ \A b \in Blobs: repositoryMeta.blobs[b].state = "NULL" => physicalBlobs[b] = FALSE

MasterOk == master.unpublished \in {{}, {repositoryMeta.repoStateId}}

AllOK == TypeOK /\ BlobMetaOK /\ NoStaleBlobs /\ MasterOk

-----

\* Eventually all snapshots are attempted
AttemptAllSnapshots == <>[](outstandingSnapshots = {})

\* Eventually all blobs end up in a final state
AllBlobsReachFinalState == <>[](\A b \in Blobs: repositoryMeta.blobs[b].state \in {"NULL", "DONE"})

-----

Next == \/ StartSnapshot
        \/ StartUploading
        \/ FinishOneUpload
        \/ FinishSnapshot
        \/ LoseClusterState
        \/ MasterFailOver
        \/ MasterUploadsOneBlob
        \/ CleanupDanglingRepositoryState
        \/ RecoverStateAndHeight
        \/ ExecuteOneDelete
        \/ HandleAbortedSnapshot
        \/ PublishNextRepoStateId
        \/ MasterPublishesNextSnapshotState

Spec == /\ Init
        /\ [][Next]_vars
        /\ SF_vars(StartSnapshot)
        /\ SF_vars(StartUploading)
        /\ SF_vars(FinishOneUpload)
        /\ SF_vars(ExecuteOneDelete)
        /\ SF_vars(FinishSnapshot)
        /\ SF_vars(CleanupDanglingRepositoryState)
        /\ SF_vars(PublishNextRepoStateId)
        /\ SF_vars(MasterUploadsOneBlob)
        /\ SF_vars(HandleAbortedSnapshot)
        /\ SF_vars(MasterPublishesNextSnapshotState)
        /\ WF_vars(RecoverStateAndHeight)

