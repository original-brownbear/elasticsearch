----------------------------- MODULE blobstore -----------------------------

\* Simplified specification of the snapshot create and delete blob store interaction.
\* The specification assumes just a single metadata file per snapshot that references
\* all segment files belonging to the snapshot. The real repository structure by comparison
\* contains another two layers of hierarchy by organizing segments into shards and shards into indices.

EXTENDS Naturals, FiniteSets, Sequences, TLC

\* Set of all segment files that can be part of a snapshot (e.g. {"X", "Y", "Z"})
\* TODO: Model segments becoming unavailable due to updates/deletes
CONSTANT AllSegments

\* Set of all possible snapshots (e.g. {"SnapshotA", "SnapshotB"})
CONSTANT Snapshots

\* We test all possible mappings of snapshot names to segment files.
Segments == [Snapshots -> SUBSET(AllSegments)]

\* TODO: Just use a different structure here in the blob meta so we don't have to manually set any index-N blob names.
IndexNBlobs == <<"index-1", "index-2", "index-3", "index-4">>

IndexBlobSet == {IndexNBlobs[i]: i \in 1..Len(IndexNBlobs)}

Blobs == IndexBlobSet \union Snapshots \union AllSegments

VARIABLES clusterState,
          outstandingSnapshots,
          physicalBlobs,
          segmentMap,
          indexBlobContent,
          master,
          dataNode,
          \* All blobmeta contents ever written in order
          blobMetaStates

ExistStates == {"DONE", "UPLOADING"}
FinalStates == {"DONE", "NULL"}

----
\* Utilities

Max(s) == CHOOSE x \in s : \A y \in s : x >= y

----

EmptySnapshotInProgress == [name |-> "NULL", state |-> "NULL"]

EmptyClusterState == [
                        repoStateId |-> 1,
                        height |-> 1,
                        snapshotInProgress |-> EmptySnapshotInProgress,
                        snapshotDeletionInProgress |-> "NULL"
                ]

NoBlob == [height |-> 0, state |-> "NULL"]

EmptyRepoMeta == [repoStateId |-> 1, blobs |-> [b \in Blobs |-> NoBlob]]

EmptyMasterMemory == [
                        unpublished |-> {},
                        uploaded |-> {},
                        pendingUpload |-> {},
                        nextSnapshotState |-> "NULL",
                        hasPreviousState |-> FALSE,
                        tryCleanDanglingMeta |-> FALSE,
                        repositoryMeta |-> EmptyRepoMeta
                ]

Init == \E segs \in Segments:
            /\ clusterState = EmptyClusterState
            /\ outstandingSnapshots = Snapshots
            /\ physicalBlobs = [b \in Blobs |-> FALSE]
            /\ segmentMap = segs
            /\ indexBlobContent = [i \in IndexBlobSet |-> {}]
            /\ master = EmptyMasterMemory
            \* TODO: Implement
            /\ dataNode = [pendingUploads |-> {}, uploaded |-> {}]
            /\ blobMetaStates = {EmptyRepoMeta}

vars == <<clusterState,
          outstandingSnapshots,
          physicalBlobs,
          segmentMap,
          indexBlobContent,
          master,
          dataNode,
          blobMetaStates>>

----

\* Get the next N for writing a new Index-N blob
NextIndex == IF master.repositoryMeta.blobs[IndexNBlobs[1]].state = "NULL" THEN
                1
             ELSE
                CHOOSE i \in 1..Len(IndexNBlobs): \E j \in 1..Len(IndexNBlobs):
                    /\ i = j + 1
                    /\ master.repositoryMeta.blobs[IndexNBlobs[j]].state = "DONE"
                    /\ master.repositoryMeta.blobs[IndexNBlobs[i]].state = "NULL"

BestRepoMeta ==  CHOOSE x \in blobMetaStates : \A y \in blobMetaStates : x.repoStateId >= y.repoStateId


UpdateRepoMeta(newBlobs, startedSnapshot) ==
                            LET nextRepoState == clusterState.repoStateId + 1
                                newRepoMeta == [master.repositoryMeta EXCEPT
                                                !.repoStateId = nextRepoState,
                                                !.blobs = newBlobs]
                            IN
                                /\ master' = [master EXCEPT
                                    !.repositoryMeta = newRepoMeta,
                                    !.unpublished = @ \union {nextRepoState},
                                    !.uploaded =
                                        @ \ {b \in DOMAIN newBlobs: newBlobs[b].state = "DONE"},
                                    !.nextSnapshotState =
                                        IF startedSnapshot THEN
                                            "STARTED"
                                        ELSE
                                            master.nextSnapshotState
                                      ]
                                /\ blobMetaStates' = {meta \in (blobMetaStates \union {newRepoMeta}): meta.repoStateId >= clusterState.repoStateId}

\* Mark all files resulting from the snapshot as "UPLOADING" in the metadata
MarkUploads(snapshotBlob) ==
    UpdateRepoMeta([blob \in Blobs |->
                    IF  /\ master.repositoryMeta.blobs[blob].state /= "DONE"
                            /\ \/ blob \in segmentMap[snapshotBlob]
                               \/ blob = snapshotBlob
                               \/ blob = IndexNBlobs[NextIndex]
                    THEN
                        [state |-> "UPLOADING", height |-> clusterState.height]
                    ELSE
                        master.repositoryMeta.blobs[blob]
                  ],
                  TRUE)

RollbackLastStep == UpdateRepoMeta([blob \in Blobs |->
                                    IF master.repositoryMeta.blobs[blob].state \in ExistStates /\ master.repositoryMeta.blobs[blob].height = clusterState.height
                                    THEN
                                        [state |-> "DELETED", height |-> clusterState.height]
                                    ELSE
                                        master.repositoryMeta.blobs[blob]
                                  ], FALSE)

\* Can master update new the repository meta?
\* Master can only update it again if it is in sync with the repository and there isn't an update
\* to the blob meta pending that didn't yet get published to the CS
CanUpdateRepoMeta == master.hasPreviousState /\ master.unpublished = {}

MasterHasCleanRepo == master.hasPreviousState /\ master.tryCleanDanglingMeta = FALSE

UploadingBlobs == {b \in Blobs: master.repositoryMeta.blobs[b].state = "UPLOADING"}

DeletedBlobs == {bl \in Blobs: master.repositoryMeta.blobs[bl].state = "DELETED"}

DoneBlobs == {bl \in Blobs: master.repositoryMeta.blobs[bl].state = "DONE"}

\* Are there any pending uploads in the repository meta?
UploadsInProgress == UploadingBlobs /= {}

\* Are there any pending deletes/tombstones in the repository meta?
DeletesInProgress == DeletedBlobs /= {}

SnapshotInProgress == clusterState.snapshotInProgress /= EmptySnapshotInProgress

\* Aborts a snapshot by setting its statusw to ABORTED in the CS
AbortSnapshot == /\ SnapshotInProgress
                 /\ clusterState' = [clusterState EXCEPT
                                        !.snapshotInProgress =
                                            [clusterState.snapshotInProgress EXCEPT
                                                !.state = "ABORTED"]]

----

StartUploading == /\ MasterHasCleanRepo
                  /\ CanUpdateRepoMeta
                  /\ UploadsInProgress = FALSE
                  /\ \A b \in Blobs : master.repositoryMeta.blobs[b].state \in FinalStates
                  /\ clusterState.snapshotInProgress.name \in Snapshots
                     /\ clusterState.snapshotInProgress.state = "INIT"
                     /\ MarkUploads(clusterState.snapshotInProgress.name)
                  /\ indexBlobContent' = [indexBlobContent EXCEPT ![IndexNBlobs[NextIndex]] =
                        (IF NextIndex - 1 = 0 THEN {} ELSE indexBlobContent[IndexNBlobs[NextIndex - 1]]) \union {clusterState.snapshotInProgress.name}]
                  /\ UNCHANGED <<clusterState, outstandingSnapshots, physicalBlobs, segmentMap, dataNode>>

\* Starting the snapshot process by adding the snapshot to the cluster state.
StartSnapshot == /\ MasterHasCleanRepo
                 /\ UploadsInProgress = FALSE
                 /\ DeletesInProgress = FALSE
                 /\ SnapshotInProgress = FALSE
                 /\ clusterState.snapshotDeletionInProgress = "NULL"
                 /\ \E s \in outstandingSnapshots:
                    /\ outstandingSnapshots' = outstandingSnapshots \ {s}
                    /\  clusterState' = [clusterState EXCEPT !.snapshotInProgress = [name |-> s, state |-> "INIT"], !.height = @ + 1]
                /\ UNCHANGED <<physicalBlobs,
                               segmentMap,
                               indexBlobContent,
                               master,
                               dataNode,
                               blobMetaStates>>

MasterUploadsOneBlob == /\ MasterHasCleanRepo
                        /\ clusterState.snapshotInProgress.state = "STARTED"
                        /\ \E b \in master.pendingUpload:
                            /\ master' = [master EXCEPT !.pendingUpload = @ \ {b}, !.uploaded = @ \union {b}]
                            /\ physicalBlobs' = [physicalBlobs EXCEPT ![b] = TRUE]
                        /\ UNCHANGED <<clusterState,
                                       segmentMap,
                                       indexBlobContent,
                                       dataNode,
                                       outstandingSnapshots,
                                       blobMetaStates>>

MasterPublishesNextSnapshotState == /\ master.nextSnapshotState /= "NULL"
                                    /\ master' = [master EXCEPT !.nextSnapshotState = "NULL"]
                                    /\ clusterState' = [clusterState EXCEPT
                                                                    !.snapshotInProgress = [clusterState.snapshotInProgress EXCEPT
                                                                                                !.state = master.nextSnapshotState]]
                                    /\ UNCHANGED <<outstandingSnapshots,
                                                   physicalBlobs,
                                                   segmentMap,
                                                   indexBlobContent,
                                                   dataNode,
                                                   blobMetaStates>>

\* Finish a single upload. Modeled as a single step of updating the repository metablob and writing the file.
FinishOneUpload == /\ MasterHasCleanRepo
                   /\ CanUpdateRepoMeta
                   /\ clusterState.snapshotInProgress.state = "STARTED"
                   /\ \E b \in master.uploaded:
                        /\ UpdateRepoMeta([blob \in Blobs |->
                                           IF blob = b THEN
                                                [state |-> "DONE", height |-> master.repositoryMeta.blobs[blob].height]
                                           ELSE
                                                master.repositoryMeta.blobs[blob]
                                           ], FALSE)
                  /\ UNCHANGED <<clusterState,
                                 outstandingSnapshots,
                                 segmentMap,
                                 indexBlobContent,
                                 dataNode,
                                 physicalBlobs>>

FinishSnapshot == /\ MasterHasCleanRepo
                  /\ clusterState.snapshotInProgress.state = "STARTED"
                  /\ \A b \in Blobs : master.repositoryMeta.blobs[b].state \in FinalStates
                  /\ master.uploaded = {} /\ master.pendingUpload = {} /\ master.unpublished = {}
                  /\ master.repositoryMeta.blobs[clusterState.snapshotInProgress.name].state = "DONE"
                  /\ clusterState' = [clusterState EXCEPT !.snapshotInProgress = EmptySnapshotInProgress]
                  /\ UNCHANGED <<outstandingSnapshots,
                                 physicalBlobs,
                                 segmentMap,
                                 indexBlobContent,
                                 master,
                                 dataNode,
                                 blobMetaStates>>

\* Losing the cluster state (i.e. restoring from scratch) by moving all CS entries back to defaults
LoseClusterState == /\ master.hasPreviousState
                    /\ clusterState' = EmptyClusterState
                    /\ master' = EmptyMasterMemory
                    /\ UNCHANGED <<outstandingSnapshots,
                                   physicalBlobs,
                                   segmentMap,
                                   indexBlobContent,
                                   dataNode,
                                   blobMetaStates>>

MasterFailOver == /\ master.hasPreviousState
                  /\ master' = [EmptyMasterMemory EXCEPT
                                    !.tryCleanDanglingMeta = TRUE,
                                    !.repositoryMeta = CHOOSE metaState \in blobMetaStates: metaState.repoStateId = clusterState.repoStateId]
                  /\ AbortSnapshot
                  /\ UNCHANGED <<outstandingSnapshots,
                                 physicalBlobs,
                                 segmentMap,
                                 indexBlobContent,
                                 dataNode,
                                 blobMetaStates>>

CleanDanglingMeta == /\ master.hasPreviousState
                     /\ master.tryCleanDanglingMeta
                     /\ master.repositoryMeta.repoStateId = clusterState.repoStateId
                     /\ master' = [master EXCEPT !.tryCleanDanglingMeta = FALSE]
                     /\ blobMetaStates' = {master.repositoryMeta}
                     /\ UNCHANGED <<clusterState,
                                    outstandingSnapshots,
                                    physicalBlobs,
                                    segmentMap,
                                    indexBlobContent,
                                    dataNode>>

HandleAbortedSnapshot == /\ MasterHasCleanRepo
                         /\ clusterState.snapshotInProgress.state = "ABORTED"
                         /\ \/ UploadsInProgress
                                /\ RollbackLastStep
                                /\ UNCHANGED clusterState
                            \/ UploadsInProgress = FALSE
                                /\ clusterState' = [
                                    clusterState EXCEPT !.snapshotInProgress = EmptySnapshotInProgress]
                                /\ UNCHANGED <<blobMetaStates, master>>
                        /\ UNCHANGED <<outstandingSnapshots,
                                       physicalBlobs,
                                       segmentMap,
                                       indexBlobContent,
                                       dataNode>>

RecoverStateAndHeight == LET repoMeta == BestRepoMeta
                         IN
                         /\ master.hasPreviousState = FALSE
                         /\ clusterState' = [clusterState EXCEPT
                                        !.height = Max({repoMeta.blobs[b].height :b \in Blobs}),
                                        !.repoStateId = repoMeta.repoStateId]
                         /\ master' = [master EXCEPT !.hasPreviousState = TRUE, !.repositoryMeta = repoMeta]
                         /\ UNCHANGED <<outstandingSnapshots,
                                        physicalBlobs,
                                        segmentMap,
                                        indexBlobContent,
                                        dataNode,
                                        blobMetaStates>>

CleanupDanglingRepositoryState == /\ UploadsInProgress
                                  /\ SnapshotInProgress = FALSE
                                  /\ master.hasPreviousState
                                  /\ CanUpdateRepoMeta
                                  /\ RollbackLastStep
                                  /\ UNCHANGED <<clusterState,
                                                 outstandingSnapshots,
                                                 physicalBlobs,
                                                 segmentMap,
                                                 indexBlobContent,
                                                 dataNode>>

\* TODO: This is needlessly careful and models the worst case of having to delete one by one and trying to save delete calls.
\* In most repos the delete operation is (effectively) free and done in bulk.
\* Deletes and Updating the repo meta to reflect them is modeled as an atomic step. In the real world it isn't, but since deletes
\* are idempotent we can simply assume we would execute a delete repeatedly before writing a meta update.
ExecuteOneDelete == /\ CanUpdateRepoMeta
                    /\ DeletesInProgress
                    /\ UploadsInProgress = FALSE \* Physical deletes may only run after all uploads finished
                    /\ \E b \in DeletedBlobs:
                        /\ UpdateRepoMeta([blob \in Blobs |->
                                           IF blob = b THEN
                                                [state |-> "NULL", height |-> clusterState.height]
                                           ELSE
                                                master.repositoryMeta.blobs[blob]
                                          ], FALSE)
                        /\ physicalBlobs' = [physicalBlobs EXCEPT ![b] = FALSE]
                    /\ UNCHANGED <<clusterState,
                                   outstandingSnapshots,
                                   segmentMap,
                                   indexBlobContent,
                                   dataNode>>

\* Publish
PublishNextRepoStateId == /\ \E s \in master.unpublished:
                            /\ clusterState' = [clusterState EXCEPT !.repoStateId = s]
                            /\ master' = [master EXCEPT
                                            !.unpublished = @ \ {s},
                                            !.pendingUpload = @ \union UploadingBlobs]
                          /\ UNCHANGED <<outstandingSnapshots,
                                         physicalBlobs,
                                         segmentMap,
                                         indexBlobContent,
                                         dataNode,
                                         blobMetaStates>>

-----
\* Invariants
TypeOK == \A b \in Blobs: master.repositoryMeta.blobs[b].state \in {"NULL", "UPLOADING", "DONE", "DELETED"}

\* All segments must be referenced by snapshots, i.e. the set of all uploading or existing blobs
\* must be the same as the union of all snapshot's segments, snapshot-meta blobs and the root level
\* index blobs.
NoStaleBlobs == /\ {b \in Blobs: master.repositoryMeta.blobs[b].state \in ExistStates } =
                                    (UNION {
                                        segmentMap[s]: s \in {
                                            sn \in Snapshots: master.repositoryMeta.blobs[sn].state \in ExistStates}})
                                                \union {sn \in Snapshots: master.repositoryMeta.blobs[sn].state \in ExistStates}
                                                    \union {ib \in IndexBlobSet: master.repositoryMeta.blobs[ib].state \in ExistStates}
                \* There can be no dangling uploads from past heights
                /\ \A b \in Blobs:
                        master.repositoryMeta.blobs[b].height < clusterState.height => ~(b \in UploadingBlobs)

BlobMetaOK == /\ \/ master.hasPreviousState = FALSE
                 \/ Max({master.repositoryMeta.blobs[b].height: b \in Blobs}) <= clusterState.height
                    /\ master.repositoryMeta.repoStateId >= clusterState.repoStateId
                    \* Either we have a state that is in sync with the repo or the pointers in the state are zeroed out
                    /\ Cardinality(UploadingBlobs \intersect IndexBlobSet) <= 1
                    \* There should not be pending uploads from different heights, all uploads at a certain height must
                    \* fail or complete before incrementing the height.
                    /\ \A x,y \in UploadingBlobs: master.repositoryMeta.blobs[x].height = master.repositoryMeta.blobs[y].height
                    \* All blobs marked as existing in the metadata exist
                    /\ \A bl \in Blobs: master.repositoryMeta.blobs[bl].state = "DONE" => physicalBlobs[bl]
                    \* No blobs exist that aren't tracked by the metadata
                    /\ \A blo \in Blobs: master.repositoryMeta.blobs[blo].state = "NULL" => physicalBlobs[blo] = FALSE

MasterOk == master.unpublished \in {{}, {master.repositoryMeta.repoStateId}}

AllHistoryTracked == master.repositoryMeta \in (blobMetaStates \union {EmptyRepoMeta})

AllOK == TypeOK /\ BlobMetaOK /\ NoStaleBlobs /\ MasterOk /\ AllHistoryTracked

-----
\* Liveness Checks

\* Eventually all snapshots are attempted
AttemptAllSnapshots == <>[](outstandingSnapshots = {})

\* Eventually all blobs end up in a final state
AllBlobsReachFinalState == <>[](\A b \in Blobs: master.repositoryMeta.blobs[b].state \in {"NULL", "DONE"})

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
        \/ CleanDanglingMeta

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
        /\ SF_vars(CleanDanglingMeta)
        /\ WF_vars(RecoverStateAndHeight)
