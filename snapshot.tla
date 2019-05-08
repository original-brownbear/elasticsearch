----------------------------- MODULE blobstore -----------------------------

\* Simplified specification of the snapshot create and delete blob store interaction.
\* The specification assumes just a single metadata file per snapshot that references
\* all segment files belonging to the snapshot. The real repository structure by comparison
\* contains another two layers of hierarchy by organizing segments into shards and shards into indices.

EXTENDS Naturals, FiniteSets, Sequences, TLC

\* Number of distinct segment files that can be part of a snapshot.
CONSTANT SegmentCount

\* Number of snapshots to attempt to take.
CONSTANT SnapshotCount

CONSTANT NULL

Snapshots == 1..SnapshotCount

\* We test all possible mappings of snapshot names to segment files.
Segments == [Snapshots -> SUBSET(1..SegmentCount)]

Blobs == Snapshots \union SegmentCount

VARIABLES clusterState,
          outstandingSnapshots,
          physicalBlobs,
          segmentMap,
          master,
          dataNode,
          nextRepoMetaUUID

ExistStates == {"DONE", "UPLOADING"}
FinalStates == {"DONE"}

----
\* Utilities

Max(s) == CHOOSE x \in s : \A y \in s : x >= y

MaxOrZero(s) == IF s = {} THEN 0 ELSE CHOOSE x \in s : \A y \in s : x >= y

----
\* Initializations

InitialRepoStateId == [time |-> 1, uuid |-> 1]

EmptyClusterState == [repoStateId |-> InitialRepoStateId,
                      height |-> 1,
                      snapshotInProgress |-> NULL,
                      snapshotDeletionInProgress |-> NULL,
                      repoStateIdsToDelete |-> {}]

EmptyRepoMeta == [
    repoStateId |-> InitialRepoStateId,
    blobs       |-> {}]

EmptyMasterMemory == [unpublished |-> {},
                      uploaded |-> {},
                      nextSnapshotState |-> NULL,
                      networkBufferInbound |-> {},
                      networkBufferOutbound |-> {},
                      justDeleted |-> {},
                      justDeletedMeta   |-> {}, \* Blob meta state ids just deleted
                      repositoryMeta |-> NULL]

Init == \E segs \in Segments:
            /\ clusterState = EmptyClusterState
            /\ outstandingSnapshots = Snapshots
            /\ physicalBlobs = [
                    blobs      |-> {},
                    metaBlobs  |-> {EmptyRepoMeta}
                ]
            /\ segmentMap = segs
            /\ master = EmptyMasterMemory
            /\ dataNode = [pendingUploads |-> {},
                           readyUploads |-> {},
                           uploaded |-> {},
                           finishedUploads |-> {},
                           networkBufferInbound |-> {},
                           networkBufferOutbound |-> {}]
            /\ nextRepoMetaUUID = 2

vars == <<clusterState,
          outstandingSnapshots,
          physicalBlobs,
          segmentMap,
          master,
          dataNode,
          nextRepoMetaUUID>>

----
\* Utilities around the blobstore and blob meta handling

(*
   Creates a new SnapshotInfo blob.
*)
NewSnapshotInfo(snapshotId, res, indces) == [type |-> "SnapshotInfo",
                                             name |-> snapshotId,
                                             indices |-> indces]

(*
  Creates a new IndexShardSnapshot.
*)
NewIndexShardSnapshot(snapshotId, indx, sgments) == [type |-> "IndexShardSnapshot",
                                                     name |-> snapshotId,
                                                     index |-> indx,
                                                     segments |-> sgments]





EqualTypeAndName(a, b) == a.type = b.type /\ a.name = b.name

IndexBlobs(repoMeta) == {b \in repoMeta.blobs: b.type = "INDEX"}
SnapshotBlobs(repoMeta) == {b \in repoMeta.blobs: b.type = "SNAPSHOT"}

ValidIndexBlobs(repositoryMeta) == {i \in IndexBlobs(repositoryMeta): i.state = "DONE"}

HighestValidIndexBlob(repositoryMeta) == IF ValidIndexBlobs(repositoryMeta) = {} THEN
                                            NULL
                                         ELSE
                                            CHOOSE index \in
                                                ValidIndexBlobs(master.repositoryMeta):
                                                    index.name = Max(
                                                        {i.name:
                                                            i \in ValidIndexBlobs(master.repositoryMeta)})

SnapshotsInIndexBlob(indexBlobMeta) == IF indexBlobMeta = NULL THEN
                                        {}
                                       ELSE
                                       (CHOOSE blob \in physicalBlobs.blobs:
                                            blob.type = "INDEX" /\ blob.name = indexBlobMeta.name).snapshots

\* Get the next N for writing a new Index-N blob from meta data.
NextIndex(repoMeta) == IF IndexBlobs(repoMeta) = {} THEN
                1
             ELSE
                Max({i.name: i \in IndexBlobs(repoMeta)}) + 1

NextRepoStateId(repoId, height) == [uuid |-> nextRepoMetaUUID, time |-> repoId.time + 1 + height]

MaxHeightInRepo(repoMeta) == MaxOrZero({b.height :b \in repoMeta.blobs})

RemoveFromRepoMeta(repoMeta, removedBlobs) == [repoMeta EXCEPT
                                                    !.repoStateId = NextRepoStateId(repoMeta.repoStateId, clusterState.height),
                                                    !.blobs = {b \in @: ~\E m \in removedBlobs: EqualTypeAndName(m, b)}]

UpdateInRepoMeta(repoMeta, updates) == [repoMeta EXCEPT
                                                    !.repoStateId = NextRepoStateId(repoMeta.repoStateId, clusterState.height),
                                                    !.blobs = (@ \ {b \in @: \E n \in updates: EqualTypeAndName(b,n)}) \union updates]

ApplyToMaster(masterState, repoMeta, startedSnapshot) == [masterState EXCEPT
                                                !.repositoryMeta = repoMeta,
                                                !.unpublished = @ \union {repoMeta.repoStateId},
                                                \* Remove finished uploads from
                                                !.uploaded = {b \in @:
                                                    ~\E n \in repoMeta.blobs:
                                                        /\ EqualTypeAndName(b,n)
                                                        /\ n.state \in {"DONE", "DELETED"}},
                                                !.justDeleted = {d \in @:
                                                        \E b \in repoMeta.blobs: EqualTypeAndName(b, d)},
                                                !.nextSnapshotState =
                                                    IF startedSnapshot THEN
                                                        "STARTED"
                                                    ELSE
                                                        @
                                    ]

\* Create new repo meta with the given updates and move snapshot state in CS to started
\* iff startedSnapshot is TRUE.
UpdateRepoMeta(newBlobs, deletedBlobs, startedSnapshot) ==
                            LET newRepoMeta == UpdateInRepoMeta(RemoveFromRepoMeta(master.repositoryMeta, deletedBlobs), newBlobs)
                            IN
                                /\ master' = ApplyToMaster(master, newRepoMeta, startedSnapshot)
                                /\ physicalBlobs' = [physicalBlobs EXCEPT !.metaBlobs = @ \union {newRepoMeta}]
                                /\ nextRepoMetaUUID' = nextRepoMetaUUID + 1

NewUploadingBlob(blobName, blobType) ==
    [type |-> blobType, name |-> blobName, state |-> "UPLOADING", height |-> clusterState.height]

\* Mark all files resulting from the snapshot as "UPLOADING" in the metadata
MarkUploads ==
    UpdateRepoMeta(
       {NewUploadingBlob(clusterState.snapshotInProgress.name, "SNAPSHOT"),
        NewUploadingBlob(NextIndex(master.repositoryMeta), "INDEX")}
            \union {NewUploadingBlob(segmentId, "SEGMENT"):
                        segmentId \in segmentMap[clusterState.snapshotInProgress.name]},
    {},
    TRUE)

RollbackLastStep == UpdateRepoMeta({\* Rollback operations at current height on exist states
                                    IF blob.state \in ExistStates
                                        /\ blob.height = MaxHeightInRepo(master.repositoryMeta)
                                    THEN
                                        [blob EXCEPT !.state = "DELETED", !.height = MaxHeightInRepo(master.repositoryMeta)]
                                    ELSE
                                        blob
                                    : blob \in master.repositoryMeta.blobs
                                  }, {}, FALSE)

\* Can master update new the repository meta?
\* Master can only update it again if it is in sync with the repository and there isn't an update
\* to the blob meta pending that didn't yet get published to the CS.
CanUpdateRepoMeta == master.repositoryMeta /= NULL /\ master.unpublished = {}

UploadingBlobs(repoMeta) == {b \in repoMeta.blobs: b.state = "UPLOADING"}

DeletedBlobs == {b \in master.repositoryMeta.blobs: b.state = "DELETED"}

DoneBlobs == {b \in master.repositoryMeta.blobs.segmentBlobs: b.state = "DONE"}

\* Are there any pending uploads in the repository meta?
UploadsInProgress(repoMeta) == UploadingBlobs(repoMeta) /= {}

\* Are there any pending deletes/tombstones in the repository meta?
DeletesInProgress == DeletedBlobs /= {}

----
\* Cluster state related utilities.

SnapshotInProgress == clusterState.snapshotInProgress /= NULL

HasStartedSnapshot(cState) == /\ clusterState.snapshotInProgress /= NULL
                              /\ clusterState.snapshotInProgress.state = "STARTED"

(*
    Master can, after recovering from a loss of cluster state, get into the situation of having in-progress uploads
    in the recovered repository metadata but no ongoing snapshot.
*)
DanglingRepoStateOnMaster == /\ master.repositoryMeta /= NULL
                             /\ UploadsInProgress(master.repositoryMeta)
                             /\ SnapshotInProgress = FALSE

----
\* 1. State transitions for master node

----
HandleUploadRequest == /\ CanUpdateRepoMeta
                       /\ \E m \in master.networkBufferInbound:
                            master' = [master EXCEPT !.networkBufferInbound = @ \ {m}]

RespondToUploadRequest == /\ \E m \in master.networkBufferOutbound:
                             /\ master' = [master EXCEPT !.networkBufferOutbound = @ \ {m}]
                             /\ dataNode' = [dataNode EXCEPT !.networkBufferInbound = @ \union {m}]
----
\* Snapshot Steps

(*
    0.1. Move the snapshot state.
*)
MasterPublishesNextSnapshotState == /\ master.nextSnapshotState /= NULL
                                    /\ clusterState.repoStateIdsToDelete = {}
                                    /\ master' = [master EXCEPT !.nextSnapshotState = NULL]
                                    /\ clusterState' = [clusterState EXCEPT
                                                                    !.snapshotInProgress = [
                                                                        clusterState.snapshotInProgress EXCEPT
                                                                            !.state = master.nextSnapshotState
                                                                    ]
                                                ]
                                    /\ UNCHANGED <<outstandingSnapshots,
                                                   physicalBlobs,
                                                   segmentMap,
                                                   dataNode,
                                                   nextRepoMetaUUID>>

(*
   0.2. Upload one blob from master.
*)
MasterUploadsOneBlob == /\ CanUpdateRepoMeta
                        /\ clusterState.snapshotInProgress /= NULL
                        /\ clusterState.repoStateIdsToDelete = {}
                        /\ \E b \in {bl \in {[name |-> blob.name, type |-> blob.type]: blob \in UploadingBlobs(master.repositoryMeta)}: bl \notin master.uploaded}:
                            /\ master' = [master EXCEPT !.uploaded = @ \union {b}]
                            /\ physicalBlobs' = [physicalBlobs EXCEPT
                                                    !.blobs = @ \union {
                                                        IF b.type = "INDEX" THEN
                                                            [type |-> "INDEX",
                                                             name |-> b.name,
                                                             snapshots |-> (IF HighestValidIndexBlob(master.repositoryMeta) = NULL THEN
                                                                        {}
                                                                       ELSE
                                                                        SnapshotsInIndexBlob(HighestValidIndexBlob(master.repositoryMeta)))
                                                                            \union {clusterState.snapshotInProgress.name}]
                                                        ELSE
                                                            [type |-> b.type, name |-> b.name]}]
                        /\ UNCHANGED <<clusterState,
                                       segmentMap,
                                       dataNode,
                                       outstandingSnapshots,
                                       nextRepoMetaUUID>>

(*
    0.3 Finish a single upload from master by updating the repoMeta.
*)
FinishOneUpload == /\ CanUpdateRepoMeta
                   /\ clusterState.repoStateIdsToDelete = {}
                   /\ clusterState.snapshotInProgress /= NULL
                   /\ \E b \in master.uploaded:
                        /\ UpdateRepoMeta({IF blob.type = b.type /\ blob.name = b.name THEN
                                                [blob EXCEPT !.state = "DONE", !.height = clusterState.height]
                                           ELSE
                                                blob
                                           : blob \in master.repositoryMeta.blobs}, {}, FALSE)
                  /\ UNCHANGED <<clusterState,
                                 outstandingSnapshots,
                                 segmentMap,
                                 dataNode>>

(*
    1.1. Starting the snapshot process by adding the snapshot to the cluster state with state "INIT".
*)
StartSnapshot == /\ CanUpdateRepoMeta
                 /\ clusterState.repoStateIdsToDelete = {}
                 /\ UploadsInProgress(master.repositoryMeta) = FALSE
                 /\ DeletesInProgress = FALSE
                 /\ SnapshotInProgress = FALSE
                 /\ clusterState.snapshotDeletionInProgress = NULL
                 /\ \E s \in outstandingSnapshots:
                    /\  outstandingSnapshots' = outstandingSnapshots \ {s}
                    /\  clusterState' = [clusterState EXCEPT !.snapshotInProgress = [name |-> s, state |-> "INIT"], !.height = @ + 1]
                /\ UNCHANGED <<physicalBlobs,
                               segmentMap,
                               master,
                               dataNode,
                               nextRepoMetaUUID>>

(*
    1.2. Writes the new metadata blob containing all the changes that are required for the current snapshot.
*)
StartUploading == /\ CanUpdateRepoMeta
                  /\ clusterState.repoStateIdsToDelete = {}
                  /\ UploadsInProgress(master.repositoryMeta) = FALSE
                  /\ \A b \in master.repositoryMeta.blobs : b.state \in FinalStates
                  /\ clusterState.snapshotInProgress /= NULL
                  /\ clusterState.snapshotInProgress.state = "INIT"
                  /\ MarkUploads
                  /\ UNCHANGED <<clusterState,
                                 outstandingSnapshots,
                                 segmentMap,
                                 dataNode>>

(*
    1.3. Once all uploads have completed a snapshot in state "STARTED" is assumed
         to have completed and gets removed from the cluster state.
*)
FinishSnapshot == /\ CanUpdateRepoMeta
                  /\ clusterState.repoStateIdsToDelete = {}
                  /\ HasStartedSnapshot(clusterState)
                  /\ \A b \in master.repositoryMeta.blobs : b.state \in FinalStates
                  /\ clusterState' = [clusterState EXCEPT !.snapshotInProgress = NULL]
                  /\ UNCHANGED <<outstandingSnapshots,
                                 physicalBlobs,
                                 segmentMap,
                                 master,
                                 dataNode,
                                 nextRepoMetaUUID>>

------
\* Failure Modes

(*
    2.1. Losing the cluster state (i.e. restoring from scratch) by moving all CS entries back to defaults.
*)
LoseClusterState == /\ master.repositoryMeta /= NULL
                    /\ clusterState' = EmptyClusterState
                    /\ master' = EmptyMasterMemory
                    /\ UNCHANGED <<outstandingSnapshots,
                                   physicalBlobs,
                                   segmentMap,
                                   dataNode,
                                   nextRepoMetaUUID>>

(*
   2.2. Master failover.
*)
MasterFailOver == /\ master.repositoryMeta /= NULL
                  /\ master' = [EmptyMasterMemory EXCEPT !.repositoryMeta = CHOOSE metaState \in physicalBlobs.metaBlobs:
                                            metaState.repoStateId = clusterState.repoStateId]
                  /\ clusterState' = IF clusterState.snapshotInProgress = NULL THEN
                                       clusterState
                                     ELSE
                                       [clusterState EXCEPT !.snapshotInProgress = [clusterState.snapshotInProgress EXCEPT !.state = "ABORTED"]]
                  /\ UNCHANGED <<outstandingSnapshots,
                                 physicalBlobs,
                                 segmentMap,
                                 dataNode,
                                 nextRepoMetaUUID>>

--------

DeleteDanglingMetaBlobs == /\ clusterState.repoStateIdsToDelete /= {}
                           /\ master' = [master EXCEPT !.justDeletedMeta = clusterState.repoStateIdsToDelete]
                           /\ physicalBlobs' = [physicalBlobs EXCEPT
                                                    !.metaBlobs = { mb \in @:
                                                                        mb.repoStateId \notin clusterState.repoStateIdsToDelete}]
                           /\ UNCHANGED <<clusterState,
                                          outstandingSnapshots,
                                          segmentMap,
                                          dataNode,
                                          nextRepoMetaUUID>>

HandleAbortedSnapshot == /\ CanUpdateRepoMeta
                         /\ clusterState.repoStateIdsToDelete = {}
                         /\ clusterState.snapshotInProgress /= NULL /\ clusterState.snapshotInProgress.state = "ABORTED"
                         /\ clusterState' = [clusterState EXCEPT !.snapshotInProgress = NULL, !.height = @ + 1]
                         /\ UNCHANGED <<physicalBlobs,
                                        outstandingSnapshots,
                                        segmentMap,
                                        dataNode,
                                        master,
                                        nextRepoMetaUUID>>

RecoverStateAndHeight == LET repoMeta == CHOOSE x \in physicalBlobs.metaBlobs :
                                                    \A y \in physicalBlobs.metaBlobs :
                                                        /\ x.repoStateId.time >= y.repoStateId.time
                                                        /\ IF x.repoStateId.time = y.repoStateId.time THEN MaxHeightInRepo(x) >= MaxHeightInRepo(y) ELSE TRUE
                         IN
                         /\ master.repositoryMeta = NULL
                         /\ clusterState' = [clusterState EXCEPT
                                                !.height = MaxHeightInRepo(repoMeta),
                                                !.repoStateId = repoMeta.repoStateId]
                         /\ master' = [master EXCEPT !.repositoryMeta = repoMeta]
                         \* Simply delete all the meta blobs we're not going to use, no need to even register them in the cluster state.
                         \* This is safe since in practice thsi would happen via listing all blobs and then deleting so we would not delete
                         \* a blob newer than the one we select here if we were in fact a dangling master.
                         /\ physicalBlobs' = [physicalBlobs EXCEPT !.metaBlobs = {repoMeta}]
                         /\ UNCHANGED <<outstandingSnapshots,
                                        segmentMap,
                                        dataNode,
                                        nextRepoMetaUUID>>

(*
  Add a new repo meta blob that has all current uploading blobs (and DONE blobs at the same height) moved to DELETED.
*)
CleanupDanglingRepositoryState == /\ DanglingRepoStateOnMaster
                                  /\ clusterState.repoStateIdsToDelete = {}
                                  /\ CanUpdateRepoMeta
                                  /\ RollbackLastStep
                                  /\ UNCHANGED <<clusterState,
                                                 outstandingSnapshots,
                                                 segmentMap,
                                                 dataNode>>

RunDeletes == /\ CanUpdateRepoMeta
              \* Only run deletes for blobs when meta blobs have all been cleaned up
              /\ clusterState.repoStateIdsToDelete = {}
              /\ \E dels \in (SUBSET(DeletedBlobs) \ {}):
                            /\ physicalBlobs' = [physicalBlobs EXCEPT
                                                !.blobs = @ \
                                                            {b \in @:
                                                                \E d \in dels:
                                                                    b.type = d.type /\ b.name = d.name}]
                            /\ master' = [master EXCEPT !.justDeleted = @ \union {[name |-> d.name, type |-> d.type] : d \in dels}]
              /\ UNCHANGED <<clusterState,
                             outstandingSnapshots,
                             segmentMap,
                             dataNode,
                             nextRepoMetaUUID>>

FinishDeletes ==  /\ CanUpdateRepoMeta
                  /\ master.justDeleted /= {}
                  /\ UpdateRepoMeta({}, master.justDeleted, FALSE)
                  /\ UNCHANGED <<clusterState,
                                 outstandingSnapshots,
                                 segmentMap,
                                 dataNode>>

RemoveDeletedRepoStateIdsFromCS ==  /\ clusterState.repoStateIdsToDelete /= {}
                                    /\ master.justDeletedMeta /= {}
                                    /\ clusterState' = [clusterState EXCEPT !.repoStateIdsToDelete = {}]
                                    /\ master' = [master EXCEPT !.justDeletedMeta = {}]
                                    /\ UNCHANGED <<outstandingSnapshots,
                                                   physicalBlobs,
                                                   segmentMap,
                                                   dataNode,
                                                   nextRepoMetaUUID>>



\* Publish next repo state to CS.
PublishNextRepoStateToCS ==   /\ clusterState.repoStateIdsToDelete = {}
                              /\ \E s \in master.unpublished:
                                /\ clusterState' = [clusterState EXCEPT !.repoStateId = s,
                                                                        !.repoStateIdsToDelete = {clusterState.repoStateId},
                                                                        !.snapshotInProgress = IF master.nextSnapshotState = NULL THEN
                                                                                                   @
                                                                                                ELSE
                                                                                                  [@ EXCEPT !.state = master.nextSnapshotState]]
                                /\ master' = [master EXCEPT
                                              !.unpublished = {},
                                              !.nextSnapshotState = NULL]
                                /\ UNCHANGED <<outstandingSnapshots,
                                               physicalBlobs,
                                               segmentMap,
                                               dataNode,
                                               nextRepoMetaUUID>>

-----
\* Data Node Helpers

InProgressSegmentsOnDataNode == /\ dataNode.pendingUploads
                                /\ dataNode.readyUploads
                                /\ dataNode.uploaded
                                /\ dataNode.finishedUploads

-----
\* State transitions for data node

ApplyNewClusterStateOnDataNode ==  LET newSegments == clusterState.snapshotsInProgress.segments \ InProgressSegmentsOnDataNode /= {}
                                   IN
                                   /\ newSegments /= {}
                                   /\ dataNode' = [dataNode EXCEPT !.pendingUploads = @ \union newSegments]

----
\* Invariants

LegalBlobStates == {"UPLOADING", "DONE", "DELETED"}
LegalBlobTypes == {"INDEX", "SNAPSHOT", "SEGMENT"}

TypeOK == /\ \/ master.repositoryMeta = NULL
             \/ \A b \in master.repositoryMeta.blobs:
                /\ b.state \in LegalBlobStates
                /\ b.type \in LegalBlobTypes
                /\ DOMAIN b = {"type", "name", "height", "state"}
          /\ \A u \in (master.uploaded
                        \union
                       master.justDeleted): DOMAIN u = {"type", "name"}
          /\ \A d \in clusterState.repoStateIdsToDelete: DOMAIN d = {"uuid", "time"}
          /\ \A d \in master.justDeletedMeta: DOMAIN d = {"uuid", "time"}
          /\ master.nextSnapshotState \in {NULL, "STARTED"}
          /\ \A i \in {ind \in physicalBlobs.blobs: ind.type = "INDEX"}: i.snapshots \in SUBSET(Snapshots)

\* All segments must be referenced by snapshots, i.e. the set of all uploading or existing blobs
\* must be the same as the union of all snapshot's segments, snapshot-meta blobs and the root level
\* index blobs.

NoStaleOrMissingSnapshotBlobs(repoMeta) == \/ HighestValidIndexBlob(repoMeta) = NULL
                                           \/ \A snapshot \in SnapshotsInIndexBlob(HighestValidIndexBlob(repoMeta)):
                                                \E snb \in SnapshotBlobs(master.repositoryMeta): snb.name = snapshot

NoStaleBlobs == master.repositoryMeta /= NULL
                => /\ NoStaleOrMissingSnapshotBlobs(master.repositoryMeta)

NoBlobMetaIdCollisions == /\ \A x,y \in physicalBlobs.metaBlobs: x /= y => x.repoStateId.uuid /= y.repoStateId.uuid

BlobMetaOK == \* If master has recovered the repo meta state, the state must be correct
              /\ \/ master.repositoryMeta = NULL
                 \/ /\ MaxHeightInRepo(master.repositoryMeta) <= clusterState.height
                    /\ \A bl \in master.repositoryMeta.blobs:
                        bl.state = "DONE" => \E pb \in physicalBlobs.blobs: EqualTypeAndName(pb, bl)
                    /\ master.repositoryMeta.repoStateId.time >= clusterState.repoStateId.time
                    \* We should never be uploading two index blobs at the same time
                    /\ Cardinality({u \in UploadingBlobs(master.repositoryMeta): u.type = "INDEX"}) <= 1
                    \* There should not be pending uploads from different heights, all uploads at a certain height must
                    \* fail or complete before incrementing the height.
                    /\ \A x,y \in UploadingBlobs(master.repositoryMeta): x.height = y.height
                    \* No blobs exist that aren't tracked by the metadata
                    /\ \A xx \in physicalBlobs.blobs: \E yy \in master.repositoryMeta.blobs: EqualTypeAndName(xx,yy)

MasterOk == /\ master.repositoryMeta = NULL => master.unpublished = {}
            /\ master.repositoryMeta /= NULL
                => master.unpublished \in {{}, {master.repositoryMeta.repoStateId}}
            /\ clusterState.repoStateId \notin master.unpublished
            \* All blobs that master finished uploading exist as physical blobs and are tracked
            \* in the latest version of the metadata.
            /\ \A u \in master.uploaded:
                    /\ \E b \in master.repositoryMeta.blobs:
                        EqualTypeAndName(b, u) /\ b.state = "UPLOADING"
                    /\ (\E pb \in physicalBlobs.blobs: EqualTypeAndName(pb, u))
            /\ master.repositoryMeta /= NULL =>
                    /\ UploadingBlobs(master.repositoryMeta) = {} => master.uploaded = {}
                    /\ (\A b \in master.repositoryMeta.blobs : b.state \in FinalStates)
                            => (/\ master.uploaded = {}
                                /\ master.justDeleted = {})
            /\ master.nextSnapshotState = "STARTED" => clusterState.snapshotInProgress.state = "INIT"
            /\ HasStartedSnapshot(clusterState) => master.nextSnapshotState = NULL

              \* A segment handled by the data node can only be in one of the four stages of work
DataNodeOk == \A x,y \in {dataNode.pendingUploads, dataNode.readyUploads, dataNode.uploaded, dataNode.finishedUploads}:
                    /\ x \intersect y = {}

ClusterStateOk == Cardinality(clusterState.repoStateIdsToDelete) <= 1

SnapshotDoneOk == /\ HasStartedSnapshot(clusterState)
                    /\ \A b \in master.repositoryMeta.blobs : b.state \in FinalStates
                  => \E blob \in master.repositoryMeta.blobs:
                            /\ blob.type = "SNAPSHOT"
                            /\ blob.name = clusterState.snapshotInProgress.name
                            /\ blob.state = "DONE"

AllHistoryTracked == /\ master.repositoryMeta /= NULL => master.repositoryMeta \in physicalBlobs.metaBlobs

AllOK == /\ TypeOK
         /\ BlobMetaOK
         /\ NoStaleBlobs
         /\ MasterOk
         /\ DataNodeOk
         /\ AllHistoryTracked
         /\ NoBlobMetaIdCollisions
         /\ ClusterStateOk
         /\ SnapshotDoneOk

-----
\* Liveness Checks

\* Eventually all snapshots are attempted
AttemptAllSnapshots == <>[](outstandingSnapshots = {})

\* Eventually all blobs end up in a final state
AllBlobsReachFinalState == <>[](\A meta \in physicalBlobs.metaBlobs: \A b \in meta.blobs: b.state \in {NULL, "DONE"})

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
        \/ RunDeletes
        \/ FinishDeletes
        \/ HandleAbortedSnapshot
        \/ PublishNextRepoStateToCS
        \/ MasterPublishesNextSnapshotState
        \/ DeleteDanglingMetaBlobs
        \/ HandleUploadRequest
        \/ RespondToUploadRequest
        \/ RemoveDeletedRepoStateIdsFromCS

Spec == /\ Init
        /\ [][Next]_vars
        /\ SF_vars(StartSnapshot)
        /\ SF_vars(StartUploading)
        /\ SF_vars(FinishOneUpload)
        /\ SF_vars(RunDeletes)
        /\ SF_vars(FinishDeletes)
        /\ SF_vars(FinishSnapshot)
        /\ SF_vars(CleanupDanglingRepositoryState)
        /\ SF_vars(PublishNextRepoStateToCS)
        /\ SF_vars(MasterUploadsOneBlob)
        /\ SF_vars(HandleAbortedSnapshot)
        /\ SF_vars(MasterPublishesNextSnapshotState)
        /\ SF_vars(DeleteDanglingMetaBlobs)
        /\ SF_vars(RecoverStateAndHeight)
        /\ SF_vars(HandleUploadRequest)
        /\ SF_vars(RespondToUploadRequest)
        /\ SF_vars(RemoveDeletedRepoStateIdsFromCS)

====