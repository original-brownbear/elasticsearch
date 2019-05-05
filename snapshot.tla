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

CONSTANT NULL

\* We test all possible mappings of snapshot names to segment files.
Segments == [Snapshots -> SUBSET(AllSegments)]

Blobs == Snapshots \union AllSegments

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

EmptySnapshotInProgress == [name |-> NULL, state |-> NULL]

NullRepoStateId    == [time |-> 0, uuid |-> 0]
InitialRepoStateId == [time |-> 1, uuid |-> 1]

EmptyClusterState == [repoStateId |-> InitialRepoStateId,
                      height |-> 1,
                      snapshotInProgress |-> EmptySnapshotInProgress,
                      snapshotDeletionInProgress |-> NULL]

EmptyRepoMeta == [
    repoStateId |-> InitialRepoStateId,
    parent      |-> NullRepoStateId,  
    blobs       |-> {}]

EmptyMasterMemory == [unpublished |-> {},
                      uploaded |-> {},
                      pendingUpload |-> {},
                      nextSnapshotState |-> NULL,
                      hasPreviousState |-> FALSE,
                      tryCleanDanglingMeta |-> FALSE,
                      justDeleted |-> {},
                      repositoryMeta |-> EmptyRepoMeta]

Init == \E segs \in Segments:
            /\ clusterState = EmptyClusterState
            /\ outstandingSnapshots = Snapshots
            /\ physicalBlobs = [
                    blobs      |-> {},
                    metaBlobs  |-> {EmptyRepoMeta}
                ]
            /\ segmentMap = segs
            /\ master = EmptyMasterMemory
            \* TODO: Implement
            /\ dataNode = [pendingUploads |-> {}, uploaded |-> {}]
            /\ nextRepoMetaUUID = 2

vars == <<clusterState,
          outstandingSnapshots,
          physicalBlobs,
          segmentMap,
          master,
          dataNode,
          nextRepoMetaUUID>>

----
\* Utilities around the Blobstore

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

\* Get the next N for writing a new Index-N blob
NextIndex == IF IndexBlobs(master.repositoryMeta) = {} THEN
                1
             ELSE
                Max({i.name: i \in IndexBlobs(master.repositoryMeta)}) + 1

NextRepoStateId(repoId) == [uuid |-> nextRepoMetaUUID, time |-> repoId.time + 1]

MaxHeightInRepo(repoMeta) == MaxOrZero({b.height :b \in repoMeta.blobs}) 

\* Create new repo meta with the given updates and move snapshot state in CS to started 
\* iff startedSnapshot is TRUE.
UpdateRepoMeta(newBlobs, deletedBlobs, startedSnapshot) ==
                            LET nextRepoState == NextRepoStateId(clusterState.repoStateId)
                                newRepoMeta   == [master.repositoryMeta EXCEPT
                                                    !.repoStateId = nextRepoState,
                                                    !.blobs = (
                                                        (@ \ deletedBlobs) \ 
                                                            {b \in @: \E n \in newBlobs: 
                                                                EqualTypeAndName(b,n)}) 
                                                                    \union newBlobs]
                            IN
                                /\ master' = [master EXCEPT
                                                !.repositoryMeta = newRepoMeta,
                                                !.unpublished = @ \union {nextRepoState},
                                                !.uploaded = @ \{b \in @: 
                                                                    \E n \in newBlobs: 
                                                                        /\ EqualTypeAndName(b,n) 
                                                                        /\ n.state = "DONE"},
                                                !.justDeleted = @ \intersect newBlobs,
                                                !.tryCleanDanglingMeta = TRUE, 
                                                !.nextSnapshotState =
                                                    IF startedSnapshot THEN
                                                        "STARTED"
                                                    ELSE
                                                        master.nextSnapshotState
                                    ]
                                \* TODO don't assume write + deletes to be atomic, do one of the two first
                                /\ physicalBlobs' = [physicalBlobs EXCEPT !.metaBlobs = @ \union {newRepoMeta}]
                                /\ nextRepoMetaUUID' = nextRepoMetaUUID + 1

NewUploadingBlob(blobName, blobType) == [type |-> blobType, name |-> blobName, state |-> "UPLOADING", height |-> clusterState.height]

\* Mark all files resulting from the snapshot as "UPLOADING" in the metadata
MarkUploads ==
    UpdateRepoMeta(
       {NewUploadingBlob(clusterState.snapshotInProgress.name, "SNAPSHOT"),
        NewUploadingBlob(NextIndex, "INDEX")} \union 
            {NewUploadingBlob(segmentId,"SEGMENT"): 
                segmentId \in segmentMap[clusterState.snapshotInProgress.name]},
    {},
    TRUE)

RollbackLastStep == UpdateRepoMeta({\* Rollback operations at current height on exist states
                                    IF blob.state \in ExistStates
                                        /\ blob.height = clusterState.height
                                    THEN
                                        [blob EXCEPT !.state = "DELETED", !.height = clusterState.height]
                                    ELSE
                                        blob
                                    : blob \in master.repositoryMeta.blobs
                                  }, {}, FALSE)

MasterHasCleanRepo == master.hasPreviousState /\ master.tryCleanDanglingMeta = FALSE

\* Can master update new the repository meta?
\* Master can only update it again if it is in sync with the repository and there isn't an update
\* to the blob meta pending that didn't yet get published to the CS
CanUpdateRepoMeta == MasterHasCleanRepo /\ master.unpublished = {}

UploadingBlobs == {b \in master.repositoryMeta.blobs: b.state = "UPLOADING"}

DeletedBlobs == {b \in master.repositoryMeta.blobs: b.state = "DELETED"}

DoneBlobs == {b \in master.repositoryMeta.blobs.segmentBlobs: b.state = "DONE"}

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

StartUploading == /\ CanUpdateRepoMeta
                  /\ UploadsInProgress = FALSE
                  /\ \A b \in master.repositoryMeta.blobs : b.state \in FinalStates
                  /\ clusterState.snapshotInProgress.name \in Snapshots
                     /\ clusterState.snapshotInProgress.state = "INIT"
                     /\ MarkUploads
                  /\ UNCHANGED <<clusterState,
                                 outstandingSnapshots, 
                                 segmentMap, 
                                 dataNode>>

\* Starting the snapshot process by adding the snapshot to the cluster state.
StartSnapshot == /\ MasterHasCleanRepo
                 /\ UploadsInProgress = FALSE
                 /\ DeletesInProgress = FALSE
                 /\ SnapshotInProgress = FALSE
                 /\ clusterState.snapshotDeletionInProgress = NULL
                 /\ \E s \in outstandingSnapshots:
                    /\ outstandingSnapshots' = outstandingSnapshots \ {s}
                    /\  clusterState' = [clusterState EXCEPT !.snapshotInProgress = [name |-> s, state |-> "INIT"], !.height = @ + 1]
                /\ UNCHANGED <<physicalBlobs,
                               segmentMap,
                               master,
                               dataNode,
                               nextRepoMetaUUID>>

MasterUploadsOneBlob == /\ CanUpdateRepoMeta
                        /\ clusterState.snapshotInProgress.state = "STARTED"
                        /\ \E b \in master.pendingUpload:
                            /\ master' = [master EXCEPT !.pendingUpload = @ \ {b}, !.uploaded = @ \union {b}]
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

MasterPublishesNextSnapshotState == /\ master.nextSnapshotState /= NULL
                                    /\ master' = [master EXCEPT !.nextSnapshotState = NULL]
                                    /\ clusterState' = [clusterState EXCEPT
                                                                    !.snapshotInProgress = [clusterState.snapshotInProgress EXCEPT
                                                                                                !.state = master.nextSnapshotState]]
                                    /\ UNCHANGED <<outstandingSnapshots,
                                                   physicalBlobs,
                                                   segmentMap,
                                                   dataNode,
                                                   nextRepoMetaUUID>>

\* Finish a single upload. Modeled as a single step of updating the repository metablob and writing the file.
FinishOneUpload == /\ CanUpdateRepoMeta
                   /\ clusterState.snapshotInProgress.state = "STARTED"
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

FinishSnapshot == /\ CanUpdateRepoMeta
                  /\ clusterState.snapshotInProgress.state = "STARTED"
                  /\ \A b \in master.repositoryMeta.blobs : b.state \in FinalStates
                  /\ \E blob \in master.repositoryMeta.blobs:
                            /\ blob.type = "SNAPSHOT" 
                            /\ blob.name = clusterState.snapshotInProgress.name 
                            /\ blob.state = "DONE"
                  /\ clusterState' = [clusterState EXCEPT !.snapshotInProgress = EmptySnapshotInProgress]
                  /\ UNCHANGED <<outstandingSnapshots,
                                 physicalBlobs,
                                 segmentMap,
                                 master,
                                 dataNode,
                                 nextRepoMetaUUID>>

\* Losing the cluster state (i.e. restoring from scratch) by moving all CS entries back to defaults.
LoseClusterState == /\ master.hasPreviousState
                    /\ clusterState' = EmptyClusterState
                    /\ master' = EmptyMasterMemory
                    /\ UNCHANGED <<outstandingSnapshots,
                                   physicalBlobs,
                                   segmentMap,
                                   dataNode,
                                   nextRepoMetaUUID>>

MasterFailOver == /\ master.hasPreviousState
                  /\ master' = [EmptyMasterMemory EXCEPT
                                    !.tryCleanDanglingMeta = TRUE,
                                    !.repositoryMeta = CHOOSE metaState \in physicalBlobs.metaBlobs: 
                                        metaState.repoStateId = clusterState.repoStateId]
                  /\ AbortSnapshot
                  /\ UNCHANGED <<outstandingSnapshots,
                                 physicalBlobs,
                                 segmentMap,
                                 dataNode,
                                 nextRepoMetaUUID>>

CleanDanglingMeta == /\ master.hasPreviousState
                     /\ master.tryCleanDanglingMeta
                     /\ master.repositoryMeta.repoStateId = clusterState.repoStateId
                     /\ master' = [master EXCEPT !.tryCleanDanglingMeta = FALSE]
                     /\ physicalBlobs' = [physicalBlobs EXCEPT 
                                            !.metaBlobs = {
                                                m \in physicalBlobs.metaBlobs: 
                                                    m.repoStateId.time >= clusterState.repoStateId.time} 
                                                        \union {master.repositoryMeta}]
                     /\ UNCHANGED <<clusterState,
                                    outstandingSnapshots,
                                    segmentMap,
                                    dataNode,
                                    nextRepoMetaUUID>>

HandleAbortedSnapshot == /\ MasterHasCleanRepo
                         /\ clusterState.snapshotInProgress.state = "ABORTED"
                         /\ \/ UploadsInProgress
                                /\ RollbackLastStep
                                /\ UNCHANGED clusterState
                            \/ UploadsInProgress = FALSE
                                /\ clusterState' = [
                                    clusterState EXCEPT !.snapshotInProgress = EmptySnapshotInProgress]
                                /\ UNCHANGED <<master, 
                                               physicalBlobs, 
                                               nextRepoMetaUUID>>
                        /\ UNCHANGED <<outstandingSnapshots,
                                       segmentMap,
                                       dataNode>>

RecoverStateAndHeight == LET repoMeta == CHOOSE x \in physicalBlobs.metaBlobs : 
                                            \A y \in physicalBlobs.metaBlobs : 
                                                x.repoStateId.time >= y.repoStateId.time
                         IN
                         /\ master.hasPreviousState = FALSE
                         /\ clusterState' = [clusterState EXCEPT
                                        !.height = MaxHeightInRepo(repoMeta),
                                        !.repoStateId = repoMeta.repoStateId]
                         /\ master' = [master EXCEPT 
                                            !.hasPreviousState = TRUE,
                                            !.tryCleanDanglingMeta = TRUE, 
                                            !.repositoryMeta = repoMeta]
                         /\ UNCHANGED <<outstandingSnapshots,
                                        physicalBlobs,
                                        segmentMap,
                                        dataNode,
                                        nextRepoMetaUUID>>

CleanupDanglingRepositoryState == /\ UploadsInProgress
                                  /\ SnapshotInProgress = FALSE
                                  /\ master.hasPreviousState
                                  /\ CanUpdateRepoMeta
                                  /\ RollbackLastStep
                                  /\ UNCHANGED <<clusterState,
                                                 outstandingSnapshots,
                                                 segmentMap,
                                                 dataNode>>

RunDeletes == /\ CanUpdateRepoMeta
              /\ UploadsInProgress = FALSE
              /\ \E dels \in SUBSET(DeletedBlobs):
                /\ physicalBlobs' = [physicalBlobs EXCEPT 
                                        !.blobs = @ \ 
                                                    {b \in @: 
                                                        \E d \in dels: 
                                                            b.type = d.type /\ b.name = d.name}]
                /\ master' = [master EXCEPT !.justDeleted = @ \union dels]
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

\* Publish next repo state to CS.
PublishNextRepoStateId == /\ \E s \in master.unpublished:
                            /\ clusterState' = [clusterState EXCEPT !.repoStateId = s]
                            /\ master' = [master EXCEPT
                                            !.unpublished = {},
                                            !.pendingUpload = 
                                                {[type |-> b.type, name |-> b.name]: 
                                                    b \in UploadingBlobs} 
                                                        \ master.uploaded]
                          /\ UNCHANGED <<outstandingSnapshots,
                                         physicalBlobs,
                                         segmentMap,
                                         dataNode,
                                         nextRepoMetaUUID>>

-----

LegalBlobStates == {"UPLOADING", "DONE", "DELETED"}
LegalBlobTypes == {"INDEX", "SNAPSHOT", "SEGMENT"}

\* Invariants
TypeOK == /\ \A b \in master.repositoryMeta.blobs: 
                /\ b.state \in LegalBlobStates 
                /\ b.type \in LegalBlobTypes
                /\ DOMAIN b = {"type", "name", "height", "state"}
          /\ \A u \in master.uploaded: DOMAIN u = {"type", "name"}
          /\ \A p \in master.pendingUpload: DOMAIN p = {"type", "name"}

\* All segments must be referenced by snapshots, i.e. the set of all uploading or existing blobs
\* must be the same as the union of all snapshot's segments, snapshot-meta blobs and the root level
\* index blobs.

NoStaleOrMissingSnapshotBlobs == \/ HighestValidIndexBlob(master.repositoryMeta) = NULL
                                 \/ \A snapshot \in SnapshotsInIndexBlob(HighestValidIndexBlob(master.repositoryMeta)): 
                                        \E snb \in SnapshotBlobs(master.repositoryMeta): snb.name = snapshot

NoStaleBlobs == /\ NoStaleOrMissingSnapshotBlobs
                \* There can be no dangling uploads from past heights
                /\ \A b \in master.repositoryMeta.blobs:
                        b.height < clusterState.height => ~(b \in UploadingBlobs)

NoBlobMetaIdCollisions == /\ \A x,y \in physicalBlobs.metaBlobs: x /= y => x.repoStateId.uuid /= y.repoStateId.uuid 

BlobMetaOK == /\ \A bl \in master.repositoryMeta.blobs: bl.state = "DONE" => \E pb \in physicalBlobs.blobs: EqualTypeAndName(pb, bl)
              /\ \/ master.hasPreviousState = FALSE
                 \/ MaxHeightInRepo(master.repositoryMeta) <= clusterState.height
                    /\ master.repositoryMeta.repoStateId.time >= clusterState.repoStateId.time
                    \* Either we have a state that is in sync with the repo or the pointers in the state are zeroed out
                    /\ Cardinality({u \in UploadingBlobs: u.type = "INDEX"}) <= 1
                    \* There should not be pending uploads from different heights, all uploads at a certain height must
                    \* fail or complete before incrementing the height.
                    /\ \A x,y \in UploadingBlobs: x.height = y.height
                    \* All blobs marked as existing in the metadata exist
                    \* No blobs exist that aren't tracked by the metadata
                    \* TODO
              /\ \/ ~\E m \in physicalBlobs.metaBlobs: m.repoStateId.time < clusterState.repoStateId.time - 1
                 \/ master.tryCleanDanglingMeta

MasterOk == /\ master.unpublished \in {{}, {master.repositoryMeta.repoStateId}}
            /\ master.pendingUpload \intersect master.uploaded = {}
            /\ \A u \in master.uploaded: 
                    /\ \E b \in master.repositoryMeta.blobs: 
                        EqualTypeAndName(b, u) /\ b.state = "UPLOADING" 
                    /\ (\E pb \in physicalBlobs.blobs: EqualTypeAndName(pb, u))
            /\ master.pendingUpload /= {} => UploadingBlobs /= {}
            /\ UploadingBlobs = {} => (master.pendingUpload = {} /\ master.uploaded = {})
            /\ (\A b \in master.repositoryMeta.blobs : b.state \in FinalStates) => (master.uploaded = {} /\ master.pendingUpload = {} /\ master.justDeleted = {})
            /\ master.nextSnapshotState = "STARTED" => clusterState.snapshotInProgress.state = "INIT"
            /\ clusterState.snapshotInProgress.state = "STARTED" => master.nextSnapshotState = NULL 

AllHistoryTracked == master.repositoryMeta \in (physicalBlobs.metaBlobs \union {EmptyRepoMeta})

AllOK == /\ TypeOK 
         /\ BlobMetaOK 
         /\ NoStaleBlobs 
         /\ MasterOk 
         /\ AllHistoryTracked 
         /\ NoBlobMetaIdCollisions

-----
\* Liveness Checks

\* Eventually all snapshots are attempted
AttemptAllSnapshots == <>[](outstandingSnapshots = {})

\* Eventually all blobs end up in a final state
AllBlobsReachFinalState == <>[](\A b \in Blobs: master.repositoryMeta.blobs[b].state \in {NULL, "DONE"})

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
        \/ PublishNextRepoStateId
        \/ MasterPublishesNextSnapshotState
        \/ CleanDanglingMeta

Spec == /\ Init
        /\ [][Next]_vars
        /\ SF_vars(StartSnapshot)
        /\ SF_vars(StartUploading)
        /\ SF_vars(FinishOneUpload)
        /\ SF_vars(RunDeletes)
        /\ SF_vars(FinishDeletes)
        /\ SF_vars(FinishSnapshot)
        /\ SF_vars(CleanupDanglingRepositoryState)
        /\ SF_vars(PublishNextRepoStateId)
        /\ SF_vars(MasterUploadsOneBlob)
        /\ SF_vars(HandleAbortedSnapshot)
        /\ SF_vars(MasterPublishesNextSnapshotState)
        /\ SF_vars(CleanDanglingMeta)
        /\ SF_vars(RecoverStateAndHeight)

====
