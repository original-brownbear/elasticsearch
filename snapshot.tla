----------------------------- MODULE blobstore -----------------------------

EXTENDS Naturals, FiniteSets, Sequences, TLC

Snapshots == {"snapshot-A", "snapshot-B"}

AllSegments == {"path/to/shard/segment-X", "path/to/shard/segment-Y", "path/to/shard/segment-Z"}

Segments == [
        Snapshots -> SUBSET(AllSegments)
    ]


IndexNBlobs == <<"index-1", "index-2", "index-3", "index-4">>

StateNBlobs == {"blobmeta-1", "blobmeta-2", "blobmeta-3", "blobmeta-4"}

IndexBlobSet == {IndexNBlobs[i]: i \in 1..Len(IndexNBlobs)}

Blobs == IndexBlobSet \union Snapshots \union AllSegments

VARIABLES clusterState, repositoryMeta, outstandingSnapshots, physicalBlobs

----
\* Utilities

Max(s) == CHOOSE x \in s : \A y \in s : x >= y
----

Init == /\ clusterState = [
                repoStateId |-> 0,
                height |-> 0,
                snapshotInProgress |-> "NULL",
                snapshotDeletionInProgress |-> "NULL"
            ]
        /\ repositoryMeta = [repoStateId |-> 1, blobs |-> [b \in Blobs |-> [height |-> 0, state |-> "NULL"]]]
        /\ outstandingSnapshots = Snapshots
        /\ physicalBlobs = [b \in Blobs |-> FALSE]

vars == <<clusterState, repositoryMeta, outstandingSnapshots, physicalBlobs>>

NextIndex == IF repositoryMeta.blobs[IndexNBlobs[1]].state = "NULL" THEN
                1
             ELSE
                CHOOSE i \in 1..Len(IndexNBlobs): \E j \in 1..Len(IndexNBlobs):
                    /\ i = j + 1
                    /\ repositoryMeta.blobs[IndexNBlobs[j]].state = "DONE"
                    /\ repositoryMeta.blobs[IndexNBlobs[i]].state = "NULL"

MarkUploads(snapshotBlob) == \E segs \in Segments:
                                    /\ repositoryMeta' = [repositoryMeta EXCEPT
                                        !.repoStateId = @ + 1,
                                        !.blobs = [
                                                blob \in Blobs |-> [height |-> clusterState.height,
                                                state |->
                                                    IF blob \in segs[snapshotBlob] \/ blob = snapshotBlob THEN
                                                        "UPLOADING"
                                                    ELSE
                                                        IF blob = IndexNBlobs[NextIndex]
                                                        THEN
                                                           "UPLOADING"
                                                        ELSE
                                                            repositoryMeta.blobs[blob].state
                                                    ]]]


StartUploading == /\ \A b \in Blobs : repositoryMeta.blobs[b].state \in {"DONE", "NULL"}
                  /\ clusterState.snapshotInProgress \in Snapshots
                     /\ repositoryMeta.blobs[clusterState.snapshotInProgress].state = "NULL"
                     /\ MarkUploads(clusterState.snapshotInProgress)
                  /\ clusterState' = [clusterState EXCEPT !.height = @ + 1, !.repoStateId = repositoryMeta.repoStateId + 1]
                  /\ UNCHANGED <<outstandingSnapshots, physicalBlobs>>

StartSnapshot == /\ clusterState.snapshotInProgress = "NULL"
                 /\ clusterState.snapshotDeletionInProgress = "NULL"
                 /\ \E s \in outstandingSnapshots:
                    /\ outstandingSnapshots' = outstandingSnapshots \ {s}
                    /\  clusterState' = [clusterState EXCEPT !.snapshotInProgress = s]
                /\ UNCHANGED <<repositoryMeta, physicalBlobs>>

FinishOneUpload == \E b \in Blobs:
                        /\ repositoryMeta.blobs[b].state = "UPLOADING"
                        /\ repositoryMeta' = [
                                     repositoryMeta EXCEPT !.repoStateId = @ + 1,
                                     !.blobs = [
                                                blob \in Blobs |-> [height |-> clusterState.height,
                                                state |->
                                                    IF blob = b THEN
                                                        "DONE"
                                                    ELSE
                                                        repositoryMeta.blobs[blob].state
                                                    ]]]
                        /\ physicalBlobs' = [physicalBlobs EXCEPT ![b] = TRUE]
                  /\ UNCHANGED <<clusterState, outstandingSnapshots>>

FinishSnapshot == /\ clusterState.snapshotInProgress /= "NULL"
                  /\ \A b \in Blobs : repositoryMeta.blobs[b].state \in {"DONE", "NULL"}
                  /\ repositoryMeta.blobs[clusterState.snapshotInProgress].state = "DONE"
                  /\ clusterState' = [clusterState EXCEPT
                                     !.snapshotInProgress = "NULL",
                                     !.repoStateId = repositoryMeta.repoStateId]
                  /\ UNCHANGED <<repositoryMeta, outstandingSnapshots, physicalBlobs>>

LoseClusterState == /\ clusterState' = [
                            repoStateId |-> 0,
                            height |-> 0,
                            snapshotInProgress |-> "NULL",
                            snapshotDeletionInProgress |-> "NULL"
                        ]
                    /\ UNCHANGED <<repositoryMeta, outstandingSnapshots, physicalBlobs>>

StateIsEmpty == clusterState.height = 0 /\ clusterState.repoStateId = 0

RecoverStateAndHeight == /\ StateIsEmpty
                         /\ /\ clusterState' = [clusterState EXCEPT
                                        !.height = Max({repositoryMeta.blobs[b].state :b \in Blobs}),
                                        !.repoStateId = repositoryMeta.repoStateId]
                         /\ UNCHANGED <<repositoryMeta, outstandingSnapshots, physicalBlobs>>

TypeOK == \A b \in Blobs: repositoryMeta.blobs[b].state \in {"NULL", "UPLOADING", "DONE", "DELETED"}

BlobMetaOK == /\ \/ \A b \in Blobs: repositoryMeta.blobs[b].height <= clusterState.height
                    /\ repositoryMeta.repoStateId >= clusterState.repoStateId
                 \/ StateIsEmpty
              /\ Cardinality({bl \in IndexBlobSet: repositoryMeta.blobs[bl].state = "UPLOADING"}) <= 1
              \* There should not be pending uploads from different heights, all uploads at a certain height must
              \* fail or complete before incrementing the height.
              /\ Cardinality({repositoryMeta.blobs[b].height: b \in {bl \in Blobs: repositoryMeta.blobs[bl].state = "UPLOADING"}}) <= 1


AllOK == TypeOK /\ BlobMetaOK

Next == \/ StartSnapshot
        \/ StartUploading
        \/ FinishOneUpload
        \/ FinishSnapshot
        \/ LoseClusterState

Spec == Init /\ [][Next]_vars

=============================================================================
\* Modification History
\* Last modified Wed May 01 18:02:15 CEST 2019 by armin
\* Created Wed May 01 10:25:51 CEST 2019 by armin
