----------------------------- MODULE blobstore -----------------------------

EXTENDS Naturals, FiniteSets, Sequences, TLC

Snapshots == {"snapshot-A", "snapshot-B"}

AllSegments == {"path/to/shard/segment-X", "path/to/shard/segment-Y, path/to/shard/segment-Z"}

Segments == [
        Snapshots -> SUBSET(AllSegments)
    ]


IndexNBlobs == <<"index-1", "index-2", "index-3", "index-4">>

IndexBlobSet == {IndexNBlobs[i]: i \in 1..Len(IndexNBlobs)}

Blobs == IndexBlobSet \union {
    "snapshot-A", "snapshot-B"
} \union AllSegments

VARIABLES clusterState, repositoryMeta, outstandingSnapshots

Init == /\ clusterState = [
                repoStateId |-> 1,
                height |-> 1,
                snapshotInProgress |-> "NULL",
                snapshotDeletionInProgress |-> "NULL"
            ]
        /\ repositoryMeta = [repoStateId |-> 1, blobs |-> [b \in Blobs |-> [height |-> 0, state |-> "NULL"]]]
        /\ outstandingSnapshots = Snapshots

vars == <<clusterState, repositoryMeta, outstandingSnapshots>>

NextIndex == IF repositoryMeta.blobs[IndexNBlobs[1]].state = "NULL" THEN
                1
             ELSE
                IF repositoryMeta.blobs[IndexNBlobs[Len(IndexNBlobs)]].state = "DONE" THEN
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
                     /\ MarkUploads(clusterState.snapshotInProgress)
                  /\ clusterState' = [clusterState EXCEPT !.height = @ + 1, !.repoStateId = repositoryMeta.repoStateId + 1]
                  /\ UNCHANGED <<outstandingSnapshots>>

StartSnapshot == /\ clusterState.snapshotInProgress = "NULL"
                 /\ clusterState.snapshotDeletionInProgress = "NULL"
                 /\ \E s \in outstandingSnapshots:
                    /\ outstandingSnapshots' = outstandingSnapshots \ {s}
                    /\  clusterState' = [clusterState EXCEPT !.snapshotInProgress = s]
                /\ UNCHANGED <<repositoryMeta>>

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
                  /\ UNCHANGED <<clusterState, outstandingSnapshots>>

FinishSnapshot == /\ clusterState.snapshotInProgress /= "NULL"
                  /\ \A b \in Blobs : repositoryMeta.blobs[b].state \in {"DONE", "NULL"}
                  /\ repositoryMeta.blobs[clusterState.snapshotInProgress].state = "DONE"
                  /\ \E b \in Blobs : repositoryMeta.blobs[b].state = "DONE"
                  /\ clusterState' = [clusterState EXCEPT !.snapshotInProgress = "NULL"]
                  /\ UNCHANGED <<repositoryMeta, outstandingSnapshots>>

TypeOK == \A b \in Blobs: repositoryMeta.blobs[b].state \in {"NULL", "UPLOADING", "DONE", "DELETED"}

BlobMetaOK == /\ \A b \in Blobs: repositoryMeta.blobs[b].height <= clusterState.height
              /\ Cardinality({bl \in IndexBlobSet: repositoryMeta.blobs[bl].state = "UPLOADING"}) <= 1

AllOK == TypeOK /\ BlobMetaOK

Next == \/ StartSnapshot
        \/ StartUploading
        \/ FinishOneUpload
        \/ FinishSnapshot

Spec == Init /\ [][Next]_vars

=============================================================================
\* Modification History
\* Last modified Wed May 01 16:01:51 CEST 2019 by armin
\* Created Wed May 01 10:25:51 CEST 2019 by armin
