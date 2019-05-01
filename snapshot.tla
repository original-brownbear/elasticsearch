----------------------------- MODULE blobstore -----------------------------

EXTENDS Naturals, FiniteSets, Sequences, TLC

Snapshots == {"snapshot-A", "snapshot-B"}

Segments == [
        snapshotA |-> {"path/to/shard/segment-X", "path/to/shard/segment-Y"},
        snapshotB |-> {"path/to/shard/segment-X", "path/to/shard/segment-Z"}
    ]


IndexNBlobs == <<"index-1", "index-2", "index-3", "index-4">>

IndexBlobSet == {IndexNBlobs[i]: i \in 1..Len(IndexNBlobs)}

Blobs == IndexBlobSet \union {
    "path/to/shard/index-1", "path/to/shard/index-2",
    "snapshot-A", "snapshot-B"
} \union Segments.snapshotA \union Segments.snapshotB

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
                CHOOSE i \in 1..Len(IndexNBlobs): \A j \in 1..Len(IndexNBlobs): i > j /\ repositoryMeta.blobs[IndexNBlobs[j]].state = "DONE"

MarkUploads(s) == repositoryMeta' = [
                                     repositoryMeta EXCEPT !.repoStateId = @ + 1,
                                    !.blobs = [
                                                blob \in Blobs |-> [height |-> clusterState.height,
                                                state |->
                                                    IF blob \in s THEN
                                                        "UPLOADING"
                                                    ELSE
                                                        IF blob = IndexNBlobs[NextIndex]
                                                        THEN
                                                           "UPLOADING"
                                                        ELSE
                                                            repositoryMeta.blobs[blob].state

                                                    ]]]

StartUploading == \A b \in Blobs : \/ repositoryMeta.blobs[b].state = "DONE"
                                   \/ repositoryMeta.blobs[b].state = "NULL"
                  /\ \/ clusterState.snapshotInProgress = "snapshot-A"
                        /\ MarkUploads(Segments.snapshotA)
                     \/ clusterState.snapshotInProgress = "snapshot-B"
                        /\ MarkUploads(Segments.snapshotB)
                  /\ clusterState' = [clusterState EXCEPT !.height = @ + 1, !.repoStateId = repositoryMeta.repoStateId + 1]
                  /\ UNCHANGED <<outstandingSnapshots>>

StartSnapshot == /\ clusterState.snapshotInProgress = "NULL"
                 /\ clusterState.snapshotDeletionInProgress = "NULL"
                 /\ \E s \in outstandingSnapshots:
                    /\ outstandingSnapshots' = outstandingSnapshots \ {s}
                    /\  clusterState' = [clusterState EXCEPT !.snapshotInProgress = s]
                /\ UNCHANGED <<repositoryMeta>>

TypeOK == \A b \in Blobs: repositoryMeta.blobs[b].state \in {"NULL", "UPLOADING", "DONE", "DELETED"}

BlobMetaOK == /\ \A b \in Blobs: repositoryMeta.blobs[b].height <= clusterState.height
              /\ Cardinality({bl \in IndexBlobSet: repositoryMeta.blobs[bl].state = "UPLOADING"}) <= 1

AllOK == TypeOK /\ BlobMetaOK

Next == \/ StartSnapshot
        \/ StartUploading

Spec == Init /\ [][Next]_vars

=============================================================================
\* Modification History
\* Last modified Wed May 01 14:12:09 CEST 2019 by armin
\* Created Wed May 01 10:25:51 CEST 2019 by armin
