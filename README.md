# Modist
## Conflicts, Consistency, and Clocks
### Test Cases
``TestVectorHappensBeforeBothEmpty`` tests input of two empty vectors for ``VectorHappensBefore``. 

``TestVectorHappensBeforeEqualVectors`` tests input of two equal vectors for ``VectorHappensBefore``.

``TestVectorHappensBeforeZeroAndEmptyVectors`` tests input of zero and empty input vector for ``VectorHappensBefore``.

``TestOnMessageReceive`` tests normal case of ``OnMessageReceive`` ensure its correct behavior.

``TestResolveConcurrentEvents`` tests normal case of ``ResolveConcurrentEvents`` with physical clock.

## Leaderless Replication
### Test Cases
``TestReadNonexistentKeys`` tests get non-existent keys

``TestGetUpToDate`` see if we could get the latest key back

``TestReadRepair`` test if read repair work

``TestConsistency`` test with quick write and read see if it is correct

## Partitioning
### Test Cases
``TestAddReplicaGroup`` tests add group

``TestRemoveReplicaGroup`` tests remove group

``TestLookup`` test lookup when there is no replica group

``TestConsistency`` test with quick write and read see if it is correct