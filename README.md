# Modist
## Conflicts, Consistency, and Clocks
### Test Cases
``TestVectorHappensBeforeBothEmpty`` tests input of two empty vectors for ``VectorHappensBefore``.

``TestVectorHappensBeforeEqualVectors`` tests input of two equal vectors for ``VectorHappensBefore``.

``TestVectorHappensBeforeZeroAndEmptyVectors`` tests input of zero and empty input vector for ``VectorHappensBefore``.

``TestOnMessageReceive`` tests normal case of ``OnMessageReceive`` ensure its correct behavior.

``TestResolveConcurrentEvents`` tests normal case of ``ResolveConcurrentEvents`` with physical clock.