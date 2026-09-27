# Stage 3 failure and repair log

1. Local Spark initially attempted to advertise a non-routable container address. The runtime was
   corrected at the root by explicitly binding and advertising `127.0.0.1`; the accepted runtime still
   uses real Java 17, PySpark 3.5.9, and Py4J 0.10.9.9.
2. The rehearsal preserved unaffected values with the predecessor envelope. That is semantically stale
   even when the feature value is unchanged. The implementation now constructs a new immutable value
   carrying the target generation, event cutoff, and knowledge cutoff.
3. The first mutation test removed the first conservatively affected key, which may be a deliberate
   false positive whose value did not change. The proof was repaired to identify the independently
   changed key set, assert it is a subset of planned scope, and then remove a truly changed key.
4. Immutable replay compared Python tuple/list representations after JSON restoration. The artifact
   protocol was repaired to compare the content-binding artifact digest; readers still recalculate the
   manifest, row, marker, and artifact bindings before replay is accepted.
5. The predecessor run exposed a stale legacy `evidence/local-simulation.json` pointer left from before
   Stage 1 added `empty_default` to definition identity. The current Stage 1 simulation was already
   authoritative and green. Stage 3 refreshes the legacy compatibility copy and its Stage 0 index rather
   than suppressing or ignoring the mismatch.
