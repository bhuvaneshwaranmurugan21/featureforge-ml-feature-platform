# Stage 5 skeptical technical walkthrough

1. Start with immutable corrections and the event/knowledge frontier in the Stage 3 fixture. Run full and incremental Spark builds and compare canonical rows.
2. Open `src/featureforge/expected.py`; show its standard-library-only boundary and reconstruct the same five features from primitive inputs.
3. Materialize an isolated candidate. Inject one wrong value and show semantic plus generation-wide envelope/aggregate failure while the pointer remains unchanged.
4. Inspect the nine-field decision, ordered reasons, decision digest, persistence row, and activation-receipt binding. Demonstrate that a stale eligible decision still loses Stage 4 CAS.
5. Explain freshness equality versus expiry equality using controlled time; TTL deletion is not part of the correctness argument.
6. Follow a failure using the bounded event schema and diagnosis table without using customer identity as a label.
7. Recompute `benchmark-summary.json` from all 486 raw trials. Explain Spark startup dominance, fallback cases, variability, and why no cloud or production claim follows.
8. Run Stage 0–5 validators and regenerate every deterministic proof. The benchmark raw times need not repeat; their summary must reproduce exactly from the immutable raw file.

The repository demonstrates bounded platform semantics and auditability. It does not claim a deployed observability stack or live managed-service performance.
