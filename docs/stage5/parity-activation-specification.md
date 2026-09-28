# Stage 5 parity and activation specification

Stage 5 closes a bounded local/CI platform behavior: primitive immutable revisions and definitions are independently projected into expected feature rows and online envelopes, then compared with the Spark and online paths. `src/featureforge/expected.py` is standard-library-only and its import guard rejects any FeatureForge dependency. This prevents a shared production helper from certifying its own defect.

Bounded candidates use exhaustive comparison. The comparator checks key membership, typed value, definition digest, event and knowledge frontiers, generation, source digest, expiry, complete record envelope, and ordered aggregate digest. Its stable failures are `MISSING_RECORD`, `EXTRA_RECORD`, `SEMANTIC_MISMATCH`, `ENVELOPE_MISMATCH`, `INELIGIBLE_RECORD`, and `AGGREGATE_MISMATCH`. Deterministic stratified samples are diagnostic only; they use seed 73 and explicit hot, sparse, boundary, and ordinary strata. A full-set aggregate mismatch still detects an out-of-sample mutation.

Activation requires the conjunction of nine immutable receipt-backed facts, evaluated in this order: source completeness, manifest validity, offline validity, online reconciliation, independent parity, freshness, candidate consistency, uniqueness/no conflicting operation, and pointer currency. A decision is `ELIGIBLE` only when all nine are true. Otherwise all failed reason codes are retained in order.

The decision is content-addressed and persisted before publication. Eligible activation binds `policy_decision_digest` into the existing Stage 4 activation receipt. The Stage 4 compare-and-swap remains the final authority, so a decision that becomes stale after evaluation cannot publish. Exact replay returns the durable receipt without a second pointer transition. `LocalOnlineStore.activate` remains the lower-level Stage 4 primitive; Stage 5 callers must use `activate_eligible`/`activate_with_decision`.

Sampling cannot prove that an unsampled individual record is semantically correct. It is never the activation authority in the bounded Stage 5 workload. No AWS or DynamoDB runtime behavior is inferred from the SQLite contract adapter.
