# Stage 5 freshness and observability specification

Correctness clocks are injected integer logical times. Duration clocks are monotonic and may never determine eligibility. Wall time is descriptive only.

- Source arrival lag: `knowledge_time - event_time`.
- Build duration: monotonic build completion minus build start.
- Materialization duration: monotonic materialization completion minus start.
- Build/materialization availability lag: materialization completion tick minus source-frontier availability tick in the same injected lifecycle clock domain.
- Activation lag: activation commit time minus candidate validation time.
- Online freshness age: request time minus record knowledge frontier.
- TTL age: request time minus expiry; `request_time >= expires_at` is expired.
- End-to-end availability lag: request time minus selected source event time.

Negative identities, regressing required orderings, and non-finite observations fail closed. Staleness uses `age > maximum_freshness_age`; equality is eligible. Expiry takes effect at equality and is synchronous—background TTL deletion is never needed for correctness.

Telemetry uses `stage5-telemetry-event-v1`. Event names and label names are finite. Only `phase`, `profile`, `reason_code`, `size_class`, and `status` may be labels. Customer/entity/request identifiers and exception text are forbidden labels. Definition-set, source-frontier, generation, and operation identities are required trace fields rather than metric dimensions. Measurements must be finite. Emission describes a decision and cannot override it.

The catalog covers source admission, Spark build/scope, materialization/retry/reconciliation, parity, freshness, activation, serving, rollback, and retirement eligibility. The local evidence proves schema and diagnosis semantics, not a deployed metrics backend, alert delivery, dashboard, or production SLO.
