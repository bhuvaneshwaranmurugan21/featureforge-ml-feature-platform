# Stage 3 Spark computation and incremental-backfill specification

## Authority and boundary

Part 2 Stage 1 is globally numbered Stage 3. It begins only from FeatureForge commit
`d7fa3b4a2d529dca4e8ee03b8eaf58637b583a12`, tree
`383e56b3ddbe0f1c4ab6faa7e72270c50684f306`. The stage is a bounded local Spark proof;
it does not authorize cloud infrastructure, deployment, serving, release, or production-scale claims.

## Temporal contract

The source is an append-only revision history. Exact duplicate revisions are idempotent. Reuse of a
revision identifier with different content, customer drift for a logical event, or competing revisions
at the same logical-event knowledge clock is ambiguous and rejected. For a requested knowledge
frontier, Spark first chooses the latest revision whose `knowledge_time <= knowledge_cutoff`. It then
removes retracted state, applies `event_time <= event_cutoff`, and applies the definition window as
`event_time > event_cutoff - window_seconds`. This ordering prevents future corrections from rewriting
historical knowledge and keeps the lower boundary open and upper boundary closed.

## Full build

Inputs are canonicalized definitions, revision histories, unique customer identities, and explicit
event/knowledge cutoffs. Five definitions are computed with Spark aggregations. Integer and floating
values occupy separate typed columns until conversion into `FeatureValue`; this avoids integer
precision loss through a common floating union. Canonical rows are sorted by customer and feature.
Their manifest binds the generation, cutoffs, definitions, full source history, customer set, row count,
row digest, and structural Spark diagnostics.

Because this is a local proof harness and canonical rows return to the driver for artifact binding, it
rejects more than 1,000,000 normalized input revisions or 100,000 customer-feature output rows. This
makes the collection boundary explicit instead of presenting a local driver path as production scale.

## Conservative affected scope

The planner compares authoritative logical-event state at the base and target knowledge frontiers. A
feature key is affected when either the prior or target state of a changed event can enter that
definition's business-time window. Considering both states is necessary for event-time corrections;
customer identity movement is ambiguous and rejected. False positives are safe; a false negative violates
the stage contract. A configured
affected-pair threshold selects a full rebuild. Ambiguous histories and corrupt or incompatible
predecessors are rejected rather than partially published.

## Incremental build

Spark recomputes only affected customers, then selects affected feature keys. Every unaffected value is
preserved from the predecessor but is re-created with the target generation identifier, target event
cutoff, and target knowledge cutoff. The resulting complete generation is accepted only when its
canonical rows equal an independently computed full rebuild. Evidence reports recomputed, preserved,
and avoided feature-pair counts; it does not convert those bounded counts into time, cost, or scale
claims.

## Immutable local artifact protocol

Rows and a manifest are written under a staging directory. A completion marker binds the manifest and
rows through a SHA-256 artifact digest, then the directory is atomically renamed to the generation
identity. Readers require exactly the manifest, rows, and marker and recalculate all bindings. An exact
replay is idempotent; reuse of a generation identity with different content, extra files, partial state,
or tampering fails closed.

## Diagnostics and limits

The proof records plan digest, input/authoritative/output row counts, partition counts, partition row
distribution, maximum partition fraction, and Cartesian-product detection. These are deterministic
structural diagnostics from bounded local workloads. Task durations, production extrapolation, and
performance SLOs are deliberately excluded.
