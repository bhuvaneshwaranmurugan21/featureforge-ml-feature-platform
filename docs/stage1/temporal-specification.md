# FeatureForge Part 1 Stage 1 temporal authority

**Status:** normative local contract

**Predecessor:** `ce2646754065145cbf609f286c588bbaa4fb3284`

**Requirements:** `FF-P1-03`, `FF-P1-04`, `FF-P1-05`

## Clocks and row cutoffs

`event_time` is the business-valid time of a logical payment. `knowledge_time` is the earliest
epoch second at which the platform could have known one immutable revision. A payment revision
is invalid when `knowledge_time < event_time`.

A training row has an `event_cutoff` and an explicit `prediction_knowledge_cutoff`.
`dataset_as_of` is the available extraction frontier. The effective knowledge cutoff is:

```text
min(prediction_knowledge_cutoff, dataset_as_of)
```

The row records all three values and a truncation flag. Using the minimum without recording the
requested prediction cutoff and truncation is forbidden.

## Eligibility and windows

A selected revision must satisfy both:

```text
event_time <= event_cutoff
knowledge_time <= effective_knowledge_cutoff
```

After revision selection, a finite trailing feature window is left-open and right-closed:

```text
event_cutoff - window_seconds < event_time <= event_cutoff
```

The lower boundary is excluded, the upper boundary is included, and an unbounded feature has no
lower bound. A feature window is not an arrival-lag target, TTL, freshness target, or retention
policy.

## Logical events, revisions, and replay

- `event_id` identifies one logical payment.
- `revision_id` identifies one immutable revision.
- Exact replay of the same revision ID and canonical content is idempotent.
- Reusing a revision ID for different content fails closed.
- Two distinct revisions for one event at the same `knowledge_time` are ambiguous and fail closed.
- Input/container order never breaks a tie.
- Customer identity is immutable across revisions of one logical event.
- Legacy v1 input deterministically derives `revision_id = event_id + "@" + knowledge_time` and
  `operation = "upsert"`; this compatibility mapping makes no broader schema-evolution claim.

## Corrections and retractions

A correction is a full `upsert` state with later knowledge time. It replaces the logical event
only for cutoffs at or after that knowledge time. Earlier rows retain the earlier knowable state.
A correction may change payload or event time.

A retraction is a typed `retract` tombstone with null amount, status, and risk. At cutoffs before
the tombstone it has no effect. At cutoffs at or after the tombstone, the logical event is absent.
Source history is never deleted, so an earlier row remains reconstructable.

## Defaults and definition identity

The empty/default policy is part of every definition digest:

| Computation | Default |
| --- | ---: |
| transaction count | `0` |
| successful spend | `0` |
| failed-payment ratio | `0.0` |
| hours since success | `null` |
| maximum merchant risk | `null` |

Changing a default, window, computation, type, TTL, description, name, or version changes the
definition digest. A changed semantic definition cannot reuse the prior result identity.

## Rejected alternatives

- Event-time-only joining leaks a correction learned after prediction.
- Input-order tie resolution makes results depend on file or partition ordering.
- A fake failed payment, negative amount, or null-filled upsert is not a retraction.
- TTL cannot stand in for offline history retention.
- Deleting a retracted source event destroys historical reconstructability.
- Silently substituting `dataset_as_of` for the requested prediction cutoff hides incomplete data.

## Proof boundary

Stage 1 proves these rules locally against hand-calculated fixtures, a structurally independent
test oracle, bounded enumeration, deterministic property tests, and negative controls. It does
not establish managed Spark/AWS parity, production scale, serving latency, cost, availability,
immutable source-snapshot provenance, or publication/recovery behavior.
