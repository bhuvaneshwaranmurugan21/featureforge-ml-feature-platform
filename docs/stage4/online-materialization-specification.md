# Stage 4 online materialization and serving specification

## Verified boundary

Stage 4 proves a bounded local/CI online contract. The SQLite adapter is executable evidence for the declared state transitions. The DynamoDB boundary emits low-level request dictionaries that are validated against the pinned Botocore service model. No live AWS request is made, and the two adapters are not claimed to share physical behavior.

## Data model and access paths

An online value is one immutable item per entity, feature, definition, and generation:

- `PK = ENTITY#<customer_id>#GEN#<generation_id>`;
- `SK = FEATURE#<feature_name>#DEF#<definition_digest>`.

This makes the request's pinned generation part of every read address. One consistent DynamoDB query retrieves one entity's features for one generation. Candidate and pointer control items live below `CONTROL#<feature_set>` and are never selected by a timestamp or lexicographic generation maximum.

The alternative of bundling every feature into one entity item was rejected because it couples unrelated feature updates, increases conditional-write conflict scope, and creates a less useful per-feature digest boundary. The selected layout costs one write per feature. A future design must be reconsidered before item sizes, features per entity, or hot-partition concentration exceed the bounded envelope recorded in the capacity proof.

## Canonical representation

Record envelopes use UTF-8 canonical JSON with sorted keys and compact separators. Integer values remain integers. Finite floating values become normalized base-ten strings before DynamoDB `N` encoding. Null is an explicit tagged value and maps to DynamoDB `NULL`. Booleans, NaN, infinity, unknown definitions, mixed generations, duplicate entity-feature keys, empty generations, and mismatched value types fail before request construction.

Epoch times are non-negative integer seconds. `record_digest` binds the complete record body; `records_digest` binds the sorted list of record digests; `plan_digest` binds the generation, definition set, source/offline identities, operation, expected count, and aggregate. Validation and activation receipts bind their bodies in the same way.

## Candidate protocol

`ABSENT -> WRITING -> VALIDATED -> ACTIVE -> RETIRED` is the successful lifecycle. `WRITING` is invisible to readers. An exact materialization replay returns the durable receipt. A reused operation, generation, or record identity with different content is a conflict.

Reconciliation traverses deterministic keyset pages, compares the exact ordered key/digest set, verifies every stored record body, count, expected metadata, and aggregate, then commits the validation receipt. A partial, extra, or corrupt set remains activation-ineligible.

There is deliberately no cross-service transaction between the offline artifact and online store. Safety comes from immutable offline identities, isolated generation namespaces, complete reconciliation, and a receipt-bound pointer transition.

## Activation and serving

Activation is one logical compare-and-swap. Its production-shaped transaction condition-checks the validated candidate, conditionally creates or advances the active pointer from the expected generation/version, and writes an immutable operation/history item. The local adapter mirrors those observable outcomes in one SQLite transaction. Two independent connections racing from one pointer can produce only one committed transition.

A committed activation receipt is stored before acknowledgement. If acknowledgement is lost, an exact operation replay after restart returns that receipt without changing the pointer again.

A reader resolves the pointer once into a `ReaderToken` and uses its generation for every feature lookup. Retired generations remain readable for already-pinned requests. New requests observe the new pointer; neither request mixes generations.

## Eligibility and failure semantics

`ttl_epoch_seconds` is only an asynchronous deletion hint. A record is synchronously `EXPIRED` when `request_time >= expires_at`. It is `STALE` only when age is strictly greater than the configured maximum; equality remains eligible. No stale fallback is implicit.

Serving reports typed outcomes rather than converting failure to absence or a default. The precedence is versioned in `serving-decision-table.json`. Online expiry, pointer movement, and retirement do not modify Stage 3 offline artifacts.

## Retry and capacity boundary

Retries use a bounded attempt count, exponential growth, a maximum delay, and deterministic seeded jitter. Attempt identity and its confirmed effect count are durable; an exact replay is idempotent and a different replay conflicts. Unknown service error codes are not assumed retryable.

Capacity evidence is structural: canonical request bytes, item-limit headroom, item/request counts, logical partitions, features per entity, reconciliation work, and modeled retry amplification. It is not evidence of DynamoDB latency, throughput, throttling probability, adaptive capacity, cost, availability, or an SLO.
