# FeatureForge Part 1 Stage 2 lifecycle specification

## Scope and authority

Stage 2 proves a bounded local lifecycle in Python and file-backed SQLite. Stage 1 remains the temporal authority: event time determines business eligibility, knowledge time determines what was knowable, finite windows are left-open/right-closed, and corrections or typed retractions create later knowledge rather than rewriting earlier prediction inputs.

This specification does not claim Spark, DynamoDB, AWS, distributed-system, scale, latency, availability, durability, or cost behavior.

## Immutable data plane

Accepted source events use `payment-event-v2`; labels use `training-request-v1`. Decoding rejects unknown or missing fields, booleans masquerading as integers, invalid clocks, payload-bearing retractions, reused revision identities, same-clock ambiguity, and customer reassignment. A batch is decoded and normalized before it is persisted, so a malformed row cannot create a partial snapshot.

An artifact identity is scoped by `(kind, artifact_id)`. Its manifest binds contract, kind, metadata, row count, canonical ordered-row digest, and manifest digest. The enclosing artifact digest domain-separates the manifest and rows with `featureforge-artifact-v1`. Exact replay is idempotent; different content under an existing identity is rejected. Reads recalculate counts and digests so direct or accidental storage corruption fails closed.

Definitions are immutable by `name:vN` and by complete definition digest. The digest includes computation, type, TTL, window, default, and description. The persistent authority is reopened during proof; memory is not the authority. A sorted duplicate-free definition-set artifact binds all definitions.

The training-dataset manifest binds source snapshot, label snapshot, definition set, declared code tree, the Stage 1 production manifest, row cutoffs, count, ordered rows digest, and its own digest. The current serving view is separately materialized at explicit event and knowledge cutoffs; it is never inferred from the latest training row.

## Generation manifest and validation

A generation manifest has one exact versioned shape. It binds the source, label, definition-set, dataset, code, event cutoff, knowledge cutoff, expected predecessor, candidate count, and candidate digest. Candidate rows live under the generation namespace and are invisible to active serving.

The only path to `READY` is `validate_candidate`. Validation reopens persisted authorities, verifies every linked artifact digest, verifies the exact candidate count and digest, and compares every candidate row with an immutable expected-current artifact produced by the independent primitive-record oracle. A failing receipt is still persisted and the generation becomes `FAILED`. No caller-provided Boolean can mark a generation ready.

Before publication, the transaction rechecks the persisted receipt and recalculates candidate count and digest. Direct post-validation tampering therefore cannot cross the visibility boundary.

## States and transitions

The happy path is `CREATED → BUILDING → VALIDATING → READY → ACTIVE → RETIRED`. Validation mismatch produces terminal `FAILED`. A ready race loser remains inspectable and may be requalified against a later predecessor; `SUPERSEDED` is reserved in the schema for an explicit future disposition rather than inferred cleanup. Rollback is a new audited `RETIRED → ACTIVE` publication, never deletion or reversal of history.

Illegal transitions raise `StateConflict`. Publication eligibility, evidence, expected predecessor, and pointer state are checked inside `BEGIN IMMEDIATE`.

## Atomic publication and idempotency

Activation and rollback require an operation ID, expected active generation, and expected pointer version. One SQLite transaction:

1. resolves exact operation replay or conflicting operation reuse;
2. checks pointer and target eligibility;
3. verifies the stored validation receipt and current candidate bytes;
4. compare-and-swaps the pointer;
5. retires the former generation and activates the target;
6. appends pointer history; and
7. persists the immutable publication receipt in the operation ledger.

Two independent connections that read the same predecessor cannot both commit. The loser receives `CompareAndSwapConflict`, not a lock error. If acknowledgement is deliberately lost after commit, an identical retry after process restart returns the stored receipt without incrementing the pointer again. Reusing that operation ID for different content raises `IdempotencyConflict`.

## Reader pinning and TTL

A logical request calls `pin_reader` once and receives `(generation_id, pointer_version)`. All feature lookups use that generation even if publication occurs between lookups. Retired generations are retained in Part 1, so existing reader tokens remain valid. An unavailable or invalid generation raises `PinnedGenerationUnavailable`; serving never silently repins.

TTL is read from the persisted definition referenced by the row's definition digest. A value is present strictly before `event_time + ttl_seconds` and missing at or after that boundary. Expiry never deletes offline artifacts or old generations.

## Restart, rollback, and retention

Definitions, artifacts, generation states, candidates, receipts, pointer, operations, and history are SQLite state. Reopening the file reconstructs authority without a process-global registry. A build interrupted in `BUILDING` can replay exact candidate rows and continue; changed replay fails. A validated interruption remains `READY` until a guarded publication.

Rollback targets a retained, validated `RETIRED` generation and uses the same CAS and operation rules as activation. A stale rollback cannot clobber a later pointer version. Failed, retired, and race-losing candidates remain inspectable; Stage 2 performs no implicit cleanup.

## Determinism and code identity

Canonical JSON uses sorted keys, compact separators, UTF-8, and SHA-256. Ordered rows have declared stable keys. Committed evidence contains no wall-clock time, random ID, host path, username, credential, or database bytes. SQLite file-byte equality is not claimed; canonical content equality is.

The generation's `code_tree` is a declared implementation identity. Committed proof binds the exact predecessor commit/tree, fixture and independent-oracle SHA-256, and artifact digests. The exact candidate head is checked externally with `validate_stage2.py --expect-head` to avoid a self-referential commit digest.
