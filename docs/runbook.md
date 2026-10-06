# Managed execution and publication runbook

## Local Stage 2 qualification

Before translating semantics to a managed runtime, run `pytest`, all three stage validators, and
regenerate both Stage 2 proofs. Confirm the source, label, definition-set, dataset, expected-current,
and generation artifacts reopen with the same canonical digests. Require a persisted passing
validation receipt, expected active generation, expected pointer version, and a unique operation ID
for publication. Pin one generation per logical read. Retry a lost acknowledgement with the exact
same operation ID and request; changed content must be rejected. Treat rollback as a new guarded
publication and retain the failed or retired generation for inspection.

The exact commands and committed outputs are in `docs/stage2/walkthrough.md`. These local steps do
not replace the managed evidence requirements below.

## Local Stage 3 qualification

Install `.[dev,spark]` with Java 17. Run the full predecessor suite, `validate_stage0.py` through
`validate_stage3.py`, and regenerate both Stage 3 proofs twice. Require byte equality with committed
evidence. Inspect the affected-scope reasons, confirm incremental and full row digests match, confirm
preserved rows carry the target generation and knowledge frontier, and reject any Cartesian product.
Exercise exact replay, partial staging, tampering, corrupt predecessor, and full-rebuild fallback. The
bounded pair counts and partition distribution are correctness diagnostics, not managed-runtime
performance evidence.

## Local Stage 4 qualification

Install `.[dev,spark,aws]` with Java 17. Run the full suite and `validate_stage0.py` through
`validate_stage4.py`. Regenerate both Stage 4 proofs twice and require byte equality with committed
evidence. Inspect the exact candidate count/digest, paginated traversal, request-model validation,
one-winner activation race, lost-ack restart replay, pinned-reader result, TTL/freshness boundaries,
retry ledger, item-limit headroom, and explicit non-claims. These checks make no AWS call and do not
replace managed-runtime evidence.

## Preconditions

- Feature owners approve versioned definitions, types, TTLs, null policy, and owners.
- Source contract and source frontier are pinned.
- Dataset labels have documented event and availability timestamps.
- Capacity, hot-key distribution, and DynamoDB quotas are reviewed.
- Previous online generation remains retained and readable.

## Local Stage 5 qualification

Run the full suite and Stage 0–5 validators. Regenerate both Stage 5 correctness proofs twice and
require byte equality. Recompute the benchmark summary from the immutable raw trial file and require
byte equality; do not require newly observed durations to match. Inspect all nine gate decisions,
pointer preservation, stale CAS, freshness/expiry boundaries, bounded labels, the complete 3-by-3
matrix, retained variability, and claim classifications. No Stage 5 local evidence authorizes an
AWS or DynamoDB request.

## Local Stage 6 qualification

Install the pinned Python dependency sets with Python 3.12 and Java 17. Run the full suite and Stage
0–6 validators; regenerate both Stage 6 proofs and both runtime archives twice and require byte
equality. Verify Terraform 1.9.8 and AWS provider 5.100.0 provenance and lock data. The local graph
must retain disabled dispatch, runtime concurrency one, two Glue `G.1X` workers, a fifteen-minute
timeout, encryption, PITR, bounded logs, and no placeholder state. Local qualification makes no AWS
request and does not authorize a plan or apply.

## Stage 6 read-only AWS and saved-plan procedure

1. Resolve the exact authorized region and approved OIDC role or AWS session mechanism; record only
   sanitized account and role fingerprints publicly.
2. Reverify the immutable Git head/tree, provider lock, artifact digests, backend identity, and the
   current project-neutral FeatureForge lease. An initial lease must be absent; an explicitly
   authorized successor requires an expired current lease whose exact ETag is retained for CAS.
3. Execute only the APIs in `docs/stage6/aws-read-api-manifest.json`, restrict resource queries to
   the exact FeatureForge namespace, and produce a sanitized read-only receipt.
4. Fail closed on wrong account/region/role, a current/conflicting lease, residual inventory,
   changed state, missing service, quota shortfall, unknown price, insufficient budget headroom,
   or stale input.
5. Before a separately authorized successor, verify Terraform 1.9.8, locally initialize and
   validate with the backend disabled, build artifacts twice, and recheck state and inventory.
6. Perform exactly one encrypted `If-Match` successor lease write. Preserve version history, stop
   on conflict or unknown outcome, and perform no other AWS write.
7. Create private variables/backend inputs, run `terraform plan -refresh=true -lock=false`, and
   retain the binary plan and raw JSON only in private temporary storage.
8. Normalize and review the plan. Reject deletion, replacement, non-FeatureForge addresses,
   enabled scheduling/runtime, unencrypted storage, concurrency above one, or any action outside the
   allowlist.
9. Bind the binary and normalized plan digests to commit/tree, variables, provider lock, state
   lineage/serial, account, region, artifacts, inventory, lease, cost, creation, and expiry.
10. Retain only sanitized receipts. Never commit state, plan binaries, private backend configuration,
   account IDs, ARNs containing account IDs, signed URLs, credentials, or session material.

The OIDC role, hardened backend bucket, initial empty state lineage, and each conditional exclusive
lease transition are separately authorized writes. They do not authorize Terraform apply or
runtime-resource mutation. After the one lease transition, the saved-plan command window is
read-only.

Stage 6 ends at a verified saved plan. Apply, workload execution, and teardown require later,
separate authorization.

## Execute

1. Create run and generation IDs; record commit, definition-set digest, region, and operator.
2. Capture source snapshot/frontier plus `dataset_as_of`.
3. Build a training dataset and persist its manifest before model consumption.
4. Materialize the new offline generation; capture Iceberg snapshot and Spark run IDs.
5. Stage the online generation under generation-prefixed keys.
6. Sample and fully compare critical features across both stores.
7. Inject one late correction and prove a pre-correction label does not change.
8. Inject one staged online mismatch and prove publication is blocked.
9. Repair/rematerialize, re-run type, freshness, null-rate, distribution, and parity gates.
10. Conditionally swap the online pointer and observe latency, errors, missing rate, and skew.

The managed execution steps above are future operational requirements, not actions performed by
the Stage 4 local/CI proof.

## Roll back

1. Stop materialization/publication workflows.
2. Read the current pointer version and last proven generation.
3. Conditionally swap the pointer to the previous generation.
4. Validate model request success, freshness, missing rate, and sampled parity.
5. Preserve the failed generation and evidence for incident review.

## Required evidence before production claims

- run/generation IDs and definition digests;
- deployed resource identifiers and Terraform plan/apply output;
- source and Iceberg snapshot frontiers;
- Spark/Step Functions run links and CloudWatch logs;
- late-correction leakage test and online mismatch/recovery proof;
- dataset and parity manifests;
- measured offline build time, online write rate, p50/p95/p99 read latency, missing rate;
- item count/bytes, hot-key analysis, and actual AWS cost;
- rollback and teardown evidence.
