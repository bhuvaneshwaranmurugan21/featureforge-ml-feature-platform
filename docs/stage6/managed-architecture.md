# Stage 6 managed architecture authority

## Claim boundary

The following flow is the required target architecture, not evidence that Stage 6 is complete.
Integration review found admission, output-provenance, online-effects, orchestration-shape, and cost
proof defects. Repairs and their validation are tracked in the autonomy checkpoint. In particular,
runtime admission must fail closed until an independently verified current authority is wired; a
manifest-byte match alone never makes a run eligible. Historical plan receipts do not qualify the
changed source or runtime archives.

Stage 6 is intended to make the FeatureForge AWS path deployable and plan-verifiable. It does not apply Terraform,
create resources, execute a managed workload, or claim live Glue, DynamoDB, S3, Lambda, Step
Functions, CloudWatch, or KMS behavior. Those claims require later bounded-run evidence.

## Authority flow

1. A managed-run manifest binds one Git commit/tree, sanitized account fingerprint, region,
   namespace, version-pinned S3 inputs, artifact digests, row limits, output prefix, cost ceiling, and
   safety margin.
2. Admission compares the manifest with the current account/region, exclusive lease, empty residual
   inventory, service quotas, and current cost envelope. Every mismatch rejects before mutation.
3. The disabled EventBridge rule is inventory only. A future authorized operator manually starts one
   Standard Step Functions execution after admission.
4. Lambda validates the manifest and records each idempotent task in the control table. The state
   machine then runs the exact Glue job and quarantines any exception.
5. Glue reads only object versions named by the manifest, verifies bytes, computes the five verified
   features, and writes versioned KMS-encrypted rows, generation manifest, and receipt beneath the
   run-isolated prefix.
6. The control worker writes the isolated online candidate, strongly reads every expected entity page,
   rejects missing, extra, corrupt, or repeated-page results, and persists an immutable validation
   receipt.
7. Parity must match a separately frozen oracle digest. Only `ELIGIBLE` reaches DynamoDB conditional
   activation; a stale pointer or receipt fails the transaction without changing the active generation.
8. Completion binds the manifest, admission decision, task receipts, and output object authorities.

## Failure containment

- Concurrency is one at Glue, Lambda, lease, and run layers.
- Retries are bounded and operate under stable request identities.
- All outputs are generation/run isolated; partial output is never active.
- Schedule dispatch is disabled and the state machine defaults to quarantine.
- No list result is treated as object authority; lists are used only for bounded residual inventory.
- Unknown DynamoDB errors are not assumed retryable.
- Logs exclude execution data at the Step Functions boundary and retain only seven days.

## Deliberate limits

The planned job is a small managed qualification, not a scale benchmark. It uses two `G.1X` workers,
a fifteen-minute timeout, a bounded JSON source object, and explicit input/output row limits. Stage 7
must measure the managed behavior; Stage 8 owns failure injection and teardown. Stage 6 cannot promote
`AWS_MANAGED_RUNTIME` beyond `NOT_YET_VERIFIED`.

## Deployment-pinned admission adapter

The control worker now requires `ADMISSION_AUTHORITY_JSON` to identify an immutable approval
object by bucket, key, version and SHA-256. A missing pin fails closed. The worker cannot issue
or write this authority. Its role can read versioned objects under the artifacts bucket's
`admission/` prefix and the exact bootstrap lease key, but cannot change either authority.

`AWSManagedAdmission` checks canonical bounded approval bytes, the complete manifest, a maximum
one-hour validity interval, live STS account and region, the latest lease version and checksum,
current Glue/Lambda quota observations, and current gross USD monthly budget headroom. Inventory
and pricing remain trusted pipeline snapshots with the existing freshness limits; they are not
claimed to be continuously enumerated live inventory. Each later task rechecks the authority
against its durable admission receipt before performing effects.

The new eighth contract is `stage6-admission-authority-v1`. Local boundary tests prove rejection
behavior and read request construction, not real AWS execution or a complete cost bound.
No production approval has been issued. The cost-bound evidence digest identifies an issuer's
proof; it does not manufacture one. `bound_enforcement_verified` remains false until aggregate
internal effects, redrive limits and finite teardown are mechanically proved. Runtime enablement
and admission authority therefore remain absent by default.

## Durable Glue launch admission

`START_GLUE` is a real control-worker task. The state machine cannot call `StartJobRun` directly;
only the worker role can launch the exact job. Before launching, it checks the deployed job's
retry and concurrency settings. Three fixed DynamoDB attempt keys bind the manifest, execution,
deployment job, compute limits and request. Conditional transactions reserve a slot before a
single-attempt SDK request. Unknown reservations and unknown launch outcomes consume slots.
Completed launch results replay after process restart. No runtime role may delete these records.

The launcher passes a per-slot authority digest into Glue. The version-2 output authority carries
that digest, and the worker rejects outputs associated with another physical launch. Job/run IDs
also match the durable launch receipt. This preserves provenance when a lost acknowledgement
causes another counted attempt. The state machine waits ten seconds between exact-job status
reads, accepts only matching successful completion, quarantines terminal failures, and has a
one-hour execution timeout. A quarantined asynchronous job remains subject to its fifteen-minute
job timeout; quarantine does not claim that AWS cancellation or resource cleanup occurred.

Local SQLite tests exercise independent concurrent database connections and persistence across
launcher reconstruction. They validate request shapes against the pinned SDK and inject lost
acknowledgements at reservation, launch and completion boundaries. This is local contract evidence,
not a live DynamoDB/Glue test or aggregate cost proof. The workload cost profile remains unverified.
