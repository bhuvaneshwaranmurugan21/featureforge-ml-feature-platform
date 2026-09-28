# Stage 6 managed architecture authority

## Claim boundary

Stage 6 makes the FeatureForge AWS path deployable and plan-verifiable. It does not apply Terraform,
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
