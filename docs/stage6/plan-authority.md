# Stage 6 saved-plan authority

The binary Terraform plan is private and short-lived. Public evidence records only its SHA-256 and a
sanitized normalized JSON derivative. The plan authority binds:

- Git commit and tree;
- canonical variable-file digest;
- provider-lock digest;
- backend state lineage fingerprint and serial;
- sanitized AWS account/role fingerprint and region;
- deterministic artifact digests;
- residual-inventory and lease digests;
- binary and normalized-plan digests;
- creation and expiry, no more than sixty minutes apart.

Changing any binding, reaching expiry, detecting drift, or moving the reviewed head invalidates the
plan. Regeneration requires a fresh read-only qualification, refresh, normalization, cost calculation,
review, and authorization. Binary plans, state, backend configuration, account IDs, role ARNs, signed
URLs, session material, and private variable files are never committed.

The normalized plan must contain only allowlisted FeatureForge creates/reads for the bounded graph. A
delete, replacement, unexplained update, non-FeatureForge address, enabled schedule, concurrency above
one, unencrypted store, or cost above the ceiling blocks approval. The three
`aws_s3_bucket_lifecycle_configuration` resources are admitted only as the exact reviewed
`artifacts`, `evidence`, and `offline` instances. Each must contain one enabled
`stage6-thirty-day-cost-horizon` rule with 30-day current and noncurrent expiration and a one-day
incomplete-multipart abort. Merely adding the resource type to an allowlist is insufficient.

`tools/execute_stage6_plan.py` is the bounded plan-only executor. Before its sole conditional lease
write becomes reachable, it requires the exact clean commit, a successful exact-head qualification
completed within one hour, the authorized account, unchanged serial-zero state, empty residual
inventory, an expired current lease, Terraform 1.9.8, backend-disabled initialization and validation,
and two byte-identical artifact builds matching committed evidence. It then rechecks state and
inventory and performs one `If-Match` successor write with SDK attempts fixed at one.

The executor uses a private `TF_DATA_DIR`, private backend and variable files, `terraform plan
-refresh=true -lock=false`, and a private binary plan. It implements no apply, destroy, import, IAM
change, deployment, or workload path. After planning it requires an unchanged lease, unchanged state
bytes/serial, unchanged empty inventory, and clean worktree. Only canonical sanitized JSON receipts
and a deterministic public archive leave the private output directory. Authority expires at the
earlier of the successor lease expiry and the qualification's one-hour freshness boundary.

The provider working directory is isolated in temporary storage rather than nested beneath the plan
output. This keeps the authenticated plan, raw JSON, inputs, logs and small deterministic artifacts
in a persistent CloudShell output directory without copying the large replaceable provider cache
into the persistent quota. Loss of the provider cache does not remove the saved plan evidence.

If the executor has already completed its one conditional successor write and created the binary
plan, it must never be blindly rerun. While the authenticated private binary/raw pair still exists,
`tools/finalize_stage6_saved_plan.py` is the recovery path for that state. It pins the observed
binary/raw-plan hashes and timestamp; reproduces raw JSON from the saved binary with Terraform 1.9.8;
rebuilds the exact plan-source artifacts twice; validates the strict lifecycle and existing
capacity/security invariants; proves the plan timestamp fell inside the original lease and exact-head
qualification windows; and re-reads the state, residual inventory, and exact three-version lease history
before and after finalization. It implements no lease write or Terraform plan/apply/destroy/import
command. Its output is explicitly historical saved-plan evidence and sets
`current_execution_authority` to false; it cannot authorize deployment.

If both authenticated private plan files are lost, their recorded hashes cannot reconstruct their
bytes and the finalizer must fail closed. A replacement plan is a distinct transaction: it requires
a new reviewed source, a new exact-head qualification, and separate explicit authorization for one
additional conditional successor write. The retry executor is pinned to the previously observed
two-version history, expired source `8fc29d36f39a8cb8105f004b594bbedea8292d77`, exact prior-object
SHA-256 and exact immutable-version fingerprint. It rejects any missing, extra, replaced, current,
or differently identified prior version and requires the CAS result to increase the history from
exactly two versions to exactly three. This is not recovery of the lost plan and does not reuse its
expired authority.

The persistent retry plan is the finalizer's current input. The finalizer is byte-bound to its
canonical zero-write failure observation, binary and raw-plan hashes, timestamp, source commit and
tree, exact-head qualification, latest successor object hash and immutable-version fingerprint. The
Glue log-group namespace predicate accepts only
`/aws-glue/jobs/featureforge-stage6-s6-plan-20260930/` and descendants; a sibling near-prefix or the
bare prefix is rejected. Backend-disabled provider initialization may be repeated because it does
not plan or mutate AWS; `terraform show` must reproduce the authenticated raw JSON byte for byte.
