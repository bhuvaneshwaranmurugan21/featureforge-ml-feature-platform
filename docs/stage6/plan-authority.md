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

If the executor has already completed its one conditional successor write and created the binary
plan, it must never be blindly rerun. `tools/finalize_stage6_saved_plan.py` is the only recovery path
for that state. It pins the observed binary/raw-plan hashes and timestamp; reproduces raw JSON from
the saved binary with Terraform 1.9.8; rebuilds the exact plan-source artifacts twice; validates the
strict lifecycle and existing capacity/security invariants; proves the plan timestamp fell inside
the original lease and exact-head qualification windows; and re-reads the state, residual inventory,
and two-version lease history before and after finalization. It implements no lease write or
Terraform plan/apply/destroy/import command. Its output is explicitly historical saved-plan evidence
and sets `current_execution_authority` to false; it cannot authorize deployment.
