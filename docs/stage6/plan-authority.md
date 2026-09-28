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
one, unencrypted store, or cost above the ceiling blocks approval.
