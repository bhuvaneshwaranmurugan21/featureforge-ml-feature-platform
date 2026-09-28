# Stage 6 exclusive lease procedure

The project-neutral coordination system may expose only lease metadata, never another project's
resource inventory or evidence. A FeatureForge lease contains `lease_id`, `owner`, `source_commit`,
acquisition, heartbeat, and expiry.

- Owner is the immutable Stage 6 `run_id`.
- Source is the exact reviewed Git commit.
- Maximum time from heartbeat to expiry is sixty minutes.
- Admission fails if the lease is absent, owned by another run, bound to another commit, observed
  before its heartbeat, or at/after expiry.
- A future heartbeat may extend expiry only while the same owner and source remain current.
- Expiry does not authorize takeover while an AWS execution is active; the operator must reconcile
  execution and residual inventory first.
- Stage 6 performs a read-only lease qualification. Lease acquisition/mutation belongs to the later
  managed-run authorization.
