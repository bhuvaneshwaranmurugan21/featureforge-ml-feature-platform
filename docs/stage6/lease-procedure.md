# Stage 6 exclusive lease procedure

The project-neutral coordination system may expose only lease metadata, never another project's
resource inventory or evidence. For Stage 6, the coordination object is a versioned, encrypted
object in the private FeatureForge Terraform-backend bucket at
`leases/featureforge/stage6.json`. A FeatureForge lease contains `lease_id`, `owner`,
`source_commit`, acquisition, heartbeat, and expiry.

- Owner is the immutable Stage 6 `run_id`.
- Source is the exact reviewed Git commit.
- Maximum time from heartbeat to expiry is sixty minutes.
- Admission fails if the lease is absent, owned by another run, bound to another commit, observed
  before its heartbeat, or at/after expiry.
- A future heartbeat may extend expiry only while the same owner and source remain current.
- Expiry does not authorize takeover while an AWS execution is active; the operator must reconcile
  execution and residual inventory first.
- Acquisition is one conditional `PutObject` with `If-None-Match: *`, AES-256 server-side
  encryption, and a maximum sixty-minute horizon. It occurs only after the final branch head is
  known. A conflict fails closed; Stage 6 never overwrites or silently takes over a lease.
- Lease acquisition is an explicitly authorized coordination bootstrap write. Every subsequent
  Stage 6 operation is read-only, and no runtime resource or managed workload is mutated.
- The public receipt records only the lease content digest, object-version fingerprint, source
  commit, owner/run ID, timestamps, and decision. Bucket names, account identifiers, raw version
  IDs, and credentials remain private.
