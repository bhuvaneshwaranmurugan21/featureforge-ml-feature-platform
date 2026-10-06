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
- Initial acquisition is one conditional `PutObject` with `If-None-Match: *`, AES-256 server-side
  encryption, and a maximum sixty-minute horizon. It occurs only after the final branch head is
  known. A conflict fails closed.
- A successor is permitted only by separate, explicit authorization after the current lease is
  expired, the exact Stage 6 residual inventory is empty, and the backend is still the byte-exact
  serial-zero state. It is one AES-256 `PutObject` guarded by `If-Match` against the exact current
  ETag. A missing object, conflict, precondition failure, changed state/inventory, or unknown write
  outcome stops execution; an unknown outcome is observed before any possible retry.
- The successor preserves every prior immutable object version and creates no delete marker. The
  public receipt fingerprints the prior and successor versions and bodies without exposing raw
  version IDs, ETags, account IDs, or bucket names. This is a compare-and-swap transition, not a
  silent overwrite or takeover.
- Lease acquisition or succession is an explicitly authorized coordination write. Every operation
  after that one write is read-only AWS qualification or refresh-aware planning; Terraform apply,
  runtime-resource mutation, and managed workload execution remain forbidden.
- The public receipt records only the lease content digest, object-version fingerprint, source
  commit, owner/run ID, timestamps, and decision. Bucket names, account identifiers, raw version
  IDs, and credentials remain private.
