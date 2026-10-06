# Stage 6 failure and repair log

## Terraform registry timeout

The first provider initialization could not reach the Terraform Registry. The provider constraint was
not loosened. Terraform AWS provider `5.100.0` was downloaded from HashiCorp's official release host,
matched against the official SHA-256 list, and installed through a local filesystem mirror. The
included MPL-2.0 license was hashed and recorded.

## Local provider schema process

Terraform initialized from the verified mirror, but local `terraform validate` could not start the Go
provider because the execution sandbox denied its Unix-domain socket. The configuration gate remains
open locally; no alternate provider, mock validation, or skipped requirement was accepted. The pinned
GitHub Infrastructure workflow must perform `terraform validate` on the immutable branch head before
Stage 6 can close.

## Resolved AWS authorization and immutable OIDC subject

The authorized destination is `ap-southeast-2`. The exact FeatureForge bootstrap role is recorded
publicly only by name and fingerprint. Its initial mutable-name GitHub subject was rejected by AWS.
The root cause was GitHub's immutable OIDC subject format for newer repositories. The trust was
corrected to bind owner ID `276895096`, repository ID `1332971230`, the exact FeatureForge branch,
and audience `sts.amazonaws.com`; the subsequent exact workflow attempt passed identity
verification. The Terraform-managed plan-role trust uses the same immutable owner/repository
identity and a protected environment.

## Explicitly authorized backend bootstrap

The operator created one FeatureForge-only S3 backend, enabled versioning, AES-256 default
encryption, bucket-owner enforcement, complete public-access blocking, and a TLS-only deny policy.
One version-4, serial-zero, empty Terraform state lineage was conditionally created and retrieved
byte-for-byte. These writes were explicitly authorized prerequisites, not Terraform apply or
runtime-resource mutations. Public evidence stores only fingerprints and security properties.
Namespace inventory, quota/pricing qualification, lease acquisition, and refresh-aware planning
remain open.

## Glue absence-code classification

The fourth exact-head AWS qualification reached the enumerated residual-inventory reads after IAM
admission succeeded. AWS Glue represented a missing database with `EntityNotFoundException`, while
the shared absence classifier recognized only generic resource-not-found variants. The qualifier
therefore failed closed instead of recording the expected `ABSENT` inventory state. The repair adds
only Glue's documented absence code and regression coverage proving that it maps to `ABSENT` while
an `AccessDeniedException` still raises `LiveEvidenceError`. No resource was created, no read was
removed, and no failure condition was weakened.

The next exact-head attempt advanced past Glue and exposed Step Functions' equivalent
`StateMachineDoesNotExist` absence code. It is covered by the same allowlisted classification and
parameterized regression boundary; unrelated client errors continue to fail closed.

## Step Functions pricing offer code

The following exact-head attempt completed residual-inventory reads and reached live pricing. The AWS
Price List catalog returned no products for the invalid `AWSStepFunctions` service code. The current
official offer index identifies Step Functions as `AmazonStates`, whose Sydney regional catalog is
populated. The qualifier now uses that exact offer code and a regression assertion rejects the
invalid identifier; Step Functions pricing qualification remains mandatory.

## Terraform Glue execution-property shape

The pre-acquisition plan rehearsal found that the saved-plan normalizer read Glue concurrency from
a nonexistent top-level `max_concurrent_runs` field. AWS provider `5.100.0` serializes the configured
`execution_property` block as a single-element collection containing `max_concurrent_runs`. The
normalizer now reads that exact provider shape and requires exactly one block whose value is one.
Provider-shaped regression coverage rejects a missing block, a value above one, and ambiguous
multiple blocks. The exclusive lease was not acquired while this deterministic validation defect
was present, so the one-time conditional lease object was not stranded or overwritten.

## Saved-plan lifecycle allowlist drift

The exact-head executor at `8fc29d36f39a8cb8105f004b594bbedea8292d77` passed its prewrite
checks, wrote one conditional successor lease, and created a 47-resource refresh-aware Terraform
plan. Public finalization then failed closed because the plan normalizer omitted
`aws_s3_bucket_lifecycle_configuration`, even though the reviewed Terraform and Stage 6 structural
validator require exactly three such resources. The failure occurred after the sole authorized
write and plan, so rerunning the executor would violate the transaction boundary.

Read-only diagnostics authenticated the saved binary plan SHA-256
`21283629c85ce06bad6166f505bbbd0d750505ea293802e326f16d85f2747620`, raw JSON SHA-256
`cf3df22c415c2d8c17045bdf15f6e979f82fe72427c0b9735870c3cd28c0ca7e`, Terraform timestamp
`2026-10-06T05:37:10Z`, 47 create actions, and the exact three lifecycle addresses. The lease key
has exactly the preserved historical version plus one latest successor, no delete marker, and
successor object SHA-256 `f425d87569905859407f9e50b93762321ea86ff3b5476cc53631574b4b9718c4`.

The repair adds the missing type together with strict rule semantics and negative controls; it does
not accept arbitrary lifecycle resources. A separate read-only finalizer consumes the existing
private plan, reproduces its JSON with `terraform show`, and rechecks source, artifacts, qualification
time, lease history, state bytes, and empty inventory. It has no AWS write or new Terraform plan
path and labels the result historical rather than current execution authority.

## Saved retry plan Glue log namespace

The separately authorized retry at exact source
`2acd63e2222ada623d277dbd4f5a8032dea8e376` authenticated the two-version lease history, added
exactly one CAS successor version, and generated a persistent 47-create Terraform 1.9.8 plan.
Normalization then failed closed at `aws_cloudwatch_log_group.glue_error`. The reviewed Terraform
correctly uses `/aws-glue/jobs/featureforge-stage6-s6-plan-20260930/`; the validator covered the
managed Lambda and Step Functions paths but omitted the managed Glue hierarchy.

The repair admits only the exact Glue hierarchy followed by a slash. Negative controls reject an
unrelated namespace, a near-prefix escape, and an incomplete prefix. The saved-plan finalizer is
rebound to the exact plan hashes and three immutable lease versions. It may run backend-disabled
initialization, validation, and `terraform show`; it cannot write AWS state, acquire or renew a
lease, create a new plan, apply, destroy, import, deploy, or execute a managed workload.
