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
