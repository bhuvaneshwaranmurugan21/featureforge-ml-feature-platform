# Stage 6 IAM matrix

| Principal | Allowed capability | Resource boundary | Reason |
|---|---|---|---|
| Control worker | Exact S3 version reads and encrypted writes | Offline and evidence buckets only | Validate manifests and persist receipts |
| Control worker | Get/put/query/update/transaction | Control and online tables only | Idempotency, candidate reconciliation, guarded activation |
| Control worker | Start/get one Glue job | Exact job ARN | Bounded orchestration |
| Control worker | KMS encrypt/decrypt/data key | One project key | Required by encrypted buckets/tables |
| Glue job | Read artifacts and exact inputs; write isolated output | Artifact, offline, and evidence buckets | Execute bounded feature computation |
| Glue job | KMS encrypt/decrypt/data key | One project key | Encrypted input/output and logs |
| State machine | Invoke exact Lambda; start/get/stop exact Glue job | Exact function/job | Managed control graph |
| EventBridge | Start exact state machine | Exact state machine | Disabled schedule definition only |
| GitHub plan role | Get/list/describe/simulate | Read-only qualification APIs | Terraform refresh and qualification, never apply |

The plan role trust requires audience `sts.amazonaws.com` and the immutable GitHub subject
`repo:bhuvaneshwaranmurugan21@276895096/featureforge-ml-feature-platform@1332971230:environment:featureforge-stage6-plan`.
The immutable owner and repository identifiers prevent a renamed or recreated repository from
inheriting this trust boundary.
It has no create, update, delete, pass-role, object-write, table-write, workload-start, or deployment
permission.

Wildcard resources exist only where AWS read/list APIs or Step Functions log-delivery APIs do not
support meaningful resource scoping. They are read-only or service-required log-control calls; the
action allowlist, account-bound provider, exact OIDC subject, and no-apply workflow remain the
compensating boundaries. Any added wildcard action is a blocking change requiring a new review.
# Admission read authority

The control worker additionally reads only versioned artifacts at `admission/*`, the exact
FeatureForge Stage 6 bootstrap lease object, service quota observations in the authorized region,
and gross account budget observations. Budget reads use the account's budget ARN namespace;
service quota reads require `*` because the API has no resource-level scope, with an explicit
requested-region condition. These are future Terraform policy definitions, not applied IAM changes.
