# Stage 6 cleanup matrix

Stage 6 verifies this inverse but performs no cleanup because it creates nothing.

| Resource | Pre-destroy evidence | Ordered cleanup concern | Residual proof |
|---|---|---|---|
| EventBridge rule/target | Rule disabled; no pending dispatch | Remove target, then rule | FeatureForge rule absent |
| Step Functions | No running execution | Stop/await execution, then delete machine | Machine/tag query absent |
| Glue job/catalog | Job runs terminal; output receipts retained | Delete job, then catalog database | Job/database absent |
| Lambda | No invocation; log receipt retained | Delete function/version | Function absent |
| DynamoDB tables | Active generation and receipts exported; PITR noted | Delete exact tables only | Table descriptions absent |
| S3 objects/buckets | Version inventory and retained public evidence digests captured; current/noncurrent versions expire after thirty days and incomplete multipart uploads abort after one day | Terraform destroys the three exact run-scoped buckets with version cleanup enabled; no name glob or account-wide deletion | Version listing empty; buckets absent |
| CloudWatch | Alarm/log/dashboard inventory captured | Delete alarms/dashboard/log groups | Names absent |
| IAM roles/policies | No active execution; attachment inventory captured | Detach inline policies, delete exact roles | Roles absent |
| KMS alias/key | All encrypted resources removed | Delete alias, schedule exact key deletion | Alias absent; key pending deletion |
| Terraform state/lock | Final refresh and destroy result captured privately | Release lock; retain encrypted state per policy | No live managed addresses |

Cleanup uses exact resource allowlists from the reviewed plan/state. Broad globs, account-wide deletion,
another project's names, and best-effort success are forbidden. Any residual keeps the future teardown
stage open. `force_destroy` is limited to the three exact run-scoped Terraform bucket addresses and
has no effect until a separately reviewed destroy; it is not permission for manual or prefix-wide cleanup.
