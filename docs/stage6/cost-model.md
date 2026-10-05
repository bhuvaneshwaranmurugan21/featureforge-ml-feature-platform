# Stage 6 cost authority

Authorization sets `FF_MAX_COST_USD=25` and a mandatory twenty-percent safety margin. Calculations use
integer micro-US-dollars and round each extended line upward. Admission requires:

`ceil(subtotal × 1.20) <= 25,000,000 micro-USD`

The exact worksheet must use current authorized-region prices for two Glue `G.1X` workers for at most
fifteen minutes, Lambda requests/duration at concurrency one, Step Functions Standard transitions,
DynamoDB on-demand requests and bounded storage, versioned S3 requests/storage, KMS requests,
CloudWatch logs/metrics/dashboard, and any retained catalog metadata. Free-tier or credits may be
reported separately but may not reduce worst-case cost.

Stage 6's deterministic local fixture demonstrates arithmetic only; it is not current AWS pricing.
The read-only AWS gate must capture the pricing observation time, budget/credit headroom, quantities,
unit prices, subtotal, margin, and final ceiling decision. Any missing price is conservatively bounded
or blocks planning. A configuration change invalidates the worksheet and saved plan.

## Current authority repair and deliberately open gate

The original qualification sampled one arbitrary product per service and used hardcoded lump-sum
allowances. Those observations are historical availability evidence, not an adequate current price
authority. The repaired collector paginates the authorized-region catalog, extracts OnDemand USD
dimensions, records SKU, term, rate, effective date, publication and catalog digest, matches the
frozen component unit/usage selector and takes the maximum matching tier. Tiny rates round upward
to integer micro-USD before quantity multiplication. Free tiers and credits never lower the bound.
Unknown products, absent components, pagination cycles and a twenty-percent-inclusive total above
USD 25 reject admission.

`cost-workload-profile.json` freezes proposed quantities, including three possible Glue launches
(three durable slots), twenty-eight possible Lambda invocations (seven tasks, initial plus three
retries), PITR, object versions, logs, custom metrics, alarms, dashboard and X-Ray. Each line is an
explicit billed-unit quantity; it is not a currency allowance. Thirty-day storage quantities are
full-month conservative bounds, not a claim that resources automatically expire. The profile's
`bound_enforcement_verified` is intentionally **false**: existing controls do not yet prove aggregate
Spark spill, all object versions, request/log/metric ceilings and cleanup deadline. A local arithmetic
pass cannot close `ST6-AC-16`; live qualification rejects until those exact bounds are independently
enforced and traced to configuration. Setting that flag without the missing proof is not a repair.

Budget admission now requires a currently applicable account-wide COST/USD/MONTHLY budget, with
credits and refunds excluded, exact USD actual and forecast spend, and sufficient remaining headroom
after subtracting their maximum from the budget limit. If several applicable budgets exist, the
smallest remaining headroom governs. Names are fingerprinted. A budget count, successful billing API
read, credit balance, or project ceiling alone does not establish headroom. No budget is created or
modified by qualification. Missing or credit-netted budgets reject rather than silently succeeding.

Managed/runtime artifact and graph repairs supersede the old saved plan. A fresh exact-source price
observation, enforced-bound proof, budget authority, exclusive lease and saved plan are still needed;
the expired historical plan is not renewed by this code change.

## Source review of the remaining quantity authority

The reviewed graph supplies finite *per-execution* compute limits: seven Lambda task states, each with
an initial invocation plus at most three retries, use 512 MiB and a 300-second timeout. That gives
at most `7 × 4 × 0.5 × 300 = 4,200 GB-seconds` and twenty-eight invocations **if there is exactly one
admitted Step Functions execution without redrive**. The durable Glue launcher admits three slots, each
using two `G.1X` workers for at most fifteen minutes: `3 × 2 × 15/60 = 1.5 DPU-hours` under the same
single-execution condition. A ten-second polling wait and one-hour workflow timeout allow at most
360 poll cycles per uninterrupted execution. Counting three GetJobRun attempts, one choice and one
wait per cycle gives at most 1,800 monitoring transitions; the profile rounds up to 2,000 for task,
retry and terminal paths. Redrive can reset workflow timing and retry bookkeeping, so these are
not an aggregate proof across redrives. The durable slots independently bound physical Glue starts.

| Quantity | Mechanism present | Missing mechanical proof |
| --- | --- | --- |
| Workflow executions | VALIDATE binds the execution identity. A fixed-slot durable launcher admits at most three physical Glue starts across retries and redrives, disables SDK retry, and verifies service job retries are zero. Completed launches replay their exact identity; unknown outcomes consume a slot. | Redrive can still repeat Lambda invocations and monitoring transitions. The launch proof does not bound all workflow effects. |
| Explicit Glue outputs | Five stable run-object keys (including frozen expected state and exhaustive parity evidence), conditional `If-None-Match: *`, at most 32 MiB per object, bounded publication attempts | Launch arguments and output prefix are manifest-bound, non-overridable drift rejects before launch, and conditional publication/recovery pins exact versions. Five explicit objects contribute at most 160 MiB. Inputs, Terraform artifacts, Spark temporary objects, and all physical retry charges still need a complete aggregate inventory. |
| S3 temporary storage | Run-scoped TempDir | No aggregate byte/key/version ceiling for Spark or Glue internal writes; a per-object output guard does not cover TempDir. |
| DynamoDB charged units | Candidate row plan, conditional writes, bounded reconciliation pages | Bind the manifest row limit and scan evaluated-byte limit to the priced quantities, count transaction read/write multipliers and SDK attempts, and reject repeated workflow executions before they incur these effects. |
| Log ingestion | Exact log groups and seven-day retention | Retention bounds storage age, not produced bytes. Glue/Spark service/runtime logs and error stacks are not collectively limited to the proposed one GiB. |
| Custom metrics | Metrics explicitly enabled for Glue | A finite ten-series cardinality is not established for service-emitted job/run/executor metric dimensions. |
| Retained resources | Cleanup inverse documented; KMS deletion window configured | No enforceable finite teardown horizon exists for dashboards, alarms, tables, PITR, KMS keys and buckets. A documented operator intention to delete within thirty days is not a completed control. |
| Qualification calls | Read-only API allowlist | Cost Explorer query charges must be included in the observation/run cost scope with a current price basis and finite query count. Read-only does not imply free. |

Consequently the aggregate cost cannot be rigorously bounded by the existing graph. At least one
standing resource has nonzero recurring cost and no finite enforced lifetime; repeating a workflow
execution can also multiply compute before a later conflicting task is rejected. Increasing a
safety margin, choosing smaller fixture data, disabling validation, or asserting that cleanup will
happen does not resolve either root cause.

Completion requires an exact-source execution owner/admission proof; a complete bounded request,
byte, metric and object inventory that includes managed internal effects; and a real finite cleanup
authority/controller or another mechanically enforced end-of-billing boundary. These changes must
retain the existing correctness, observability, security and cleanup acceptance requirements. The
cost profile must remain unverified until those controls are independently reproduced. No new AWS
permission, runtime resource, workload, budget mutation or lease write was exercised by this review.

## Observed catalog applicability repair

Read-only catalog qualification identified the actual billing dimensions: Glue catalog uses
`Obj-Month` and `Request`; standard x86 Lambda requests use `Request` with `APS2-Request`;
X-Ray uses lowercase `traces` with `APS2-XRay-TracesStored` and `APS2-XRay-TracesAccessed`.
Quantities were preserved. CloudWatch dashboard products are account-global: the observed catalog
declares `location=Any`, an empty region code, `DashboardsUsageHour` or its Basic variant, and
`Dashboards` units at USD 3 per dashboard per month. The collector queries those exact global
products, explicitly retains their global applicability, rejects pagination incompleteness and
selects the maximum current OnDemand USD tier. It never relabels another region's rate as Sydney's
and excludes the separate `Global-` free-tier entries. Other components retain strict region checks.
This fixes price applicability only; it does not establish quantity enforcement or budget headroom.
