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

## Current plan-bound authority

The original qualification sampled one arbitrary product per service and used hardcoded lump-sum
allowances. Those observations are historical availability evidence, not an adequate current price
authority. The repaired collector paginates the authorized-region catalog, extracts OnDemand USD
dimensions, records SKU, term, rate, effective date, publication and catalog digest, matches the
frozen component unit/usage selector and takes the maximum matching tier. Tiny rates round upward
to integer micro-USD before quantity multiplication. Free tiers and credits never lower the bound.
Unknown products, absent components, pagination cycles and a twenty-percent-inclusive total above
USD 25 reject admission.

The KMS request selector is anchored to the exact Sydney standard-request usage type,
`ap-southeast-2-KMS-Requests`. The managed key is explicitly `SYMMETRIC_DEFAULT` with
`ENCRYPT_DECRYPT` usage, and runtime roles permit encrypt, decrypt and symmetric `GenerateDataKey`
operations—not `GenerateDataKeyPair`. Premium asymmetric and RSA/ECC data-key-pair catalog entries
therefore do not describe this graph and cannot be selected merely because their usage names share
the `KMS-Requests` prefix. This applicability constraint changes neither the 250,000-request bound
nor the no-free-tier rule.

`cost-workload-profile.json` freezes one future separately authorized execution and a thirty-day
pricing/authorization horizon. It includes three durable Glue launch slots, twenty-eight possible
Lambda invocations, PITR, object versions, logs, metrics, alarms, dashboard, X-Ray and the one paid
Cost Explorer request made by qualification. The managed manifest is capped at 1,000 input and
5,000 output rows. Each explicit object is capped at 32 MiB; the exact graph has three deployment
artifacts, one immutable input and five run outputs. The Glue definition has no S3 TempDir, so the
job has no unowned temporary-object prefix. All three versioned buckets expire current and
noncurrent objects after thirty days, abort incomplete multipart uploads after one day, and are
covered by the exact Terraform destroy inverse.

`planning_bounds_verified=true` means only that this planned configuration and worksheet are
internally consistent for that one execution and horizon. It does not claim that the managed runtime
executed, that teardown already happened, that billed cost was observed, or that AWS deletion APIs
cannot be unavailable. Those claims remain explicitly false and require Stage 7/8 evidence.

Budget admission requires a currently applicable account-wide COST/USD/MONTHLY budget, with credits
and refunds excluded, exact USD actual and forecast spend, and sufficient remaining headroom after
subtracting their maximum from the budget limit. If several applicable budgets exist, the smallest
remaining headroom governs. Names are fingerprinted. No budget is created or modified.

Managed/runtime artifact and graph repairs supersede the old saved plan. A fresh exact-source price
observation, budget authority, exclusive lease and saved plan are still needed; historical authority
is not renewed by this code change.

## Source review of the quantity authority

Seven Lambda task states, each with an initial invocation plus at most three retries, use 512 MiB and
a 300-second timeout. That gives `7 × 4 × 0.5 × 300 = 4,200 GB-seconds` and twenty-eight invocations
for the one approved workflow execution. The durable Glue launcher admits three slots, each using
two `G.1X` workers for at most fifteen minutes: `3 × 2 × 15/60 = 1.5 DPU-hours`. A ten-second polling
wait and one-hour workflow timeout permit at most 360 poll cycles. Counting three GetJobRun attempts,
one choice and one wait per cycle gives at most 1,800 monitoring transitions; the profile rounds up
to 2,000 for task, retry and terminal paths. A redrive or second execution falls outside this
worksheet and requires new approval.

| Quantity | Plan-time authority | Later-stage evidence still required |
| --- | --- | --- |
| Workflow execution | Approval pins the complete manifest and expires within one hour. VALIDATE binds one execution identity; the launcher admits three physical starts, disables SDK retry and verifies job retries are zero. | Stage 7 must prove only the approved execution was dispatched and reconcile attempts. |
| S3 objects | Nine explicit artifact/input/output objects are each capped at 32 MiB. Conditional publication prevents changed replay; no S3 TempDir exists. Lifecycle rules cover every exact bucket. | Stage 7 records actual versions/bytes; Stage 8 proves destroy and residual state. |
| DynamoDB | The approved manifest fixes 5,000 output rows. Candidate items are size-preflighted before the first write; the worksheet prices 250,000 read and write units plus two GiB-month of storage/PITR. | Stage 7 reconciles consumed capacity and item inventory. |
| Logs and metrics | Four exact KMS-encrypted log groups retain seven days; one job is limited to two workers/fifteen minutes and one workflow to one hour. The worksheet charges one GiB ingestion/storage and ten metric series without free-tier credit. | Stage 7 records actual ingestion and metric cardinality; an observation above the profile invalidates authority. |
| Standing resources | The worksheet charges a full thirty-day month. S3 lifecycle and the resource-specific Terraform destroy inverse cover every planned resource; KMS uses the seven-day deletion window. | Stage 8 must execute teardown and scan residuals; Stage 6 makes no completed-cleanup claim. |
| Qualification calls | SDK retry is one physical attempt, Cost Explorer rejects pagination, and the worksheet includes one primary-billing-view request at the published USD 0.01 rate observed on 2026-10-05. | Any later qualification requires a new observation and worksheet. |

This separation is deliberate: `ST6-AC-16` approves the exact plan and its conservative horizon;
`ST6-AC-17` proves the destroy path covers the plan. Runtime usage, billed cost and completed cleanup
remain later-stage acceptance criteria and are not relabeled as Stage 6 evidence.

The documented Cost Explorer rate is accepted for at most seven days from its observation date.
A later or predated qualification fails closed until the published authority is re-observed and the
profile, evidence index, checks and plan are regenerated.

## Initial candidate transaction projection

The online runtime preflights every planned record and its generation-owner item before the first
DynamoDB request. The same item builder supplies the physical transaction and projection, including
the `record_json` copy, UTF-8 attribute names and values, nullable values, and conservative numeric
representation. Unsupported attribute shapes and service-invalid numeric ranges reject. An oversized
later record cannot leave an earlier partially written candidate.

The versioned compact projection binds the plan digest, record count, total and maximum item byte
bounds, owner size, and initial transactional units. Record writes round each item upward to one-KiB
blocks and count two write units per block. Owner ConditionCheck actions in TransactWriteItems also
round the initial owner to one-KiB blocks and count two write units per block, included in the total.
This covers the initial successful record pass
only. Retries, failed conditions, reconciliation scans, control writes, per-item storage overhead
and PITR remain separate obligations. The projection cannot establish aggregate cost admission.

Qualification SDK clients use one physical attempt, explicit five-second connection and ten-second
read timeouts. Cost Explorer accepts only one complete response; a next-page token rejects without
another paid request. The query interval derives from the observation timestamp. The worksheet
includes that request at AWS's published USD 0.01 primary-billing-view rate, with the source and
observation date retained. Each later qualification produces a new worksheet and pays its own
request charge; one qualification never authorizes an unbounded series of later calls.

Enabled Glue job metrics now have the missing `cloudwatch:PutMetricData` permission, restricted to
the `Glue` namespace and configured region. The worksheet prices the frozen ten-series planning
quantity without free-tier credit. The later managed run must capture actual metric cardinality;
an observation above ten invalidates this authority rather than being ignored.

Primary service references:
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html
- https://docs.aws.amazon.com/cost-management/latest/userguide/ce-what-is.html
- https://docs.aws.amazon.com/glue/latest/dg/monitoring-awsglue-with-cloudwatch-metrics.html

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

The 2026-10-06 exact-head qualification then exposed one remaining over-broad selector: generic
`KMS-Requests` admitted five Sydney dimensions and chose the USD 12 per 10,000 RSA
GenerateDataKeyPair rate. Read-only catalog inspection confirmed that the graph's ordinary symmetric
rate has the exact usage type `ap-southeast-2-KMS-Requests`; asymmetric and data-key-pair products
carry additional suffixes. The anchored selector and symmetric Terraform key declaration bind the
price to the executable graph while preserving all workload quantities, the twenty-percent margin
and the USD 25 ceiling.
