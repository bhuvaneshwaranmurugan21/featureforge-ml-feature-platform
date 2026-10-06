# Stage 6 autonomous recovery checkpoint

Status: incomplete; no Stage 6 PR was opened or merged.

## 2026-10-06 modern budget authority repair

Exact-head AWS qualification at `ea519c98fe5af831283da15b65eaf8a3fa6ca2cf` passed current
catalog pricing after the symmetric-KMS correction, then failed before the paid Cost Explorer read
because the budget validator recognized only AWS's deprecated `CostFilters`/`CostTypes` model. A
read-only account observation showed one current COST/USD/MONTHLY budget with USD 20 limit, USD
0.111 actual spend, USD 0.31 forecast spend, and the modern expression `NOT RECORD_TYPE IN (Credit,
Refund)` with the sole `UnblendedCost` metric. That gives USD 19.69 headroom against the unchanged
USD 17.703514 project envelope; the budget itself is sufficient and requires no mutation.

The repair recognizes that exact modern semantic shape while retaining the legacy path. It rejects
service/tag/account predicates, extra excluded record types, other metrics, billing views, mixed
legacy/modern representations, and malformed expressions. This is compatibility with AWS's current
API model, not a reduction of the account-wide gross-cost requirement. Fresh exact-head local and
remote validation remains required; no AWS write, Terraform operation, managed workload, PR, or
merge has been performed by this repair.

## Verified progress

The Stage 6 branch is at `ac1907373b89273d70e1247ebc38db90c314db0d`, tree `0ece218791afc623e33b20123efa20fd9f017060`. GitHub Actions run `36984709058` completed both `recover_historical_evidence` and `qualify` successfully using the existing FeatureForge OIDC role. The recovery observation is retained in `evidence/stage6/recovery-observation.json`; its embedded receipt SHA-256 is `756edd3e6deec133df05e79991696c456afc306b33479786b070424814b10a13`. The observation proves a single unchanged state version and a single expired lease version; it is historical evidence only, not current execution authority. No AWS writes or Terraform execution occurred in recovery.

The published recovery change passed 220 tests, 88.85% coverage against the unchanged 85% gate, Ruff, mypy, dependency integrity, and predecessor validators. The new uncommitted historical reconstitution helper and its seven tests passed focused checks; actual-live-receipt reconstitution and complete-suite integration remain unperformed.

## Required root-cause repairs

## 2026-10-02 second work-window checkpoint

Implemented conditional online writes, bounded strong reconciliation, primitive-source parity,
receipt-bound activation CAS and lost-ack recovery; Glue conditional version/checksum-bound output
publication; API-shaped orchestration and canonical DynamoDB keys; catalog-derived cost arithmetic
and actual/forecast budget headroom; and an executed independent local admission oracle. Focused
tests passed for each repair. Runtime admission now rejects before task writes if its verified
boundary is absent. The concrete Lambda admission boundary is not wired, so managed execution
is intentionally ineligible, not complete. Packaging import inspection found and repaired missing
allowlisted `store.py` dependencies. Original proof reconstitution matched all three witness hashes.

The first combined full run passed 276 tests and the unchanged coverage gate at 88.03%; five tests
failed only on stale predecessor indexes after CI/documentation updates. Those current-file indexes
were regenerated without changing predecessor requirements or proof outputs. The final full run
passed all 282 tests and the unchanged 85% coverage gate at 87.43%. Ruff and strict source mypy
passed. This local result is not Stage 6 completion or exact-head remote CI evidence.

Still open: concrete live admission, pre-execution frozen expected-output authority, candidate seal
and full predecessor/concurrency binding, strict manifest integer parsing, Glue log KMS/retention
and X-Ray IAM, enforceable aggregate cost/cleanup bounds, current AWS qualification and new plan
authority, exact-head CI/review, merged-main verification and the external continuation receipt.
No new lease, plan, AWS write, workload, PR or merge was performed in this repair window.

- `ST6-AC-16`: replace arbitrary catalog samples and hardcoded allowances with exact-region OnDemand unit-rate provenance, a bounded workload/retention profile, contract-valid cost arithmetic, and calculated budget headroom. Preserve the $25 ceiling and 20% margin.
- `ST6-AC-06`: implement and wire actual online candidate writes, strong reconciliation, independent parity, and activation CAS. Current worker returns summaries rather than performing these effects.
- `ST6-AC-08`: correct the parity Choice path to the worker's actual receipt shape and correct Glue response/receipt paths with API-shaped tests.

Resume from this checkpoint, not from Stage 6 initialization. Preserve original source `60c9fb508548470943ba6c66cec6774cc86ad3c0` and all historical evidence. Do not create another lease or saved plan under the consumed one-time authorization. Do not apply, mutate AWS resources, weaken gates, or touch other projects. A runtime/graph change requires fresh plan authority before final closure. Stop at permission boundaries.

Paused after the user's 15-minute work window on 2026-10-02 UTC.

## 2026-10-04 resumed implementation checkpoint

The earlier checkpoint paragraphs are historical observations, not current open-work lists.
The preserved local repair commit `1b2b63c` passed 321 tests, 87.58% coverage, all seven local
validators, Ruff, strict source mypy, dependency integrity, and identical deterministic proofs.
It closes strict manifest integer parsing, pre-Glue primitive expected-state freezing, durable
candidate ownership/sealing, exact Glue log encryption/retention configuration and tracing IAM,
and authenticated historical authority reconstitution. Historical receipts remain expired and
do not confer current plan authority.

The next repair requires a bounded execution identity for VALIDATE and durably binds it through
the existing immutable task request digest. Exact replay preserves that owner; a second owner
conflicts before the state machine can reach Glue. Tests reproduce this across a new DynamoDB
ledger instance. Lease, inventory, cost, plan and quota integer authorities now reject booleans,
floats, strings and missing values without coercion. The resulting complete suite passes 395
tests and the unchanged coverage gate at 87.72%; deterministic runtime archives rebuild identically.

Remote main and the Stage 6 branch were independently rechecked and still match the identities
recorded above. This remains implementation progress, not Stage 6 closure. The Lambda admission
adapter is still absent. Same-execution redrive, managed internal spill/log/metric quantities,
and finite end-of-billing authority remain unproven in the cost model. The cost profile therefore
remains unverified. Fresh exact-source AWS qualification, an independently authorized new lease
transaction, saved plan, required remote checks, review, merge and continuation evidence remain
open. No AWS write, Terraform execution, workload or merge was performed in this resumed window.

### Published repair and live price qualification

The repair tree was published to the existing Stage 6 branch without a force push. Remote CI
passed at heads `1dbe63694fb2c0ff1313ab2beebd671eb8b10167`,
`cf4985156a6dc7a16a971a2162f490aada576d51` and
`1ad03b799bb9ea7c031c2b1f12663f9462e80af0`. Infrastructure run `37222677650`
passed Terraform formatting, clean backend-disabled initialization with the existing locked
providers, and validation. Read-only AWS qualification and historical recovery run automatically
through the existing OIDC role; no CloudShell intervention is necessary for these reads.

Live catalog failures identified incorrect Glue, Lambda and X-Ray unit/usage selectors. They were
repaired from observed dimensions without reducing quantities. The remaining dashboard price was
found in the account-global catalog (`location=Any`, empty region code, USD 3/Dashboard/month).
An explicit narrowly scoped global-dashboard applicability check retains the original region
metadata and rejects other services, other regions and the separate `Global-` free-tier entries.
The final local suite passes 396 tests at 87.72% coverage. All predecessor validators, Ruff,
source mypy, dependency integrity and deterministic Stage 6 proofs pass. Final remote checks
must still be read against the published exact head; earlier green heads never close that gate.

The enforced-bound gate remains false. Price applicability repairs do not prove aggregate managed
internal spill/log/metric/request limits, same-execution redrive limits, finite cleanup, or budget
headroom. Concrete managed admission, a fresh separately authorized lease/plan transaction and
governance closure remain open. No Stage 6 PR, merge, new lease, state change, Terraform runtime
execution or managed workload occurred. Resume from the current branch head and this checkpoint;
do not rerun completed initialization or overwrite historical authorities.
# Resumed admission adapter work

Added a deployment-pinned, read-only AWS admission adapter and an eighth versioned contract.
The control worker now wires this adapter with bounded SDK retry/timeouts and fails closed when
no approval pin is configured. Twenty-one local boundary tests cover expiration, strict numeric
types, source/account mismatch, lease version drift, quotas, cost arithmetic, gross budget
eligibility and durable authority identity. These tests do not constitute production approval
or live managed-runtime evidence. Aggregate cost enforcement and finite cleanup remain unresolved;
the cost workload profile remains unverified. No lease, AWS resource or Terraform state was written.

The integrated suite passed 411 tests with 88.00% coverage before four additional focused admission
tests were added; all nineteen admission tests then passed. Ruff, strict mypy across twenty-three
source files, dependency integrity, all seven stage validators and two byte-identical artifact
builds passed. The first Infrastructure run identified a single spacing correction, now applied
from the pinned formatter's actual diff. GitHub exact-head checks must be verified independently;
no local result substitutes for the remaining acceptance, plan or governance gates.

Added an end-of-read authority check: the latest lease is reread and the current clock is checked
after quota and budget pagination, before returning admission. Expiry or lease replacement during
those reads rejects. The corrected Infrastructure run `37225557658` passed on remote commit
`4b786ed96457cfc871c0c99dcb6f54ae0e76cefa`; the following timing repair requires new exact-head
checks. Read-only qualification still rejects unproved workload/retention bounds. Broad optional
Ruff formatting inspection also reported pre-existing formatting differences; required Ruff lint
passes, and unrelated predecessor files were not rewritten.
# Durable launch budget repair

Continued from published head `06c58de87c6438bd1c364df5993bec6e2b45cc6a`, without restarting.
Added a fixed-slot persistent launcher, a `START_GLUE` task, conditional reservations and
single-attempt SDK enforcement. Deployed Glue retry and concurrency metadata is checked before
reservation; unknown write/launch acknowledgements consume slots and completed launch identities
replay. Local SQLite tests cover separate concurrent connections and reconstructed launcher objects.
The state machine launches through this worker, polls the exact run with a ten-second wait, and
has a one-hour timeout. Its role no longer has direct launch permission. A per-slot digest binds
the version-2 output authority to the recorded physical launch; another launch's outputs reject.

The two new schemas bring Stage 6 to ten contracts. Proposed single-execution cost quantities are
updated to twenty-eight Lambda invocations, 4,200 GB-seconds, and 2,000 state transitions. These
are not an aggregate redrive or resource-lifetime proof. The workload profile deliberately remains
unverified; no approval, lease, state, AWS resource, or managed workload was written or executed.
The previous saved plan is superseded and cannot qualify these changed artifacts and IAM definitions.

Final local validation of the durable launch repair: 426 tests passed, 87.69% coverage, Ruff and strict MyPy passed, all seven local validators passed, and two clean runtime artifact rebuilds matched byte for byte. These are local results; aggregate cost-bound qualification and refreshed AWS/plan/governance closure remain open.

## Immutable-input read-bound follow-up

The worker now applies the same 32 MiB immutable-input byte limit as the Glue entry point before parsing the source in validation and independent projection. Bounded reads request only the limit plus one byte and close the stream on both success and rejection. Output/manifest reads retain their existing behavior; this is not an aggregate S3 storage or memory proof. The final local suite passed 427 tests with 87.72% coverage, Ruff and strict MyPy passed, and two updated artifact rebuilds matched byte for byte.

Published launch repair source `4664cc77ebea04727a326fabeee8acea5c9eb760` passed Infrastructure run `37230304592` and quality run `37230304542`. AWS qualification run `37230304563`, job `111518389658`, rejected the unproved frozen-workload/thirty-day-retention bounds. That rejection remains a necessary open gate, not a rerun-only or CloudShell-access issue. The input-read follow-up supersedes that source for final exact-head validation and saved planning. No new lease, AWS resource/state mutation, PR, merge, or completion receipt was performed.

## Resumed transport, provenance, and reconciliation repair

Resumed the published source `3f0fa0e82eaad8cd58a7f9844528823b61abd2e2`, tree `f3d9bcd5925c18409a6f428fce91ed4f6221ffff`, without restarting or changing main. Replaced quadratic expected-plan searches with exact-key indexing; reject Glue non-overridable argument drift before slot reservation; pin Glue S3 region and one physical SDK attempt; extract the existing conditional publisher into an allowlisted shared runtime module; close read streams.

Move full primitive-derived expected state and exhaustive parity evidence into bounded immutable S3 objects, retaining version/checksum authorities and compact digests in task receipts. Activation verifies the full parity object before conditional promotion; completion reads and inventories all five objects. Bound control ingress to 64 KiB and task receipts to 16 KiB, preserving full independent projection and exhaustive parity. Version both new evidence contracts. No dependency was added. The attempted use of an unavailable optional JSON Schema package in a new test was replaced with repository-native closed-contract checks plus production semantic validation; no package or requirement was bypassed.

Final local validation: 446 tests passed, 88.07% coverage; Ruff and strict MyPy (25 source files) passed. Stage 6 deterministic proof regeneration matched committed bytes. Two clean artifact rebuilds matched. Exact-source CI and current AWS qualification must run after publication; historical green checks are not final-head proof. The cost-bound flag remains false: native logs, Spark spill, cumulative workflow effects, charged-unit accounting, and finite retained-resource lifetime remain open. No AWS write, lease replacement, Terraform runtime operation, PR, merge, or Stage 6 continuation/completion receipt was performed.

## 2026-10-05 charged-effect continuation

Resumed remote source `53f886f1889bf0303a91d80740637ccc96fe1bb0`, tree
`b92d2991b162492541dccedf0775f2871ecf0bc1`; independently checked that remote main remained
`86b3cd27ae95a6142a1d6601d188d83b8e783d29`. No completed initialization was restarted.

Added initial candidate transaction projections derived from the exact physical item builder,
including canonical duplicated content and UTF-8 names/values. Every record and owner is preflighted
before the first DynamoDB request; invalid numeric ranges, unaccounted attribute shapes and an
oversized later item reject without database effects. The compact projection has a thirteenth
versioned closed contract and is retained in the materialization task result. It covers initial
record transactions only, not retry, scan, control, storage/PITR or lifetime quantities.

Pinned qualification SDK clients to one physical attempt and explicit timeouts. Qualification
rejects a wrong account before backend/inventory/pricing reads; its OIDC workflow also binds the
authorized account. Cost Explorer query dates derive from the observation timestamp, and an
incomplete page rejects without another paid request. The published query fee remains an unclosed
pricing obligation; no paid Cost Explorer call occurred during local verification.

Added the permission required by enabled Glue metrics, restricted to the configured region and Glue
namespace, without disabling observability. Bounded and closed the latest Glue-authority stream read.
These controls do not establish aggregate cost or a thirty-day end-of-billing guarantee. The profile
remains unverified. Standing billable resources have no enforced termination, and a cleanup retry
controller alone cannot guarantee a finite deadline under unavailable/denied deletion APIs. A
documented promise or a smaller fixture cannot close that requirement.

Final local integration passed 470 tests with 88.14% coverage, the unchanged 85% gate, Ruff and
strict MyPy across 25 source files. All seven stage validators passed; two clean final artifact
builds matched byte for byte, and deterministic proof bytes remained unchanged. Official AWS
transaction documentation was checked before final publication: owner ConditionCheck actions are
counted as transactional writes at one-KiB boundaries, including in the projected total. Boundary
tests distinguish that charge from four-KiB transactional reads. Exact-source remote checks must
still be independently completed after publication; local success is not acceptance closure.
No AWS write, lease replacement, Terraform runtime operation, workload, PR or merge was performed.

## 2026-10-05 plan-bound cost and cleanup correction

Reconciled the later repair notes with the original Stage 6 acceptance contract. Stage 6 must prove
an exact conservative worksheet and a complete destroy inverse; it must not claim that the future
managed run, billed-cost reconciliation or teardown already occurred. The cost profile now binds one
separately authorized workflow execution, a thirty-day pricing/authorization horizon, 1,000 input
rows, 5,000 output rows, nine explicit 32 MiB objects, the existing compute/retry limits and all
priced service dimensions. Managed execution, billed cost and completed teardown remain false.

Removed the unowned Glue S3 TempDir. Added thirty-day current/noncurrent lifecycle rules and one-day
multipart abort to all three exact versioned buckets. The reviewed Terraform inverse can remove
versions only inside those exact run-scoped buckets during a separately authorized destroy. Added the
published USD 0.01 Cost Explorer primary-billing-view request to the worksheet and changed AWS
qualification triggering so every Stage 6 branch head receives an exact-source read-only run.

This correction performs no AWS write, lease replacement, Terraform plan/apply/destroy, workload,
PR or merge. Fresh exact-head local/remote checks, current read-only qualification, a new separately
authorized lease/plan transaction and governance closure remain required.

Final local correction validation collected and passed 472 tests at 88.14% coverage against the
unchanged 85% gate. Ruff, strict MyPy across 25 source files, dependency integrity and all seven
stage validators passed. Stage 1–6 deterministic proofs reproduced byte for byte. Two clean Stage 6
artifact builds were identical: control worker `f991eae29880ccf4c040255b84370b97906b62f32a53e4ee437893acd0c9be21`,
Glue library `bf4df096ca91bf53bc0c3526395747412eab25a39fe98d72e70c475a423984bd`,
and artifact manifest `0e7f32250cf395d1b0431d69b17afcdb61c8fc12bae1d16b2caee98d44f04cb7`.
The local environment has no Terraform binary; pinned formatting, clean initialization and validation
remain mandatory exact-head remote checks rather than an inferred local success.

## 2026-10-06 exact-head cost-selector repair

Remote infrastructure and quality checks passed at exact head
`a06f7bd7c04c5b7b5e74b53f99d470dfa67488e7`; historical evidence recovery also passed. The
read-only AWS qualification failed closed at the unchanged USD 25 ceiling because the generic
`KMS-Requests` selector admitted five current Sydney price dimensions and conservatively selected
the USD 12 per 10,000 RSA GenerateDataKeyPair rate. A separate read-only catalog observation showed
the standard symmetric dimension is exactly `ap-southeast-2-KMS-Requests`; premium asymmetric and
RSA/ECC data-key-pair products have suffixed usage types.

The repaired profile anchors that exact standard usage type, while Terraform explicitly declares a
`SYMMETRIC_DEFAULT`/`ENCRYPT_DECRYPT` key and the qualifier fails closed if either source binding or
selector drifts. Regression tests present the standard, asymmetric and RSA data-key-pair catalog
rows together and require only the standard row to match. Quantities, no-free-tier treatment,
twenty-percent safety margin and USD 25 ceiling are unchanged. This repair still requires full local
validation, publication, exact-head remote checks and a fresh live qualification; it confers no
lease, plan, apply, runtime, teardown or billed-cost authority.

The captured 2026-10-06 catalog rates, with only the corrected standard KMS dimension substituted,
produce a USD 14.752928 subtotal and USD 17.703514 worst case after the unchanged twenty-percent
margin, leaving USD 7.296486 below the ceiling. This projection is diagnostic evidence, not a
replacement for the next exact-source AWS qualification receipt. Local verification passed 473
tests at 88.14% coverage, 23 focused cost/security tests, Ruff, strict MyPy, dependency integrity,
all Stage 0–6 validators, `git diff --check`, and two byte-identical Stage 6 proof and artifact
builds. The checksum-verified workflow-pinned Terraform 1.9.8 binary passed recursive formatting and
clean backend-disabled initialization. Local provider-schema validation cannot open its required
Unix socket in this execution sandbox (`socket: operation not permitted`), so exact-head GitHub
Infrastructure validation remains mandatory rather than inferred or bypassed.

## 2026-10-06 exact-head budget qualification and successor-plan repair

The modern budget-shape repair was published without rewriting history at exact head
`ac39316f43e217e8ef04f31c7861154da0a38394`, tree
`1ba19c0ae56b85a90441a65ee4367a312961b59c`. Exact-head CI run `37413701529` passed. Stage 6 AWS
Qualification run `37413701426` passed both qualification and historical recovery. Its sanitized
qualification receipt SHA-256 is
`a58774097515d821a14d6af219901c73f3a218629f13febf1479cec238dd6dcb`.

The live cost subtotal was USD 14.752928 and the unchanged twenty-percent-margin envelope was USD
17.703514 against the USD 25 project ceiling. The applicable current gross-cost budget had a USD 20
limit, USD 0.806 actual-or-forecast spend, USD 19.194 available headroom, and USD 1.490486 remaining
buffer after the project envelope. Residual Stage 6 inventory was empty and the backend remained the
initial serial-zero state. No Terraform plan/apply, runtime mutation, or managed workload occurred in
that qualification. Infrastructure inputs were byte-identical to the parent exact head whose
Infrastructure run `37411791744` passed.

The historical lease is expired but its fixed object key already exists. Therefore the original
initial-only `If-None-Match: *` procedure cannot create current plan authority. Deleting or silently
overwriting the object would destroy the concurrency/evidence contract. The separately authorized
repair adds a fail-closed successor protocol: require the expired current lease, unchanged empty
inventory and serial-zero state; perform one AES-256 `PutObject` with `If-Match` against the exact
ETag; preserve immutable version history; and stop without blind retry on any unknown outcome.
Historical recovery continues to select and verify the pinned original lease version even after a
successor exists.

The plan-only executor keeps Terraform data, backend/variable inputs, the binary plan, and raw plan
JSON outside the repository. It verifies Terraform 1.9.8 and backend-disabled validation before the
single write, rebuilds artifacts twice, rechecks live preconditions, runs only refresh-aware unlocked
planning, validates the normalized allowlist/invariants, and proves the lease, state bytes, empty
inventory, and worktree did not change. Apply, destroy, import, IAM change, deployment, and managed
workload paths remain absent and unauthorized. This repair requires complete local validation,
publication, and a new exact-head qualification before the one authorized lease transition may run.

## 2026-10-06 saved-plan finalization repair

The authorized successor transaction ran once from exact source
`8fc29d36f39a8cb8105f004b594bbedea8292d77`, tree
`41814b95c2e5dde78e9cf0d3b62a7ad7a9b455a2`, after exact-head CI run `37416197471` and Stage 6
AWS Qualification run `37416197533` passed. Qualification receipt SHA-256 is
`a74c42fdbf7fab362e34f29b43d524ea2383c2e71562374efb05bbc7e3ae6199`.

The executor completed the sole conditional lease write and generated the refresh-aware saved plan,
then failed during normalization because the allowlist did not include the already-reviewed S3
lifecycle-configuration type. Read-only inspection found exactly 47 creates, including lifecycle
resources for `artifacts`, `evidence`, and `offline`; Terraform 1.9.8 timestamped the plan
`2026-10-06T05:37:10Z`. The successor lease was acquired at epoch `1791265021`, expires at
`1791268321`, and is the only version added to the preserved historical lease. No state mutation,
managed resource creation, workload, apply, destroy, or import occurred.

This repair closes the validator defect with exact lifecycle semantics and provides a saved-plan
finalizer that cannot repeat either the lease write or Terraform plan. Publication, exact-head remote
quality checks, and execution of that read-only finalizer against the preserved private CloudShell
files remain required. The generated evidence will be historical plan proof and will not claim
current deployment authority.
