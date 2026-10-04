# Stage 6 autonomous recovery checkpoint

Status: incomplete; no Stage 6 PR was opened or merged.

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
