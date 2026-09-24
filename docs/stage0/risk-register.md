# Stage 0 risk and failure register

| Risk / trigger | Detection | Response and closure evidence |
| --- | --- | --- |
| `main` or draft PR changes while auditing | Re-fetch SHA/tree and PR head before publishing and merging | Reconcile overlap, rebuild candidate and receipts; never overwrite another branch. |
| Branch checks are optional | Branch/ruleset read and PR required-check view | Establish PR/check enforcement before merge; exact-head green alone is insufficient to claim protected merge. |
| CI uses Python 3.11 while local runner has 3.12 | Interpreter and dependency versions in baseline | Preserve local result as 3.12; require exact-head 3.11 CI and do not equate the two environments. |
| Floating dev dependency versions change baseline | Clean install versions and lock/constraints | Pin the resolved dependency graph and run a fresh install plus CI. Retain earlier results. |
| Historical 14-test claim is attributed to current main | Compare PR #1 files and base/head, current tests | Report 12 tests on current main; draft adds two tests and remains unmerged. |
| Source cutoff lacks immutable snapshot digest | Inspect `dataset.py` manifest and source ingestion | Label same-input deterministic rerun accurately; require pinned input proof in the later correctness work. |
| Shared calculation hides a correlated parity bug | Trace training and local materialization calls | Do not call the existing parity comparison an independent oracle; specify separate reference proof. |
| Ambiguous clock, tie, or correction semantics | Inspect contracts, selection code, boundary fixtures | Keep explicit open decisions; freeze semantics before modifying behavior. |
| Fixture or claim copied from another repository | Diff, file provenance, project identity validator | Remove contamination, redo evidence and review; use only FeatureForge paths. |
| Evidence includes secrets or account details | Diff and secret-pattern review | Redact before push; assess exposure and regenerate receipts. |
| Test or validator gate fails | Exact command/exit/log and minimized fixture | Fix root cause, rerun clean; no skip, threshold reduction, or policy override. |

Each risk remains open until its stated proof is recorded. A risk assigned to a
later stage does not become a Stage 0 success claim.
