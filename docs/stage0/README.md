# FeatureForge Part 1 Stage 0 — exact-state audit and completion contract

The audited source is `bhuvaneshwaranmurugan21/featureforge-ml-feature-platform` at
`85d2cf08ecfbd75d88192e55a8a6db9a6ad55397`, tree
`b5c3ba78228801e7e42a8c219235afb2919401b4`. This is the **base**, not the
Stage 0 result. The final PR head, CI run IDs, merge commit/tree, and independent
post-merge proof belong in an external continuation receipt to avoid a commit
containing its own hash.

## Decision and scope

FeatureForge already contains a payment-risk feature product with five definitions,
a versioned payment-event contract, a SQLite reference store, and a deterministic
17-check simulation. Preserve that product and its fixture provenance. This audit
does not substitute a generic new dataset or import data or artifacts from another
project. It inventories and corrects current claims, captures a reproducible
baseline, and fixes the Part 1 requirement/evidence contract. Temporal oracle and
local operational improvements remain unclaimed until executed and verified.

No AWS apply, workload, IAM change, release, or tag is included. The OIDC identity
workflow and static Terraform validation are different evidence classes from a
managed FeatureForge workload. A local SQLite transaction proves only the local
publication behavior described in the observed test or simulation.

## Evidence map

- `baseline.json`: exact-base environment, commands, outcomes, CI references, and
  historical-versus-current distinction.
- `inventory.json`: code-path classification and open gaps, with each item tied to
  the exact base commit.
- `claims.json`: prominent public statements, evidence labels, limitations, and
  correction decisions.
- `requirements.json`: `FF-P1-01` through `FF-P1-10`, owner stage, proof, negative
  control, and dependencies. `planned` is not a passing test.
- `risk-register.md` and `fixture-decision.md`: failure response and product scope.
- `evidence-index.json`: SHA-256 digests of the bounded Stage 0 files. It excludes
  itself, so the digest graph is acyclic.

Run `python tools/validate_stage0.py` after editing any indexed file, then
regenerate the index with `python tools/validate_stage0.py --write-index`. The
validator verifies project/base identity, complete requirement coverage, evidence
labels and pointers, and artifact digests. Its negative controls are in
`tests/test_stage0_contract.py`. The normal test and simulation commands remain
required; this validator adds a contract gate and does not replace them.

## Acceptance and current limits

`ST0-AC-01` through `ST0-AC-04` require exact source, reproducible baseline,
truthful public claims, and a complete requirement-to-proof map. `ST0-AC-05`
requires an exact-head PR with passing quality checks, verifiable merge
provenance, independent merged-main checks, and a durable continuation receipt.
Human approval is useful when an independent reviewer is available, but is not
a Stage 0 completion criterion for this solo repository. A revert and reapply
solely to manufacture a second merge would add no technical proof.

PR #2 merged at `b531116eb1983d25514529b4fda37b4519f29cc4` before branch
protection was configured and without an approving review. Its head was
`2fd5f7a56ba4fd721a660c1dca9f408205f13203`; the `quality` PR check
passed, and the merged-main `quality` run `36031977394` passed. Local
merged-main validation and two fresh byte-identical simulations are recorded
in the external continuation receipt. The later protection rule cannot apply
retroactively to PR #2. Future PRs should retain the required `quality` check
and branch protection, without a mandatory second-person approval.

The open draft PR #1 (`agent/production-grade-foundation`, head
`31c155b4f3a2776a89fd9157380ff45152e70d3c`) adds AWS-evidence code,
documentation, Terraform lock, and CI changes. It is unmerged and is neither
adopted as current `main` evidence nor changed by this Stage 0 branch. Its small
claim/CI overlap is documented in the Stage 0 PR for later reconciliation.

The source currently selects the latest revision known by a cutoff and applies
business-time windows. The dataset manifest has `dataset_as_of`, definition
digests, row count and rows digest, but no immutable input snapshot/content
digest. Serving and training use one computation library, so local parity of
the same implementation cannot independently establish correctness. These are
explicit proof gaps, not evidence that the current local test run failed.
