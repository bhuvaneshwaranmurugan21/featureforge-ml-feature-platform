# Stage 2 failure and repair log

## Rehearsal: invalid training label clock

- Failing invariant: a historical training label cannot request knowledge after its label time.
- Minimized reproducer: event cutoff/label time `200`, prediction knowledge cutoff `205`.
- Root cause: current-view cases had been treated as historical labels even though current serving and labeled prediction rows have different clock constraints.
- Repair: the training snapshot uses the legitimate `180000` historical cutoff; `200005` and `200020` are explicit current-view materializations.
- Regression authority: `tests/fixtures/stage2-lifecycle.json`, independent training/current oracle comparisons, and the Stage 1 label invariant remain unchanged.

## Rehearsal: optional cutoff at strict type boundary

- Failing invariant: strict mypy must prove a normalized optional label cutoff is non-null before `min`/`max`.
- Root cause: runtime normalization in `Label.__post_init__` does not narrow the static `int | None` type.
- Repair: the provenance boundary rejects any non-normalized value and builds a typed list before calculating frontiers.
- Regression authority: strict mypy and label-snapshot tests.

## Implementation: post-validation mutation window

- Failing invariant: a candidate changed after validation must not publish.
- Root cause: the first control-plane draft checked that a receipt existed but did not recalculate candidate bytes inside the publication transaction.
- Repair: publication now verifies receipt digest, `matched`, current count, current digest, and generation-manifest count/digest before CAS.
- Regression authority: `test_post_validation_tamper_is_detected_before_cas`.

## Implementation: persisted artifact trust

- Failing invariant: reopening an artifact must not trust partial or tampered rows merely because the header exists.
- Root cause: the initial read path reconstructed an artifact without recalculating row count, row digest, and enclosing artifact digest.
- Repair: every artifact read recalculates and compares all three identities and raises `ArtifactConflict` on mismatch.
- Regression authority: Stage 2 provenance and failure-contract tests.

No validation, coverage threshold, branch protection, prior-stage rule, or claim boundary was weakened for these repairs.
