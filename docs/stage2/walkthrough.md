# Interview walkthrough: the FeatureForge local lifecycle

Start with `tests/fixtures/stage2-lifecycle.json`. It has two real entities plus an empty entity, five payment-risk features, a 24-hour boundary event, a late amount correction, and a later retraction. The source and labels become immutable, content-addressed artifacts; definitions are independently durable by name/version and full digest.

The historical `c-1` row is calculated at event cutoff `200000` and knowledge cutoff `180000`. The exact 24-hour lower-bound event is excluded from the count, the late correction is not yet knowable, the upper event is not yet knowable, and the later retraction is not yet knowable. The resulting 24-hour count is `3`. Both production and the primitive-record oracle produce the same entire training row.

At current knowledge `200005`, the correction and upper event are knowable, so the count is `4`. At `200020`, the retraction removes `p-3`, so the count is `3`. The earlier training artifact digest is reread after both builds and remains unchanged.

Each current view is written under an isolated generation. A deliberately corrupted expected-current artifact yields a persisted failing validation receipt and `FAILED`; its attempted publication cannot move the active pointer. Valid candidates reach `READY` only through stored count, digest, artifact-link, and row equality checks.

Two independent SQLite connections read `generation-base@1`. `generation-a` commits; `generation-b` then reaches the semantic CAS predicate and is rejected because the pointer is no longer the predecessor it read. This is deliberately not presented as a generic lock-error test.

A reader pins `generation-a@2` and reads count `4`. `generation-current` commits at version `3` after the retraction. The old token still reads `4`, while a new request reads `3`; one logical request never mixes generations.

The activation of `generation-current` deliberately loses its acknowledgement after commit. After closing and reopening SQLite, the same operation ID and request return the stored version-3 receipt. The pointer does not increment again. Reusing the same operation ID for rollback content fails with an idempotency conflict.

Rollback to the retained, validated `generation-a` is a new operation and advances the pointer to version `4`. A stale rollback carrying version `3` fails and leaves version `4` intact. Pointer history is append-only.

Run the evidence yourself:

```bash
python -m tools.run_stage2_proof --output-dir /tmp/featureforge-stage2-evidence
cmp /tmp/featureforge-stage2-evidence/vertical-slice-proof.json evidence/stage2/vertical-slice-proof.json
cmp /tmp/featureforge-stage2-evidence/failure-recovery-proof.json evidence/stage2/failure-recovery-proof.json
python tools/validate_stage2.py
```

The defensible claim is intentionally narrow: these commands prove the bounded local Python/SQLite semantics and failure controls. They do not prove AWS, Spark, DynamoDB, distributed transactions, scale, performance, availability, or cost.
