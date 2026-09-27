# Stage 4 failure and repair log

## Admission

The authorized main commit and tree matched. Every predecessor validator and deterministic proof passed byte-for-byte before Stage 4 dependencies or implementation changed.

## AWS dependency probe

The minimum pinned set qualified as Boto3 1.43.103, Botocore 1.43.103, JMESPath 1.1.0, and S3Transfer 0.19.2. Installed transitive versions were Python-dateutil 2.9.0.post0, six 1.17.0, and urllib3 2.8.0. Dependency integrity, the complete predecessor suite, and all predecessor proofs passed after installation. Package metadata reported Apache-2.0 for the AWS SDK packages and MIT for JMESPath, Python-dateutil, six, and urllib3.

## First Stage 4 validation

The first focused run passed all semantic tests and strict typing but Ruff found one unused import, three long lines, and one loop-index style issue. These were repaired directly. Running only the focused file also triggered the repository-wide 85% coverage threshold; this was not bypassed. The complete suite was run and reached 91.26% before later Stage 4 additions.

## Predecessor evidence drift

The complete suite correctly rejected the changed dependency lock and project metadata through stale Stage 0 and Stage 1 evidence indexes. The indexes are regenerated only after final content and every predecessor validator are rechecked; the gate is preserved.

## Design review corrections

The first request builder represented an initial pointer using a synthetic `NONE` value. Review found that this depended on an undeclared pre-created item. The activation transaction was corrected to conditionally create an absent pointer on version zero, while later transitions require the exact active generation and version.

The first local reconciliation used one ordered database read. Review found that this did not exercise the declared paginated access contract. It was replaced by deterministic keyset pagination with restart cursors, and reconciliation now consumes that traversal.

Final lifecycle review found that a direct reconciliation call after activation could rewrite the
candidate state to `VALIDATED`. Reconciliation was made state-preserving for already validated,
active, and retired candidates, while still rechecking records and the immutable receipt. The same
review made digest-valid but structurally invalid typed values return `CORRUPT_RECORD`.

## Packaging probe

The first wheel probe could not import `setuptools.build_meta` because the disposable environment
did not include the repository-declared build requirement. `setuptools>=69` was installed into that
disposable environment (qualified version 84.0.0), after which an isolated no-dependency wheel build
passed. No project requirement or acceptance gate was weakened.
