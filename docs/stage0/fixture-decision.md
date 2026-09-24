# Product and fixture decision

Keep the existing payment-risk product. `contracts/payment-event-v1.json`,
`src/featureforge/definitions.py`, and `src/featureforge/simulator.py` already
define the source and five features. The synthetic `p-1` late correction is
traceable in the code and gives a useful historical-knowledge counterexample.
It has no dependency on another portfolio repository, despite sharing a broad
business domain with other work.

For the next local correctness proof, retain the same stable event and customer
IDs and create two explicit prediction requests on the same customer, one
before and one after the correction's knowledge time. Show event membership,
eligible revisions, and expected feature values by hand. Keep the historical
snapshot and the current online view separately identified. Add an independent
reference oracle rather than certifying two paths that share
`featureforge.computation` as independent implementations.

Decisions still requiring a temporal specification include the meaning and
trust source for `knowledge_time`, equal-time conflicting revisions, correction
that changes event time or entity, source snapshot/frontier binding, and whether
prediction knowledge cutoff can differ from label event time. Record the chosen
rules before changing the kernel; no inferred rule in this audit is final.

The existing `pytest`, `ruff`, `mypy`, and 17-check simulation form the baseline.
The Stage 0 audit has not executed managed Spark, Glue, DynamoDB, or any AWS
workload, and does not assert their runtime behavior.
