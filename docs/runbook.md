# Managed execution and publication runbook

## Preconditions

- Feature owners approve versioned definitions, types, TTLs, null policy, and owners.
- Source contract and source frontier are pinned.
- Dataset labels have documented event and availability timestamps.
- Capacity, hot-key distribution, and DynamoDB quotas are reviewed.
- Previous online generation remains retained and readable.

## Execute

1. Create run and generation IDs; record commit, definition-set digest, region, and operator.
2. Capture source snapshot/frontier plus `dataset_as_of`.
3. Build a training dataset and persist its manifest before model consumption.
4. Materialize the new offline generation; capture Iceberg snapshot and Spark run IDs.
5. Stage the online generation under generation-prefixed keys.
6. Sample and fully compare critical features across both stores.
7. Inject one late correction and prove a pre-correction label does not change.
8. Inject one staged online mismatch and prove publication is blocked.
9. Repair/rematerialize, re-run type, freshness, null-rate, distribution, and parity gates.
10. Conditionally swap the online pointer and observe latency, errors, missing rate, and skew.

## Roll back

1. Stop materialization/publication workflows.
2. Read the current pointer version and last proven generation.
3. Conditionally swap the pointer to the previous generation.
4. Validate model request success, freshness, missing rate, and sampled parity.
5. Preserve the failed generation and evidence for incident review.

## Required evidence before production claims

- run/generation IDs and definition digests;
- deployed resource identifiers and Terraform plan/apply output;
- source and Iceberg snapshot frontiers;
- Spark/Step Functions run links and CloudWatch logs;
- late-correction leakage test and online mismatch/recovery proof;
- dataset and parity manifests;
- measured offline build time, online write rate, p50/p95/p99 read latency, missing rate;
- item count/bytes, hot-key analysis, and actual AWS cost;
- rollback and teardown evidence.
