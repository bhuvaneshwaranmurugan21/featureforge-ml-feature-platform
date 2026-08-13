# Failure lab

Run `make evidence`. The command exits non-zero unless every check passes.

| Scenario | Injection | Required proof |
|---|---|---|
| Future event | Event occurs after label | Training feature excludes it |
| Late correction | Revision arrives after label | Historical row retains the earlier knowable revision |
| Definition drift | Change an existing version | Registry rejects the digest conflict |
| Duplicate revision | Replay identical identity and payload | Operation is idempotent |
| Conflicting revision | Same identity, changed payload | Write is blocked |
| Dataset rerun | Repeat with same manifest inputs | Manifest digest is identical |
| Missing entity | Materialize unknown customer | Explicit zero/null policy applies |
| Materialization replay | Write same values again | No mutation; replay result returned |
| Parity mismatch | Alter staged online value | Exact comparison identifies mismatch |
| Premature publication | Mark mismatched generation ready | Gate blocks transition |
| Stale publisher | Use old active-pointer version | Conditional write fails |
| TTL expiry | Read at expiry boundary | Serving returns missing |
| Type mismatch | Float calculation declared integer | Contract blocks value |
| Backfill | Build historical generation | Active pointer remains unchanged |

The lab verifies reference semantics, not Spark, Glue, S3, DynamoDB, or Step Functions.
