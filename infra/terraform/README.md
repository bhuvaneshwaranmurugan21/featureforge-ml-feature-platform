# AWS reference infrastructure

This module defines encrypted/versioned offline and evidence buckets, Glue catalog database,
DynamoDB definition/generation/online/pointer tables, a disabled-by-default EventBridge schedule,
a Step Functions control-flow contract, CloudWatch logs, and a parity alarm.

The state machine intentionally contains adapter placeholders and the schedule defaults to
disabled. `terraform validate` proves configuration shape only; it does not prove Spark output,
DynamoDB latency/capacity, parity, model behavior, availability, or cost. Follow the managed
execution runbook before changing the claim registry.

```bash
terraform init
terraform plan
```
