# Stage 6 plan-only AWS graph

This module defines the minimal encrypted and bounded resource graph for one FeatureForge managed
qualification: versioned artifact, offline, and evidence buckets; a Glue 5.0 job; a Python 3.12
control Lambda; control and online DynamoDB tables; Standard Step Functions orchestration; disabled
EventBridge dispatch; CloudWatch logs, alarms, and dashboard; one KMS key; scoped runtime roles; and
an exact-repository OIDC planning role.

Terraform and the AWS provider are exactly pinned. Initialization and static validation are safe
locally because they use no AWS credentials:

```bash
terraform init -backend=false -lockfile=readonly
terraform fmt -check -diff -recursive
terraform validate
```

An actual plan requires the explicit authorized region, run ID, existing GitHub OIDC provider ARN,
private backend configuration, deterministic archives, read-only AWS qualification, a current lease,
and a current cost envelope. The binary saved plan, state, backend inputs, account identifiers,
credentials, and session material are private and must never be committed.

Stage 6 permits refresh-aware planning only. It does not authorize `apply`, `destroy`, `import`,
state mutation, resource mutation, Lambda invocation, Glue execution, or Step Functions execution.
