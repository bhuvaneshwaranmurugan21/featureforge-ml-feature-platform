# Cost controls

- DynamoDB tables use on-demand capacity for the small, intermittent lab.
- The schedule is disabled by default; no unattended materialization is allowed.
- Spark compute is created for one bounded run and stopped immediately.
- S3 holds synthetic, generation-scoped data with an explicit teardown path.
- No NAT gateway, SageMaker endpoint, MSK cluster, or permanent EMR cluster is required.
- Cost and teardown are mandatory fields in the AWS evidence contract.

Pre-run calculator estimates and post-run measured cost are recorded separately. Estimates are
never presented as actual spend.

