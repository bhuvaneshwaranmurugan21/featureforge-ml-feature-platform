# Stage 6 cost authority

Authorization sets `FF_MAX_COST_USD=25` and a mandatory twenty-percent safety margin. Calculations use
integer micro-US-dollars and round each extended line upward. Admission requires:

`ceil(subtotal × 1.20) <= 25,000,000 micro-USD`

The exact worksheet must use current authorized-region prices for two Glue `G.1X` workers for at most
fifteen minutes, Lambda requests/duration at concurrency one, Step Functions Standard transitions,
DynamoDB on-demand requests and bounded storage, versioned S3 requests/storage, KMS requests,
CloudWatch logs/metrics/dashboard, and any retained catalog metadata. Free-tier or credits may be
reported separately but may not reduce worst-case cost.

Stage 6's deterministic local fixture demonstrates arithmetic only; it is not current AWS pricing.
The read-only AWS gate must capture the pricing observation time, budget/credit headroom, quantities,
unit prices, subtotal, margin, and final ceiling decision. Any missing price is conservatively bounded
or blocks planning. A configuration change invalidates the worksheet and saved plan.
