# Workload and capacity model

The bounded lab uses synthetic entities, versioned payment events, deterministic labels, and a
small set of feature definitions. It records source revisions, label rows, feature rows, build
runtime, read latency, parity counts, DynamoDB capacity use, and recovery time.

Production sizing separates batch build pressure from online serving pressure. Offline compute is
driven by label cardinality, event history scanned, feature windows, and revision density. Online
capacity is driven by entity/feature request distribution and item size; averages cannot hide hot
entities. TTL is a business freshness rule, not merely a storage optimization.

No training scale, serving SLO, or model-quality improvement is claimed without measurements.

