# Stage 5 bounded performance analysis

The frozen protocol ran nine cases: three sizes (`4/12`, `8/24`, and `12/48` customers/events) across balanced, skewed, and high-cardinality distributions. Each of nine operations retained one cold and five warm trials, for 486 raw trials and 81 summaries. All correctness gates passed. High-cardinality cases deliberately crossed the affected-scope threshold and used full fallback; the other profiles exercised incremental mode.

Warm local medians ranged from 1.315–1.628 seconds for Spark full rebuild and 1.293–1.546 seconds for the incremental/fallback operation. The small bounded workloads are dominated by Spark job scheduling/startup, so they do not support a speedup claim. Non-Spark medians ranged from sub-millisecond planning/parity/read decisions to 4.434 ms for the largest observed online materialize/reconcile case.

Variability is retained. The maximum coefficient of variation was 0.525 for small-skewed online materialization; generation-pinned reads reached 0.518 in one case. Those short local operations are sensitive to scheduler and filesystem noise, so only the raw environment-bound observations are claimed. Spark warm CV remained at or below 0.106 for full builds and 0.096 for incremental/fallback builds in this run.

Captured partition row counts and maximum-partition fractions expose skew; affected-pair counts and mode expose work avoided versus fallback. The bounded sizes intentionally stay far below the Stage 3 safety ceiling. No extrapolation to driver-safe production scale, AWS, Glue, DynamoDB latency, throughput, cost, availability, or an SLO is made.
