# Stage 3 interview walkthrough

Start with `spark-incremental-specification.md` and state the two independent clocks: business time asks
whether an event belongs in the feature window; knowledge time asks whether that revision was knowable.
Then open `tests/stage3_oracle.py` to show that the comparator imports no production temporal or
computation code. In `spark_runtime.py`, trace revision ranking, the open-lower/closed-upper join,
separate integer/float aggregation columns, canonical output, and digest-bound diagnostics.

For incremental backfill, use the committed frontier sequence `180000 -> 200005 -> 200020`. A late
correction, a newly knowable upper-bound event, and a later retraction change only customer `c-1`.
The affected-scope plan conservatively marks its five feature keys. Spark recomputes those five keys,
preserves ten keys for `c-2` and `c-empty`, re-envelopes all fifteen rows at the target frontier, and the
result exactly equals a full rebuild.

Finish with the recovery proof: exact replay is idempotent; an incomplete staging result is unreadable;
row tampering is detected; a corrupt predecessor is rejected; and an intentionally low scope threshold
selects a full rebuild. Be explicit that the three seeded workload profiles and partition diagnostics are
bounded local correctness evidence, not production performance evidence.
