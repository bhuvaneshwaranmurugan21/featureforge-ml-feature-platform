# Stage 5 failure and repair log

The authorized main commit/tree and Stage 4 receipt matched, dependency integrity passed, and all 105 predecessor tests, validators, and proof comparisons passed before implementation.

The first focused lint run found one unused import and one overlong test line. Both were corrected; no lint rule or coverage gate changed. The first deterministic proof run exposed an incorrect assumption that the non-Spark `materialize` result had a `.values` attribute. The proof was repaired to compare its tuple directly, then regenerated twice byte-identically.

The benchmark harness initially failed Ruff's loop-closure rule. Immediate lambdas were replaced by bound `partial` calls and explicit helpers; the rule was not suppressed. The frozen 3-by-3 measurement then completed unchanged with 486 correct trials. High variability in short local operations was retained and documented rather than filtered.

Injected proof failures include semantic/envelope corruption, staleness, all nine policy gates, rejected-pointer preservation, and a decision made stale before CAS. Repair uses a new valid receipt and public activation path; no database state is edited.
