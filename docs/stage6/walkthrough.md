# Stage 6 skeptical interview walkthrough

Start with the claim boundary: Stage 6 proves deployability and a reviewed plan, not runtime success.
Show `ManagedRunManifest` and explain why bucket/key alone is insufficient without object version,
checksum, source tree, artifact digest, region, lease, and budget authority. Compare the production
admission result with the independent oracle and mutate account, region, lease, residual inventory,
quota, cost, and every saved-plan binding.

Then trace one candidate through the state machine: manifest validation, exact Glue input, immutable
offline output, isolated online writes, strong paginated reconciliation, independent parity digest,
conditional activation, and completion receipt. Point out quarantine, bounded retry, task ledger,
disabled EventBridge schedule, and concurrency one.

Inspect IAM by principal rather than by service. Explain why the plan role cannot apply, why runtime
roles cannot administer infrastructure, and why the few wildcard resources exist only for APIs that do
not support resource scoping. Show provider/action pins, deterministic zip reproduction, the private
binary-plan rule, public sanitization, and the inverse cleanup matrix.

Finish with the non-claims. A green Stage 6 authorizes only the exact unexpired reviewed plan. Stage 7
must still apply a separately authorized tiny slice and capture real managed behavior; Stage 8 must
prove failure recovery and teardown.
