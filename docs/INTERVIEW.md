# Interview defense

## Two-minute explanation

An event can occur before prediction time but be corrected after prediction time. Event-time-only
joins leak that later knowledge into historical training. FeatureForge stores event and knowledge
time, selects the revision knowable at the label cutoff, binds output to immutable definition
digests, materializes an isolated generation, and publishes online state only after exact parity.

## Questions to expect

1. **Why two clocks?** Event time answers when the fact happened; knowledge time answers when the
   platform was entitled to use that revision.
2. **How are datasets reproduced?** Labels, cutoff, definition digests, row digest, and source
   revision boundary are captured in a manifest.
3. **How do offline and online values stay consistent?** Both carry entity, feature, timestamp,
   value, definition digest, and generation; publication fails on any mismatch.
4. **What happens during backfill?** A new generation is built and verified without mutating the
   active generation.
5. **How do you avoid stale serving?** TTL is checked at read time and missing is explicit.
6. **What ran on AWS?** Only a bundle passing `validate_aws_lab_evidence` supports an AWS-lab claim.

