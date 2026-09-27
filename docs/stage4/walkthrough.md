# Stage 4 technical walkthrough

Start with the Stage 3 immutable offline generation. `build_materialization_plan` validates the definition/value relationship and creates one canonical online record for each of five features across three customers. The record keys include the generation, so these 15 records cannot affect serving until activation.

`LocalOnlineStore.materialize` durably admits the operation, writes only missing records, and reconciles through keyset pages. It issues a validation receipt only when every expected key and content digest matches exactly. The DynamoDB boundary separately renders each record and control item as low-level attribute maps and validates PutItem, Query, and TransactWriteItems inputs with Botocore's pinned DynamoDB model.

Activation consumes the exact validation receipt and expected pointer state. One SQLite transaction checks the candidate again, compare-and-swaps the pointer, records history, and persists the activation receipt. The concurrency test opens independent connections and proves one winner. The lost-ack test closes and reopens the database before replaying the same operation and receiving the existing receipt.

The reader pins `feature_set`, `generation_id`, and `pointer_version`. Old and new reader tokens remain internally consistent across a pointer switch. Eligibility checks happen on every read: expiry equality is expired, freshness equality remains eligible, and corrupt or unavailable state is never reported as absence.

The deterministic proofs are the shortest interview path: the behavior proof shows record/oracle equality, request-model validation, reconciliation, activation, serving, and capacity; the recovery proof shows interruption, keyset resume, lost acknowledgement, concurrency, retry scheduling, corruption rejection, and terminal classifications. The claims file states the bounded conclusion and the AWS-runtime non-claims.
