# Stage 1 failure and repair log

## Pre-existing equal-time ambiguity

The predecessor selector replaced an event only when a later record had a strictly greater
`knowledge_time`. Two conflicting revisions with the same logical event and knowledge time were
therefore resolved by input order. Stage 1 replaced this with explicit immutable revision IDs,
exact-replay normalization, and deterministic failure for a same-clock conflict.

## Retraction representation gap

The v1 model required a payment payload and had no operation type, so deletion could only be
faked or omitted. Stage 1 introduced a v2 typed tombstone whose payload must be null and retained
all earlier revisions for historical reconstruction.

## Golden-fixture scope mismatch found during implementation

The first golden assertion incorrectly omitted the lower-bound event from the temporal-selection
trace. Temporal selection applies the event cutoff; the feature computation applies its own
window. The fixture was repaired to retain the event in the selected-revision trace while proving
that every 100-second feature excludes it. No production rule or assertion was weakened.

## Simulator type probe exposed by definition-bound defaults

Binding empty defaults into the definition caused the existing injected integer/ratio mismatch
to return a null default because its 100-second window was empty. The probe was repaired to use
an unbounded window, ensuring it exercises the intended float result against an integer contract.
The type gate remains strict.
