"""Deterministic local Spark computation and incremental-backfill authority."""

from __future__ import annotations

import json
import random
import re
import shutil
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, cast

from featureforge.canonical import canonical_json, digest
from featureforge.model import FeatureDefinition, FeatureValue, PaymentEvent
from featureforge.temporal import TemporalContractError, normalize_events

SUPPORTED_COMPUTATIONS = {
    "transaction_count",
    "successful_spend_cents",
    "failed_payment_ratio",
    "hours_since_success",
    "max_merchant_risk",
}
COMPUTATION_TYPES = {
    "transaction_count": "integer",
    "successful_spend_cents": "integer",
    "failed_payment_ratio": "float",
    "hours_since_success": "float",
    "max_merchant_risk": "float",
}
ARTIFACT_CONTRACT = "spark-offline-generation-v1"
GENERATION_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
MAX_LOCAL_INPUT_ROWS = 1_000_000
MAX_LOCAL_OUTPUT_ROWS = 100_000


class SparkContractError(ValueError):
    """Spark input or output cannot be interpreted without weakening the contract."""


class GenerationArtifactError(RuntimeError):
    """An immutable offline generation is incomplete, conflicting, or corrupt."""


def _validate_generation_id(generation_id: str) -> None:
    if not GENERATION_ID_PATTERN.fullmatch(generation_id):
        raise SparkContractError("generation identity must be path-safe and non-empty")


@dataclass(frozen=True)
class SparkDiagnostics:
    input_rows: int
    authoritative_rows: int
    output_rows: int
    input_partitions: int
    output_partitions: int
    partition_row_counts: tuple[int, ...]
    maximum_partition_fraction: float
    physical_plan_digest: str
    cartesian_product: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SparkBuild:
    generation_id: str
    values: tuple[FeatureValue, ...]
    manifest: dict[str, Any]
    diagnostics: SparkDiagnostics


@dataclass(frozen=True)
class AffectedScopePlan:
    base_knowledge_cutoff: int
    target_knowledge_cutoff: int
    mode: Literal["incremental", "full"]
    affected_pairs: tuple[tuple[str, str], ...]
    reasons: tuple[str, ...]
    source_digest: str
    plan_digest: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Stage3Workload:
    profile: Literal["balanced", "skewed", "high-cardinality"]
    seed: int
    customer_ids: tuple[str, ...]
    events: tuple[PaymentEvent, ...]
    workload_digest: str


def generate_workload(
    profile: Literal["balanced", "skewed", "high-cardinality"],
    *,
    seed: int,
    customers: int,
    events: int,
) -> Stage3Workload:
    """Produce a deterministic, replayable Stage 3 workload without external generators."""

    if customers < 1 or events < 1:
        raise ValueError("workload sizes must be positive")
    if profile not in {"balanced", "skewed", "high-cardinality"}:
        raise ValueError("unsupported workload profile")
    generator = random.Random(seed)
    customer_ids = tuple(f"customer-{number:05d}" for number in range(customers))
    values: list[PaymentEvent] = []
    for number in range(events):
        if profile == "high-cardinality" and number < customers:
            customer_id = customer_ids[number]
        elif profile == "skewed" and generator.random() < 0.8:
            customer_id = customer_ids[0]
        else:
            customer_id = customer_ids[generator.randrange(customers)]
        event_time = 100_000 + number * 7
        values.append(
            PaymentEvent(
                event_id=f"event-{number:08d}",
                revision_id=f"event-{number:08d}-r1",
                customer_id=customer_id,
                event_time=event_time,
                knowledge_time=event_time + generator.randrange(0, 4),
                operation="upsert",
                amount_cents=generator.randrange(0, 100_001),
                status="failed" if generator.randrange(0, 5) == 0 else "succeeded",
                merchant_risk=generator.randrange(0, 10_001) / 10_000,
            )
        )
    if profile == "high-cardinality" and events < customers:
        raise ValueError("high-cardinality workload requires events >= customers")
    rendered = tuple(event.as_dict() for event in values)
    return Stage3Workload(profile, seed, customer_ids, tuple(values), digest(rendered))


def create_local_spark(app_name: str, *, shuffle_partitions: int = 4) -> Any:
    """Create the qualified loopback-only local Spark runtime."""

    if shuffle_partitions < 1:
        raise ValueError("shuffle_partitions must be positive")
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:  # pragma: no cover - installation failure path
        raise RuntimeError("install the pinned spark extra") from error
    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )


def validate_inputs(
    definitions: Sequence[FeatureDefinition],
    events: Iterable[PaymentEvent],
    customer_ids: Sequence[str],
    *,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> tuple[tuple[FeatureDefinition, ...], tuple[PaymentEvent, ...], tuple[str, ...]]:
    """Validate and canonicalize the complete Spark build envelope."""

    if event_cutoff < 0 or knowledge_cutoff < 0:
        raise SparkContractError("cutoffs must be non-negative")
    defs = tuple(definitions)
    if not defs:
        raise SparkContractError("at least one feature definition is required")
    identities: set[str] = set()
    names: set[str] = set()
    for definition in defs:
        if definition.definition_id in identities or definition.name in names:
            raise SparkContractError("feature definitions must have unique identities and names")
        if definition.computation not in SUPPORTED_COMPUTATIONS:
            raise SparkContractError(f"unsupported computation: {definition.computation}")
        if definition.value_type != COMPUTATION_TYPES[definition.computation]:
            raise SparkContractError(
                f"computation {definition.computation} requires "
                f"{COMPUTATION_TYPES[definition.computation]} output"
            )
        identities.add(definition.definition_id)
        names.add(definition.name)
    customers = tuple(sorted(set(customer_ids)))
    if len(customers) != len(customer_ids) or any(not value for value in customers):
        raise SparkContractError("customer identities must be unique and non-empty")
    try:
        normalized = normalize_events(events)
    except TemporalContractError as error:
        raise SparkContractError(str(error)) from error
    if len(normalized) > MAX_LOCAL_INPUT_ROWS:
        raise SparkContractError("local Spark input exceeds the explicit bounded-row limit")
    if len(customers) * len(defs) > MAX_LOCAL_OUTPUT_ROWS:
        raise SparkContractError("local Spark output exceeds the explicit driver-row limit")
    return tuple(sorted(defs, key=lambda value: value.name)), normalized, customers


def _event_rows(events: Sequence[PaymentEvent]) -> list[dict[str, Any]]:
    return [event.as_dict() for event in events]


def _definition_rows(definitions: Sequence[FeatureDefinition]) -> list[dict[str, Any]]:
    return [
        asdict(definition) | {"definition_digest": definition.definition_digest}
        for definition in definitions
    ]


def _canonical_plan(plan: str) -> str:
    value = re.sub(r"#\d+[A-Z]?", "#?", plan)
    value = re.sub(r"\bplan_id=\d+\b", "plan_id=?", value)
    return re.sub(r"\s+", " ", value).strip()


def _physical_plan(frame: Any) -> str:
    return _canonical_plan(frame._jdf.queryExecution().executedPlan().toString())


def _authoritative_events(
    spark: Any, events: Sequence[PaymentEvent], cutoff: int
) -> tuple[Any, int]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("event_time", LongType(), False),
            StructField("knowledge_time", LongType(), False),
            StructField("amount_cents", LongType(), True),
            StructField("status", StringType(), True),
            StructField("merchant_risk", DoubleType(), True),
            StructField("revision_id", StringType(), False),
            StructField("operation", StringType(), False),
        ]
    )
    frame = spark.createDataFrame(_event_rows(events), schema=schema)
    input_partitions = frame.rdd.getNumPartitions()
    window = Window.partitionBy("event_id").orderBy(
        F.col("knowledge_time").desc(), F.col("revision_id").desc()
    )
    authoritative = (
        frame.where(F.col("knowledge_time") <= cutoff)
        .withColumn("revision_rank", F.row_number().over(window))
        .where((F.col("revision_rank") == 1) & (F.col("operation") == "upsert"))
        .drop("revision_rank")
    )
    return authoritative, input_partitions


def _computed_rows(
    spark: Any,
    definitions: Sequence[FeatureDefinition],
    events: Sequence[PaymentEvent],
    customer_ids: Sequence[str],
    *,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> tuple[Any, int, int]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    authoritative, input_partitions = _authoritative_events(spark, events, knowledge_cutoff)
    authoritative = authoritative.where(F.col("event_time") <= event_cutoff)
    customer_schema = StructType([StructField("customer_id", StringType(), False)])
    customers = spark.createDataFrame([(value,) for value in customer_ids], customer_schema)
    outputs: list[Any] = []
    for definition in definitions:
        lower = event_cutoff - definition.window_seconds if definition.window_seconds else None
        join_condition = customers.customer_id == authoritative.customer_id
        if lower is not None:
            join_condition = join_condition & (authoritative.event_time > lower)
        eligible = customers.join(authoritative, join_condition, "left")
        grouped = eligible.groupBy(customers.customer_id.alias("customer_id"))
        if definition.computation == "transaction_count":
            value = F.count(authoritative.event_id).cast("long")
        elif definition.computation == "successful_spend_cents":
            value = F.coalesce(
                F.sum(F.when(authoritative.status == "succeeded", authoritative.amount_cents)),
                F.lit(definition.empty_default),
            ).cast("long")
        elif definition.computation == "failed_payment_ratio":
            count = F.count(authoritative.event_id)
            failures = F.sum(F.when(authoritative.status == "failed", 1).otherwise(0))
            value = F.when(count > 0, failures.cast("double") / count).otherwise(
                F.lit(definition.empty_default).cast("double")
            )
        elif definition.computation == "hours_since_success":
            last_success = F.max(
                F.when(authoritative.status == "succeeded", authoritative.event_time)
            )
            value = F.when(
                last_success.isNotNull(), (F.lit(event_cutoff) - last_success) / 3600
            ).otherwise(F.lit(definition.empty_default).cast("double"))
        else:
            value = F.coalesce(
                F.max(authoritative.merchant_risk),
                F.lit(definition.empty_default).cast("double"),
            )
        selected = grouped.agg(value.alias("raw_value"))
        outputs.append(
            selected.select(
                "customer_id",
                F.lit(definition.name).alias("feature_name"),
                (
                    F.col("raw_value").cast("long")
                    if definition.value_type == "integer"
                    else F.lit(None).cast("long")
                ).alias("value_long"),
                (
                    F.col("raw_value").cast("double")
                    if definition.value_type == "float"
                    else F.lit(None).cast("double")
                ).alias("value_double"),
                F.lit(definition.definition_digest).alias("definition_digest"),
            )
        )
    result = outputs[0]
    for output in outputs[1:]:
        result = result.unionByName(output)
    ordered = result.repartition(4, "customer_id").sortWithinPartitions(
        "customer_id", "feature_name"
    )
    return ordered, authoritative.count(), input_partitions


def full_rebuild(
    spark: Any,
    generation_id: str,
    definitions: Sequence[FeatureDefinition],
    events: Iterable[PaymentEvent],
    customer_ids: Sequence[str],
    *,
    event_cutoff: int,
    knowledge_cutoff: int,
) -> SparkBuild:
    """Compute one immutable five-feature generation with Spark."""

    _validate_generation_id(generation_id)
    defs, normalized, customers = validate_inputs(
        definitions,
        events,
        customer_ids,
        event_cutoff=event_cutoff,
        knowledge_cutoff=knowledge_cutoff,
    )
    frame, authoritative_count, input_partitions = _computed_rows(
        spark,
        defs,
        normalized,
        customers,
        event_cutoff=event_cutoff,
        knowledge_cutoff=knowledge_cutoff,
    )
    plan = _physical_plan(frame)
    collected = frame.orderBy("customer_id", "feature_name").collect()
    definition_by_name = {definition.name: definition for definition in defs}
    values: list[FeatureValue] = []
    for row in collected:
        definition = definition_by_name[row.feature_name]
        raw = row.value_long if definition.value_type == "integer" else row.value_double
        value: int | float | None
        value = cast(int | float | None, raw)
        values.append(
            FeatureValue(
                row.customer_id,
                row.feature_name,
                value,
                event_cutoff,
                knowledge_cutoff,
                row.definition_digest,
                generation_id,
            )
        )
    canonical_rows = tuple(value.as_dict() for value in values)
    partition_counts = tuple(
        sorted(
            frame.rdd.mapPartitions(lambda rows: [sum(1 for _ in rows)]).collect(),
            reverse=True,
        )
    )
    maximum = max(partition_counts, default=0)
    diagnostics = SparkDiagnostics(
        input_rows=len(normalized),
        authoritative_rows=authoritative_count,
        output_rows=len(values),
        input_partitions=input_partitions,
        output_partitions=frame.rdd.getNumPartitions(),
        partition_row_counts=partition_counts,
        maximum_partition_fraction=(maximum / len(values) if values else 0.0),
        physical_plan_digest=digest(plan),
        cartesian_product="CartesianProduct" in plan,
    )
    manifest: dict[str, Any] = {
        "contract": ARTIFACT_CONTRACT,
        "generation_id": generation_id,
        "event_cutoff": event_cutoff,
        "knowledge_cutoff": knowledge_cutoff,
        "definition_set_digest": digest(_definition_rows(defs)),
        "source_digest": digest(_event_rows(normalized)),
        "customer_digest": digest(customers),
        "row_count": len(canonical_rows),
        "rows_digest": digest(canonical_rows),
        "diagnostics": diagnostics.as_dict(),
    }
    return SparkBuild(generation_id, tuple(values), manifest, diagnostics)


def _state_by_event(events: Sequence[PaymentEvent], cutoff: int) -> dict[str, PaymentEvent | None]:
    values: dict[str, PaymentEvent | None] = {}
    for event in events:
        if event.knowledge_time <= cutoff:
            values[event.event_id] = None if event.operation == "retract" else event
    return values


def plan_affected_scope(
    definitions: Sequence[FeatureDefinition],
    events: Iterable[PaymentEvent],
    *,
    event_cutoff: int,
    base_knowledge_cutoff: int,
    target_knowledge_cutoff: int,
    maximum_pair_fraction: float = 0.75,
) -> AffectedScopePlan:
    """Conservatively plan feature keys changed between two knowledge frontiers."""

    if base_knowledge_cutoff < 0:
        raise SparkContractError("base knowledge cutoff must be non-negative")
    if base_knowledge_cutoff > target_knowledge_cutoff:
        raise SparkContractError("target knowledge cutoff cannot precede the base cutoff")
    event_rows = tuple(events)
    event_customers = tuple(sorted({event.customer_id for event in event_rows}))
    defs, normalized, _ = validate_inputs(
        definitions,
        event_rows,
        event_customers,
        event_cutoff=event_cutoff,
        knowledge_cutoff=target_knowledge_cutoff,
    )
    base = _state_by_event(normalized, base_knowledge_cutoff)
    target = _state_by_event(normalized, target_knowledge_cutoff)
    changed = sorted(key for key in set(base) | set(target) if base.get(key) != target.get(key))
    pairs: set[tuple[str, str]] = set()
    reasons: list[str] = []
    for event_id in changed:
        prior, current = base.get(event_id), target.get(event_id)
        candidates = [value for value in (prior, current) if value is not None]
        for definition in defs:
            lower = event_cutoff - definition.window_seconds if definition.window_seconds else None
            if any(
                value.event_time <= event_cutoff and (lower is None or value.event_time > lower)
                for value in candidates
            ):
                pairs.update((value.customer_id, definition.name) for value in candidates)
        reasons.append(f"revision-change:{event_id}")
    customers = {event.customer_id for event in normalized}
    total = len(customers) * len(defs)
    fraction = len(pairs) / total if total else 0.0
    mode: Literal["incremental", "full"] = "incremental"
    if not 0 < maximum_pair_fraction <= 1:
        raise ValueError("maximum_pair_fraction must be in (0, 1]")
    if fraction > maximum_pair_fraction:
        mode = "full"
        reasons.append("affected-scope-threshold")
    source_digest = digest(_event_rows(normalized))
    payload = {
        "base_knowledge_cutoff": base_knowledge_cutoff,
        "target_knowledge_cutoff": target_knowledge_cutoff,
        "event_cutoff": event_cutoff,
        "affected_pairs": sorted(pairs),
        "reasons": sorted(reasons),
        "source_digest": source_digest,
        "mode": mode,
    }
    return AffectedScopePlan(
        base_knowledge_cutoff,
        target_knowledge_cutoff,
        mode,
        tuple(sorted(pairs)),
        tuple(sorted(reasons)),
        source_digest,
        digest(payload),
    )


def incremental_rebuild(
    spark: Any,
    generation_id: str,
    definitions: Sequence[FeatureDefinition],
    events: Iterable[PaymentEvent],
    customer_ids: Sequence[str],
    base: SparkBuild,
    *,
    event_cutoff: int,
    target_knowledge_cutoff: int,
    maximum_pair_fraction: float = 0.75,
) -> tuple[SparkBuild, AffectedScopePlan]:
    """Recompute the conservative affected scope and re-envelope preserved values."""

    normalized = tuple(events)
    if base.manifest.get("contract") != ARTIFACT_CONTRACT:
        raise GenerationArtifactError("predecessor generation contract is unsupported")
    if base.manifest.get("generation_id") != base.generation_id:
        raise GenerationArtifactError("predecessor generation identity mismatch")
    if base.manifest.get("row_count") != len(base.values):
        raise GenerationArtifactError("predecessor row count mismatch")
    if base.manifest.get("rows_digest") != digest(tuple(value.as_dict() for value in base.values)):
        raise GenerationArtifactError("predecessor row digest mismatch")
    if base.manifest.get("event_cutoff") != event_cutoff:
        raise GenerationArtifactError("predecessor event cutoff differs")
    base_cutoff = base.manifest.get("knowledge_cutoff")
    if not isinstance(base_cutoff, int):
        raise GenerationArtifactError("predecessor knowledge cutoff is invalid")
    if any(
        value.generation_id != base.generation_id
        or value.event_time != event_cutoff
        or value.knowledge_time != base_cutoff
        for value in base.values
    ):
        raise GenerationArtifactError("predecessor rows are not bound to its declared frontier")
    plan = plan_affected_scope(
        definitions,
        normalized,
        event_cutoff=event_cutoff,
        base_knowledge_cutoff=base_cutoff,
        target_knowledge_cutoff=target_knowledge_cutoff,
        maximum_pair_fraction=maximum_pair_fraction,
    )
    if plan.mode == "full":
        target = full_rebuild(
            spark,
            generation_id,
            definitions,
            normalized,
            customer_ids,
            event_cutoff=event_cutoff,
            knowledge_cutoff=target_knowledge_cutoff,
        )
        return target, plan
    affected = set(plan.affected_pairs)
    base_by_key = {(value.customer_id, value.feature_name): value for value in base.values}
    defs, validated_events, customers = validate_inputs(
        definitions,
        normalized,
        customer_ids,
        event_cutoff=event_cutoff,
        knowledge_cutoff=target_knowledge_cutoff,
    )
    definition_set_digest = digest(_definition_rows(defs))
    source_digest = digest(_event_rows(validated_events))
    customer_digest = digest(customers)
    for field, expected in (
        ("definition_set_digest", definition_set_digest),
        ("customer_digest", customer_digest),
    ):
        if base.manifest.get(field) != expected:
            raise GenerationArtifactError(f"predecessor {field} is incompatible")
    expected_keys = {(customer, definition.name) for customer in customers for definition in defs}
    if set(base_by_key) != expected_keys:
        raise GenerationArtifactError("predecessor keyset is incomplete or incompatible")
    affected_customers = tuple(sorted({customer for customer, _ in affected}))
    if affected_customers:
        recomputed = full_rebuild(
            spark,
            generation_id,
            defs,
            validated_events,
            affected_customers,
            event_cutoff=event_cutoff,
            knowledge_cutoff=target_knowledge_cutoff,
        )
        recomputed_by_key = {
            (value.customer_id, value.feature_name): value for value in recomputed.values
        }
        diagnostics = recomputed.diagnostics
    else:
        recomputed_by_key = {}
        diagnostics = SparkDiagnostics(
            input_rows=len(validated_events),
            authoritative_rows=0,
            output_rows=0,
            input_partitions=0,
            output_partitions=0,
            partition_row_counts=(),
            maximum_partition_fraction=0.0,
            physical_plan_digest=digest("no-op-incremental"),
            cartesian_product=False,
        )
    combined: list[FeatureValue] = []
    for key in sorted(expected_keys):
        source = recomputed_by_key[key] if key in affected else base_by_key[key]
        combined.append(
            FeatureValue(
                source.customer_id,
                source.feature_name,
                source.value,
                event_cutoff,
                target_knowledge_cutoff,
                source.definition_digest,
                generation_id,
            )
        )
    rows = tuple(value.as_dict() for value in combined)
    manifest: dict[str, Any] = {
        "contract": ARTIFACT_CONTRACT,
        "generation_id": generation_id,
        "event_cutoff": event_cutoff,
        "knowledge_cutoff": target_knowledge_cutoff,
        "definition_set_digest": definition_set_digest,
        "source_digest": source_digest,
        "customer_digest": customer_digest,
        "row_count": len(rows),
        "rows_digest": digest(rows),
        "diagnostics": diagnostics.as_dict(),
        "incremental": {
            "base_generation_id": base.generation_id,
            "plan_digest": plan.plan_digest,
            "affected_pair_count": len(affected),
            "recomputed_pair_count": len(recomputed_by_key),
            "preserved_pair_count": len(rows) - len(affected),
            "work_avoided_pair_count": len(rows) - len(recomputed_by_key),
            "mode": plan.mode,
        },
    }
    return SparkBuild(generation_id, tuple(combined), manifest, diagnostics), plan


def write_generation(build: SparkBuild, root: Path) -> Literal["written", "replayed"]:
    """Atomically persist an immutable local generation using a completion marker."""

    _validate_generation_id(build.generation_id)
    root.mkdir(parents=True, exist_ok=True)
    final = root / build.generation_id
    manifest = dict(build.manifest)
    manifest["artifact_digest"] = digest(
        {"manifest": build.manifest, "rows": tuple(value.as_dict() for value in build.values)}
    )
    if final.exists():
        loaded = read_generation(final)
        if loaded.manifest.get("artifact_digest") != manifest["artifact_digest"]:
            raise GenerationArtifactError("generation identity conflicts with immutable content")
        return "replayed"
    staging = root / f".{build.generation_id}.staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir()
    (staging / "rows.json").write_text(
        canonical_json(tuple(value.as_dict() for value in build.values)) + "\n", encoding="utf-8"
    )
    (staging / "manifest.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    (staging / "COMPLETE").write_text(manifest["artifact_digest"] + "\n", encoding="utf-8")
    staging.rename(final)
    return "written"


def read_generation(path: Path) -> SparkBuild:
    """Fail closed when reopening an incomplete or tampered generation."""

    required = {"manifest.json", "rows.json", "COMPLETE"}
    if not path.is_dir() or {item.name for item in path.iterdir()} != required:
        raise GenerationArtifactError("generation is incomplete or contains unbound files")
    manifest: dict[str, Any] = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = json.loads((path / "rows.json").read_text(encoding="utf-8"))
    artifact_digest = manifest.pop("artifact_digest", None)
    if (path / "COMPLETE").read_text(encoding="utf-8").strip() != artifact_digest:
        raise GenerationArtifactError("completion marker does not bind the manifest")
    if manifest.get("rows_digest") != digest(tuple(rows)) or manifest.get("row_count") != len(rows):
        raise GenerationArtifactError("persisted rows do not match their manifest")
    if artifact_digest != digest({"manifest": manifest, "rows": tuple(rows)}):
        raise GenerationArtifactError("artifact digest does not bind rows and manifest")
    values = tuple(FeatureValue(**row) for row in rows)
    diagnostic_data = dict(manifest["diagnostics"])
    diagnostic_data["partition_row_counts"] = tuple(diagnostic_data["partition_row_counts"])
    diagnostics = SparkDiagnostics(**diagnostic_data)
    restored = dict(manifest)
    restored["artifact_digest"] = artifact_digest
    return SparkBuild(path.name, values, restored, diagnostics)
