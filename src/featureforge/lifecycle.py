"""Persistent local lifecycle with immutable candidates and guarded publication."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from featureforge.canonical import canonical_json, digest
from featureforge.model import FeatureDefinition, FeatureValue
from featureforge.provenance import Artifact

SCHEMA_VERSION = 2
GENERATION_MANIFEST_FIELDS = {
    "candidate_count",
    "candidate_digest",
    "code_tree",
    "contract",
    "dataset_manifest_digest",
    "definition_set_digest",
    "event_cutoff",
    "expected_predecessor",
    "generation_id",
    "knowledge_cutoff",
    "label_snapshot_digest",
    "source_snapshot_digest",
}


class LifecycleError(RuntimeError):
    pass


class ArtifactConflict(LifecycleError):
    pass


class DefinitionConflict(LifecycleError):
    pass


class StateConflict(LifecycleError):
    pass


class CompareAndSwapConflict(LifecycleError):
    pass


class IdempotencyConflict(LifecycleError):
    pass


class PinnedGenerationUnavailable(LifecycleError):
    pass


class AcknowledgementLost(LifecycleError):
    """A deterministic fault raised only after a committed publication."""


@dataclass(frozen=True)
class ReaderToken:
    generation_id: str
    pointer_version: int


OperationKind = Literal["activate", "rollback"]


class LifecycleStore:
    """File-backed SQLite authority for the bounded local lifecycle."""

    def __init__(self, database: str | Path) -> None:
        self.connection = sqlite3.connect(str(database), isolation_level=None, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.execute("PRAGMA journal_mode = WAL")
        version = int(self.connection.execute("PRAGMA user_version").fetchone()[0])
        if version not in {0, SCHEMA_VERSION}:
            self.connection.close()
            raise LifecycleError(
                f"unsupported lifecycle schema version {version}; expected {SCHEMA_VERSION}"
            )
        if version == 0:
            self._create_schema()

    def close(self) -> None:
        self.connection.close()

    def _create_schema(self) -> None:
        self.connection.executescript(
            f"""
            CREATE TABLE definitions (
                definition_id TEXT PRIMARY KEY,
                definition_digest TEXT NOT NULL UNIQUE,
                definition_json TEXT NOT NULL
            );
            CREATE TABLE artifacts (
                kind TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                artifact_digest TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                PRIMARY KEY(kind, artifact_id)
            );
            CREATE TABLE artifact_rows (
                kind TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                row_number INTEGER NOT NULL,
                row_json TEXT NOT NULL,
                row_digest TEXT NOT NULL,
                PRIMARY KEY(kind, artifact_id, row_number),
                FOREIGN KEY(kind, artifact_id) REFERENCES artifacts(kind, artifact_id)
            );
            CREATE TABLE generations (
                generation_id TEXT PRIMARY KEY,
                manifest_json TEXT NOT NULL,
                manifest_digest TEXT NOT NULL,
                expected_predecessor TEXT,
                status TEXT NOT NULL CHECK(status IN
                    ('CREATED', 'BUILDING', 'VALIDATING', 'READY', 'ACTIVE',
                     'RETIRED', 'FAILED', 'SUPERSEDED')),
                validation_digest TEXT
            );
            CREATE TABLE candidate_values (
                generation_id TEXT NOT NULL REFERENCES generations(generation_id),
                customer_id TEXT NOT NULL,
                feature_name TEXT NOT NULL,
                row_json TEXT NOT NULL,
                row_digest TEXT NOT NULL,
                ttl_seconds INTEGER NOT NULL,
                PRIMARY KEY(generation_id, customer_id, feature_name)
            );
            CREATE TABLE validation_receipts (
                generation_id TEXT PRIMARY KEY REFERENCES generations(generation_id),
                expected_artifact_id TEXT NOT NULL,
                receipt_json TEXT NOT NULL,
                receipt_digest TEXT NOT NULL
            );
            CREATE TABLE active_pointer (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                generation_id TEXT REFERENCES generations(generation_id),
                pointer_version INTEGER NOT NULL
            );
            INSERT INTO active_pointer VALUES(1, NULL, 0);
            CREATE TABLE operations (
                operation_id TEXT PRIMARY KEY,
                request_digest TEXT NOT NULL,
                result_json TEXT NOT NULL
            );
            CREATE TABLE pointer_history (
                pointer_version INTEGER PRIMARY KEY,
                from_generation TEXT,
                to_generation TEXT NOT NULL,
                operation_kind TEXT NOT NULL CHECK(operation_kind IN ('activate', 'rollback')),
                operation_id TEXT NOT NULL UNIQUE
            );
            PRAGMA user_version = {SCHEMA_VERSION};
            """
        )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def register_definition(self, definition: FeatureDefinition) -> str:
        rendered = canonical_json(asdict(definition))
        existing = self.connection.execute(
            "SELECT definition_digest, definition_json FROM definitions WHERE definition_id=?",
            (definition.definition_id,),
        ).fetchone()
        if existing is not None:
            if (
                existing["definition_digest"] != definition.definition_digest
                or existing["definition_json"] != rendered
            ):
                raise DefinitionConflict(
                    f"{definition.definition_id} already has different immutable content"
                )
            return "replayed"
        digest_owner = self.connection.execute(
            "SELECT definition_id FROM definitions WHERE definition_digest=?",
            (definition.definition_digest,),
        ).fetchone()
        if digest_owner is not None:
            raise DefinitionConflict("definition digest is already owned by another identity")
        self.connection.execute(
            "INSERT INTO definitions VALUES (?, ?, ?)",
            (definition.definition_id, definition.definition_digest, rendered),
        )
        return "written"

    def put_artifact(self, artifact: Artifact) -> str:
        existing = self.connection.execute(
            "SELECT artifact_digest FROM artifacts WHERE kind=? AND artifact_id=?",
            (artifact.kind, artifact.artifact_id),
        ).fetchone()
        if existing is not None:
            if existing["artifact_digest"] != artifact.artifact_digest:
                raise ArtifactConflict("artifact identity was reused with different content")
            return "replayed"
        with self._transaction():
            self.connection.execute(
                "INSERT INTO artifacts VALUES (?, ?, ?, ?)",
                (
                    artifact.kind,
                    artifact.artifact_id,
                    artifact.artifact_digest,
                    canonical_json(artifact.manifest),
                ),
            )
            for number, row in enumerate(artifact.rows):
                self.connection.execute(
                    "INSERT INTO artifact_rows VALUES (?, ?, ?, ?, ?)",
                    (
                        artifact.kind,
                        artifact.artifact_id,
                        number,
                        canonical_json(row),
                        digest(row),
                    ),
                )
        return "written"

    def artifact(self, kind: str, artifact_id: str) -> Artifact:
        stored = self.connection.execute(
            """SELECT artifact_digest, manifest_json FROM artifacts
               WHERE kind=? AND artifact_id=?""",
            (kind, artifact_id),
        ).fetchone()
        if stored is None:
            raise KeyError((kind, artifact_id))
        row_values = self.connection.execute(
            """SELECT row_json FROM artifact_rows WHERE kind=? AND artifact_id=?
               ORDER BY row_number""",
            (kind, artifact_id),
        )
        rows = tuple(json.loads(row["row_json"]) for row in row_values)
        manifest: dict[str, Any] = json.loads(stored["manifest_json"])
        if manifest.get("row_count") != len(rows) or manifest.get("rows_digest") != digest(rows):
            raise ArtifactConflict("persisted artifact rows do not match its manifest")
        calculated = digest(
            {
                "contract": "featureforge-artifact-v1",
                "manifest": manifest,
                "rows": rows,
            }
        )
        if calculated != stored["artifact_digest"]:
            raise ArtifactConflict("persisted artifact digest does not match its content")
        return Artifact(artifact_id, kind, manifest, rows, stored["artifact_digest"])

    def create_generation(self, manifest: dict[str, Any]) -> None:
        if set(manifest) != GENERATION_MANIFEST_FIELDS:
            missing = sorted(GENERATION_MANIFEST_FIELDS - set(manifest))
            unknown = sorted(set(manifest) - GENERATION_MANIFEST_FIELDS)
            raise ValueError(f"generation manifest shape: missing={missing}, unknown={unknown}")
        generation_id = manifest.get("generation_id")
        if not isinstance(generation_id, str) or not generation_id:
            raise ValueError("generation manifest requires a non-empty generation_id")
        if manifest.get("contract") != "generation-manifest-v1":
            raise ValueError("unsupported generation manifest contract")
        predecessor = manifest.get("expected_predecessor")
        if predecessor is not None and not isinstance(predecessor, str):
            raise ValueError("expected predecessor must be a string or null")
        for field in (
            "candidate_digest",
            "code_tree",
            "dataset_manifest_digest",
            "definition_set_digest",
            "label_snapshot_digest",
            "source_snapshot_digest",
        ):
            if not isinstance(manifest[field], str) or not manifest[field]:
                raise ValueError(f"generation manifest requires non-empty {field}")
        for field in ("candidate_count", "event_cutoff", "knowledge_cutoff"):
            value = manifest[field]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"generation manifest requires non-negative integer {field}")
        manifest_digest = digest(manifest)
        self.connection.execute(
            "INSERT INTO generations VALUES (?, ?, ?, ?, 'CREATED', NULL)",
            (generation_id, canonical_json(manifest), manifest_digest, predecessor),
        )

    def begin_build(self, generation_id: str) -> None:
        with self._transaction():
            status = self.generation_status(generation_id)
            if status == "BUILDING":
                return
            if status != "CREATED":
                raise StateConflict("only a created generation can begin building")
            self.connection.execute(
                "UPDATE generations SET status='BUILDING' WHERE generation_id=?",
                (generation_id,),
            )

    def put_candidate_batch(self, generation_id: str, values: Iterable[FeatureValue]) -> str:
        outcome = "replayed"
        with self._transaction():
            if self.generation_status(generation_id) != "BUILDING":
                raise StateConflict("candidate writes require a building generation")
            for value in values:
                if value.generation_id != generation_id:
                    raise ArtifactConflict("feature value belongs to another generation")
                definition = self.connection.execute(
                    "SELECT definition_json FROM definitions WHERE definition_digest=?",
                    (value.definition_digest,),
                ).fetchone()
                if definition is None:
                    raise ArtifactConflict("candidate references an unknown definition")
                definition_value: dict[str, Any] = json.loads(definition["definition_json"])
                row = value.as_dict()
                row_digest = digest(row)
                existing = self.connection.execute(
                    """SELECT row_digest FROM candidate_values
                       WHERE generation_id=? AND customer_id=? AND feature_name=?""",
                    (generation_id, value.customer_id, value.feature_name),
                ).fetchone()
                if existing is not None:
                    if existing["row_digest"] != row_digest:
                        raise ArtifactConflict("candidate key was replayed with different content")
                    continue
                self.connection.execute(
                    "INSERT INTO candidate_values VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        generation_id,
                        value.customer_id,
                        value.feature_name,
                        canonical_json(row),
                        row_digest,
                        int(definition_value["ttl_seconds"]),
                    ),
                )
                outcome = "written"
        return outcome

    def seal_candidate(self, generation_id: str) -> None:
        with self._transaction():
            if self.generation_status(generation_id) != "BUILDING":
                raise StateConflict("only a building generation can enter validation")
            self.connection.execute(
                "UPDATE generations SET status='VALIDATING' WHERE generation_id=?",
                (generation_id,),
            )

    def candidate_rows(self, generation_id: str) -> tuple[dict[str, Any], ...]:
        values = self.connection.execute(
            """SELECT row_json FROM candidate_values WHERE generation_id=?
               ORDER BY customer_id, feature_name""",
            (generation_id,),
        )
        return tuple(json.loads(row["row_json"]) for row in values)

    def validate_candidate(
        self, generation_id: str, *, expected_artifact_id: str
    ) -> dict[str, Any]:
        with self._transaction():
            if self.generation_status(generation_id) != "VALIDATING":
                raise StateConflict("generation is not awaiting validation")
            generation = self.connection.execute(
                "SELECT manifest_digest, manifest_json FROM generations WHERE generation_id=?",
                (generation_id,),
            ).fetchone()
            manifest: dict[str, Any] = json.loads(generation["manifest_json"])
            expected = self.artifact("expected-current", expected_artifact_id)
            actual_rows = self.candidate_rows(generation_id)
            artifact_links = {
                "dataset_manifest_digest": "training-dataset",
                "definition_set_digest": "definition-set",
                "label_snapshot_digest": "labels",
                "source_snapshot_digest": "source",
            }
            links_valid = all(
                self.connection.execute(
                    "SELECT 1 FROM artifacts WHERE kind=? AND artifact_digest=?",
                    (kind, manifest[field]),
                ).fetchone()
                is not None
                for field, kind in artifact_links.items()
            )
            candidate_matches_manifest = (
                len(actual_rows) == manifest["candidate_count"]
                and digest(actual_rows) == manifest["candidate_digest"]
            )
            receipt: dict[str, Any] = {
                "actual_count": len(actual_rows),
                "actual_digest": digest(actual_rows),
                "artifact_links_valid": links_valid,
                "candidate_matches_manifest": candidate_matches_manifest,
                "contract": "validation-receipt-v1",
                "expected_artifact_digest": expected.artifact_digest,
                "expected_artifact_id": expected_artifact_id,
                "expected_count": len(expected.rows),
                "expected_digest": digest(expected.rows),
                "generation_id": generation_id,
                "generation_manifest_digest": generation["manifest_digest"],
                "matched": (
                    actual_rows == expected.rows and candidate_matches_manifest and links_valid
                ),
            }
            receipt_digest = digest(receipt)
            self.connection.execute(
                "INSERT INTO validation_receipts VALUES (?, ?, ?, ?)",
                (
                    generation_id,
                    expected_artifact_id,
                    canonical_json(receipt),
                    receipt_digest,
                ),
            )
            next_status = "READY" if receipt["matched"] else "FAILED"
            self.connection.execute(
                "UPDATE generations SET status=?, validation_digest=? WHERE generation_id=?",
                (next_status, receipt_digest, generation_id),
            )
        return receipt

    def pointer(self) -> tuple[str | None, int]:
        row = self.connection.execute(
            "SELECT generation_id, pointer_version FROM active_pointer WHERE singleton=1"
        ).fetchone()
        return row["generation_id"], int(row["pointer_version"])

    def publish(
        self,
        generation_id: str,
        *,
        expected_generation: str | None,
        expected_version: int,
        operation_id: str,
        kind: OperationKind = "activate",
        lose_acknowledgement: bool = False,
    ) -> dict[str, Any]:
        request = {
            "expected_generation": expected_generation,
            "expected_version": expected_version,
            "generation_id": generation_id,
            "kind": kind,
        }
        request_digest = digest(request)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                "SELECT request_digest, result_json FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if existing is not None:
                if existing["request_digest"] != request_digest:
                    raise IdempotencyConflict(
                        "operation identity was reused with different content"
                    )
                result: dict[str, Any] = json.loads(existing["result_json"])
                self.connection.execute("COMMIT")
                return result

            current_generation, current_version = self.pointer()
            if (current_generation, current_version) != (
                expected_generation,
                expected_version,
            ):
                raise CompareAndSwapConflict(
                    "active pointer no longer matches the expected predecessor"
                )
            generation = self.connection.execute(
                """SELECT expected_predecessor, status, validation_digest, manifest_json
                   FROM generations WHERE generation_id=?""",
                (generation_id,),
            ).fetchone()
            if generation is None:
                raise CompareAndSwapConflict("target generation does not exist")
            allowed = {"READY"} if kind == "activate" else {"RETIRED"}
            if generation["status"] not in allowed:
                raise CompareAndSwapConflict("target generation is not eligible")
            if generation["validation_digest"] is None:
                raise CompareAndSwapConflict("target lacks persisted validation")
            validation = self.connection.execute(
                """SELECT receipt_json, receipt_digest FROM validation_receipts
                   WHERE generation_id=?""",
                (generation_id,),
            ).fetchone()
            if (
                validation is None
                or validation["receipt_digest"] != generation["validation_digest"]
            ):
                raise CompareAndSwapConflict("target validation receipt is missing or inconsistent")
            receipt: dict[str, Any] = json.loads(validation["receipt_json"])
            current_rows = self.candidate_rows(generation_id)
            manifest: dict[str, Any] = json.loads(generation["manifest_json"])
            if (
                not receipt.get("matched")
                or receipt.get("actual_count") != len(current_rows)
                or receipt.get("actual_digest") != digest(current_rows)
                or manifest.get("candidate_count") != len(current_rows)
                or manifest.get("candidate_digest") != digest(current_rows)
            ):
                raise CompareAndSwapConflict("candidate changed after validation")
            if kind == "activate" and generation["expected_predecessor"] != expected_generation:
                raise CompareAndSwapConflict("generation was built for another predecessor")

            changed = self.connection.execute(
                """UPDATE active_pointer
                   SET generation_id=?, pointer_version=pointer_version+1
                   WHERE singleton=1 AND generation_id IS ? AND pointer_version=?""",
                (generation_id, expected_generation, expected_version),
            )
            if changed.rowcount != 1:
                raise CompareAndSwapConflict("compare-and-swap predicate failed")
            if current_generation is not None and current_generation != generation_id:
                self.connection.execute(
                    "UPDATE generations SET status='RETIRED' WHERE generation_id=?",
                    (current_generation,),
                )
            self.connection.execute(
                "UPDATE generations SET status='ACTIVE' WHERE generation_id=?",
                (generation_id,),
            )
            result = {
                "contract": "publication-receipt-v1",
                "from_generation": current_generation,
                "generation_id": generation_id,
                "kind": kind,
                "pointer_version": expected_version + 1,
            }
            result["receipt_digest"] = digest(result)
            self.connection.execute(
                "INSERT INTO pointer_history VALUES (?, ?, ?, ?, ?)",
                (
                    expected_version + 1,
                    current_generation,
                    generation_id,
                    kind,
                    operation_id,
                ),
            )
            self.connection.execute(
                "INSERT INTO operations VALUES (?, ?, ?)",
                (operation_id, request_digest, canonical_json(result)),
            )
            self.connection.execute("COMMIT")
        except Exception:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise
        if lose_acknowledgement:
            raise AcknowledgementLost(
                "publication committed but acknowledgement was deliberately lost"
            )
        return result

    def pin_reader(self) -> ReaderToken:
        generation_id, version = self.pointer()
        if generation_id is None:
            raise PinnedGenerationUnavailable("no active generation")
        return ReaderToken(generation_id, version)

    def read_pinned(
        self,
        token: ReaderToken,
        customer_id: str,
        feature_name: str,
        *,
        request_time: int,
    ) -> int | float | None:
        generation = self.connection.execute(
            "SELECT status FROM generations WHERE generation_id=?",
            (token.generation_id,),
        ).fetchone()
        if generation is None or generation["status"] not in {"ACTIVE", "RETIRED"}:
            raise PinnedGenerationUnavailable(token.generation_id)
        row = self.connection.execute(
            """SELECT row_json, ttl_seconds FROM candidate_values
               WHERE generation_id=? AND customer_id=? AND feature_name=?""",
            (token.generation_id, customer_id, feature_name),
        ).fetchone()
        if row is None:
            raise KeyError((token.generation_id, customer_id, feature_name))
        value: dict[str, Any] = json.loads(row["row_json"])
        if request_time >= int(value["event_time"]) + int(row["ttl_seconds"]):
            return None
        raw = value["value"]
        if raw is None:
            return None
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise TypeError("stored feature value is not numeric or null")
        return raw

    def generation_status(self, generation_id: str) -> str:
        row = self.connection.execute(
            "SELECT status FROM generations WHERE generation_id=?", (generation_id,)
        ).fetchone()
        if row is None:
            raise KeyError(generation_id)
        return str(row["status"])

    def history(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM pointer_history ORDER BY pointer_version"
            )
        )
