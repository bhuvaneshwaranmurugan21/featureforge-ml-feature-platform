"""SQLite reference store for immutable generations and atomic online publication."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from featureforge.canonical import canonical_json, digest
from featureforge.model import FeatureValue, PaymentEvent


class FeatureStoreError(RuntimeError):
    pass


class ReplayConflict(FeatureStoreError):
    pass


class GenerationConflict(FeatureStoreError):
    pass


class CompareAndSwapConflict(FeatureStoreError):
    pass


class FeatureStore:
    def __init__(self, database: str | Path = ":memory:") -> None:
        self.connection = sqlite3.connect(str(database), isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self._create_schema()

    def close(self) -> None:
        self.connection.close()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                revision_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                knowledge_time INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                UNIQUE(event_id, knowledge_time)
            );
            CREATE TABLE IF NOT EXISTS generations (
                generation_id TEXT PRIMARY KEY,
                definition_set_digest TEXT NOT NULL,
                source_frontier INTEGER NOT NULL,
                dataset_as_of INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('building', 'ready', 'active', 'retired'))
            );
            CREATE TABLE IF NOT EXISTS offline_values (
                generation_id TEXT NOT NULL REFERENCES generations(generation_id),
                customer_id TEXT NOT NULL,
                feature_name TEXT NOT NULL,
                event_time INTEGER NOT NULL,
                knowledge_time INTEGER NOT NULL,
                value_json TEXT,
                definition_digest TEXT NOT NULL,
                value_digest TEXT NOT NULL,
                PRIMARY KEY(generation_id, customer_id, feature_name, event_time, knowledge_time)
            );
            CREATE TABLE IF NOT EXISTS online_values (
                generation_id TEXT NOT NULL REFERENCES generations(generation_id),
                customer_id TEXT NOT NULL,
                feature_name TEXT NOT NULL,
                event_time INTEGER NOT NULL,
                knowledge_time INTEGER NOT NULL,
                value_json TEXT,
                definition_digest TEXT NOT NULL,
                value_digest TEXT NOT NULL,
                PRIMARY KEY(generation_id, customer_id, feature_name)
            );
            CREATE TABLE IF NOT EXISTS active_pointer (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                generation_id TEXT REFERENCES generations(generation_id),
                pointer_version INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO active_pointer(singleton, generation_id, pointer_version)
            VALUES(1, NULL, 0);
            """
        )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def append_event(self, event: PaymentEvent) -> str:
        payload = event.as_dict()
        payload_digest = digest(payload)
        prior = self.connection.execute(
            "SELECT payload_digest FROM events WHERE revision_id = ?",
            (event.revision_id,),
        ).fetchone()
        if prior:
            if prior["payload_digest"] != payload_digest:
                raise ReplayConflict("an event revision was replayed with different content")
            return "replayed"
        same_clock = self.connection.execute(
            "SELECT revision_id FROM events WHERE event_id = ? AND knowledge_time = ?",
            (event.event_id, event.knowledge_time),
        ).fetchone()
        if same_clock:
            raise ReplayConflict("an event has two revisions at the same knowledge time")
        prior_event = self.connection.execute(
            "SELECT payload_json FROM events WHERE event_id = ? LIMIT 1", (event.event_id,)
        ).fetchone()
        if prior_event:
            prior_payload: dict[str, Any] = json.loads(prior_event["payload_json"])
            if prior_payload["customer_id"] != event.customer_id:
                raise ReplayConflict("a logical event cannot change customer identity")
        self.connection.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?)",
            (
                event.revision_id,
                event.event_id,
                event.knowledge_time,
                canonical_json(payload),
                payload_digest,
            ),
        )
        return "appended"

    def events(self) -> tuple[PaymentEvent, ...]:
        result: list[PaymentEvent] = []
        for row in self.connection.execute(
            "SELECT payload_json FROM events ORDER BY knowledge_time, event_id, revision_id"
        ):
            payload: dict[str, Any] = json.loads(row["payload_json"])
            result.append(PaymentEvent(**payload))
        return tuple(result)

    def create_generation(
        self,
        generation_id: str,
        definition_set_digest: str,
        *,
        source_frontier: int,
        dataset_as_of: int,
    ) -> None:
        self.connection.execute(
            "INSERT INTO generations VALUES (?, ?, ?, ?, 'building')",
            (generation_id, definition_set_digest, source_frontier, dataset_as_of),
        )

    def put_offline(self, values: Iterable[FeatureValue]) -> str:
        outcome = "replayed"
        with self._transaction():
            for feature in values:
                value_digest = digest(feature.as_dict())
                existing = self.connection.execute(
                    """SELECT value_digest FROM offline_values
                       WHERE generation_id=? AND customer_id=? AND feature_name=?
                         AND event_time=? AND knowledge_time=?""",
                    (
                        feature.generation_id,
                        feature.customer_id,
                        feature.feature_name,
                        feature.event_time,
                        feature.knowledge_time,
                    ),
                ).fetchone()
                if existing:
                    if existing["value_digest"] != value_digest:
                        raise ReplayConflict("materialized feature changed during replay")
                    continue
                outcome = "written"
                self.connection.execute(
                    """INSERT INTO offline_values VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        feature.generation_id,
                        feature.customer_id,
                        feature.feature_name,
                        feature.event_time,
                        feature.knowledge_time,
                        None if feature.value is None else canonical_json(feature.value),
                        feature.definition_digest,
                        value_digest,
                    ),
                )
        return outcome

    def offline_values(self, generation_id: str) -> tuple[FeatureValue, ...]:
        rows = self.connection.execute(
            """SELECT * FROM offline_values WHERE generation_id = ?
               ORDER BY customer_id, feature_name, event_time, knowledge_time""",
            (generation_id,),
        )
        return tuple(self._to_feature_value(row) for row in rows)

    def stage_online(self, generation_id: str) -> None:
        status = self._generation_status(generation_id)
        if status != "building":
            raise GenerationConflict("only a building generation can be staged")
        with self._transaction():
            self.connection.execute(
                "DELETE FROM online_values WHERE generation_id = ?", (generation_id,)
            )
            self.connection.execute(
                """INSERT INTO online_values
                   SELECT o.generation_id, o.customer_id, o.feature_name, o.event_time,
                          o.knowledge_time, o.value_json, o.definition_digest, o.value_digest
                   FROM offline_values o
                   WHERE o.generation_id = ? AND NOT EXISTS (
                       SELECT 1 FROM offline_values newer
                       WHERE newer.generation_id = o.generation_id
                         AND newer.customer_id = o.customer_id
                         AND newer.feature_name = o.feature_name
                         AND (newer.event_time > o.event_time OR
                              (newer.event_time = o.event_time AND
                               newer.knowledge_time > o.knowledge_time))
                   )""",
                (generation_id,),
            )

    def online_values(self, generation_id: str) -> tuple[FeatureValue, ...]:
        rows = self.connection.execute(
            """SELECT * FROM online_values WHERE generation_id = ?
               ORDER BY customer_id, feature_name""",
            (generation_id,),
        )
        return tuple(self._to_feature_value(row) for row in rows)

    def mark_ready(self, generation_id: str, *, parity_matched: bool) -> None:
        if not parity_matched:
            raise GenerationConflict("offline/online parity failed")
        if self._generation_status(generation_id) != "building":
            raise GenerationConflict("only a building generation can become ready")
        self.connection.execute(
            "UPDATE generations SET status = 'ready' WHERE generation_id = ?", (generation_id,)
        )

    def activate(self, generation_id: str, expected_version: int) -> int:
        if self._generation_status(generation_id) not in {"ready", "retired"}:
            raise GenerationConflict("only a parity-proven generation can be published")
        with self._transaction():
            previous = self.connection.execute(
                "SELECT generation_id FROM active_pointer WHERE singleton=1"
            ).fetchone()["generation_id"]
            changed = self.connection.execute(
                """UPDATE active_pointer
                   SET generation_id=?, pointer_version=pointer_version+1
                   WHERE singleton=1 AND pointer_version=?""",
                (generation_id, expected_version),
            )
            if changed.rowcount != 1:
                raise CompareAndSwapConflict("online pointer changed concurrently")
            if previous and previous != generation_id:
                self.connection.execute(
                    "UPDATE generations SET status='retired' WHERE generation_id=?", (previous,)
                )
            self.connection.execute(
                "UPDATE generations SET status='active' WHERE generation_id=?", (generation_id,)
            )
        return expected_version + 1

    def active_pointer(self) -> tuple[str | None, int]:
        row = self.connection.execute(
            "SELECT generation_id, pointer_version FROM active_pointer WHERE singleton=1"
        ).fetchone()
        return row["generation_id"], row["pointer_version"]

    def serve(
        self,
        customer_id: str,
        feature_name: str,
        *,
        request_time: int,
        ttl_seconds: int,
    ) -> Any:
        generation_id, _ = self.active_pointer()
        if generation_id is None:
            return None
        row = self.connection.execute(
            """SELECT value_json, event_time FROM online_values
               WHERE generation_id=? AND customer_id=? AND feature_name=?""",
            (generation_id, customer_id, feature_name),
        ).fetchone()
        if row is None or row["event_time"] <= request_time - ttl_seconds:
            return None
        return None if row["value_json"] is None else json.loads(row["value_json"])

    def _generation_status(self, generation_id: str) -> str:
        row = self.connection.execute(
            "SELECT status FROM generations WHERE generation_id=?", (generation_id,)
        ).fetchone()
        if row is None:
            raise KeyError(generation_id)
        return str(row["status"])

    @staticmethod
    def _to_feature_value(row: sqlite3.Row) -> FeatureValue:
        value = None if row["value_json"] is None else json.loads(row["value_json"])
        return FeatureValue(
            customer_id=row["customer_id"],
            feature_name=row["feature_name"],
            value=value,
            event_time=row["event_time"],
            knowledge_time=row["knowledge_time"],
            definition_digest=row["definition_digest"],
            generation_id=row["generation_id"],
        )
