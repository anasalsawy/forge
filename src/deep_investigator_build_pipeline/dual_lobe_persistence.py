"""Append-only SQLite (WAL) persistence for the dual-lobe runtime.

Provides the durable, append-only event/evidence/claim/run store and the derived
current-state layer described in the dual-lobe runtime spec:

- ``events``: append-only execution events, each attributable to run/floor/attempt.
- ``claims``: append-only claim lifecycle. A/B never write to the status fields
  that determine ``verified``/``contradicted``; only the verifier (Phase 5) may
  transition those.
- ``evidence``: append-only artifact/validation evidence attached to claims.
- ``runs``: per-run correlation and lifecycle metadata.
- ``state_versions``: append-only derived current-state snapshots (ShadowState,
  IntegrityState, OversightState) so effective state is always derived, never
  mutated in place.

Minimal tier uses SQLite in WAL mode on a single host. The project venv ships
SQLite >= 3.51.3, so the WAL-reset corruption hazard described in the spec does
not apply here.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

LOG = logging.getLogger("dual_lobe.persist")

STATE_DIR = Path(os.getenv("DUAL_LOBE_STATE_DIR", ".dual_lobe"))
DB_PATH = Path(os.getenv("DUAL_LOBE_DB_PATH", str(STATE_DIR / "dual_lobe.db")))
WAL = os.getenv("DUAL_LOBE_DB_WAL", "1") != "0"
SYNC = os.getenv("DUAL_LOBE_DB_SYNC", "1") == "1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    original_goal TEXT NOT NULL DEFAULT '',
    created_at    REAL NOT NULL,
    current_floor TEXT NOT NULL DEFAULT '',
    current_attempt INTEGER NOT NULL DEFAULT 1,
    status        TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL DEFAULT '',
    floor_id    TEXT NOT NULL DEFAULT '',
    attempt_id  INTEGER NOT NULL DEFAULT 0,
    worker_id   TEXT NOT NULL DEFAULT '',
    agent_role  TEXT NOT NULL DEFAULT '',
    ts          REAL NOT NULL,
    kind        TEXT NOT NULL,
    payload     TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS evidence (
    evidence_id       TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL DEFAULT '',
    event_id          TEXT NOT NULL DEFAULT '',
    kind              TEXT NOT NULL DEFAULT '',
    source            TEXT NOT NULL DEFAULT '',
    artifact_ref      TEXT NOT NULL DEFAULT '',
    artifact_hash     TEXT NOT NULL DEFAULT '',
    tool_name         TEXT NOT NULL DEFAULT '',
    commit_sha        TEXT NOT NULL DEFAULT '',
    validation_status TEXT NOT NULL DEFAULT 'PENDING',
    observed_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id       TEXT PRIMARY KEY,
    run_id         TEXT NOT NULL DEFAULT '',
    source_event_id TEXT NOT NULL DEFAULT '',
    claim_text     TEXT NOT NULL,
    claim_type     TEXT NOT NULL DEFAULT '',
    severity       TEXT NOT NULL DEFAULT 'MEDIUM',
    status         TEXT NOT NULL DEFAULT 'claimed',
    created_at     REAL NOT NULL,
    verified_at    REAL,
    evidence_ids   TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS state_versions (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           REAL NOT NULL,
    version_type TEXT NOT NULL,
    run_id       TEXT NOT NULL DEFAULT '',
    floor_id     TEXT NOT NULL DEFAULT '',
    attempt_id   INTEGER NOT NULL DEFAULT 0,
    payload      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_run   ON events(run_id);
CREATE INDEX IF NOT EXISTS idx_events_floor ON events(floor_id);
CREATE INDEX IF NOT EXISTS idx_claims_run   ON claims(run_id);
CREATE INDEX IF NOT EXISTS idx_claims_status ON claims(status);
CREATE INDEX IF NOT EXISTS idx_evidence_run ON evidence(run_id);
"""


class Persistence:
    """Minimal append-only store. One connection guarded by a lock (single host)."""

    def __init__(self, db_path: Path | str = DB_PATH) -> None:
        self.db_path = Path(db_path)
        self._lock = threading.RLock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        if WAL:
            self._conn.execute("PRAGMA journal_mode=WAL")
        if SYNC:
            self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    # ---- runs ---------------------------------------------------------------
    def upsert_run(self, run_id: str, original_goal: str = "", floor_id: str = "",
                   attempt: int = 1, status: str = "active") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO runs(run_id, original_goal, created_at, current_floor, current_attempt, status) "
                "VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(run_id) DO UPDATE SET "
                " original_goal=excluded.original_goal, current_floor=excluded.current_floor, "
                " current_attempt=excluded.current_attempt, status=excluded.status",
                (run_id, original_goal, _now(), floor_id, attempt, status),
            )
            self._conn.commit()

    # ---- events (append-only) ----------------------------------------------
    def append_event(self, event_id: str, kind: str, ts: float, *, run_id: str = "",
                     floor_id: str = "", attempt_id: int = 0, worker_id: str = "",
                     agent_role: str = "", payload: dict[str, Any] | str = None) -> None:
        if payload is None:
            payload = {}
        if not isinstance(payload, str):
            payload = json.dumps(payload, ensure_ascii=False)
        with self._lock:
            self._conn.execute(
                "INSERT INTO events(event_id, run_id, floor_id, attempt_id, worker_id, agent_role, ts, kind, payload) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (event_id, run_id, floor_id, attempt_id, worker_id, agent_role, ts, kind, payload),
            )
            self._conn.commit()

    def events(self, run_id: str = "", floor_id: str = "", limit: int = 200) -> list[dict[str, Any]]:
        q = "SELECT * FROM events"
        cond, args = [], []
        if run_id:
            cond.append("run_id=?"); args.append(run_id)
        if floor_id:
            cond.append("floor_id=?"); args.append(floor_id)
        if cond:
            q += " WHERE " + " AND ".join(cond)
        q += " ORDER BY ts DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            cur = self._conn.execute(q, args)
            rows = cur.fetchall()
        return [_row_to_dict(cur, r) for r in rows]

    # ---- evidence (append-only) --------------------------------------------
    def append_evidence(self, evidence_id: str, run_id: str, event_id: str, kind: str,
                        observed_at: float, *, source: str = "", artifact_ref: str = "",
                        artifact_hash: str = "", tool_name: str = "",
                        commit_sha: str = "", validation_status: str = "PENDING") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO evidence(evidence_id, run_id, event_id, kind, source, artifact_ref, "
                "artifact_hash, tool_name, commit_sha, validation_status, observed_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (evidence_id, run_id, event_id, kind, source, artifact_ref, artifact_hash,
                 tool_name, commit_sha, validation_status, observed_at),
            )
            self._conn.commit()

    def evidence_for_run(self, run_id: str, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM evidence WHERE run_id=? ORDER BY observed_at DESC LIMIT ?",
                (run_id, limit),
            )
            rows = cur.fetchall()
        return [_row_to_dict(cur, r) for r in rows]

    # ---- claims (append-only lifecycle; status only via verifier) ----------
    def create_claim(self, claim_id: str, run_id: str, source_event_id: str, claim_text: str,
                     created_at: float, *, claim_type: str = "", severity: str = "MEDIUM") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO claims(claim_id, run_id, source_event_id, claim_text, claim_type, "
                "severity, status, created_at, verified_at, evidence_ids) "
                "VALUES(?,?,?,?,?,?,'claimed',?,NULL,'[]')",
                (claim_id, run_id, source_event_id, claim_text, claim_type, severity, created_at),
            )
            self._conn.commit()

    def update_claim_status(self, claim_id: str, status: str, verified_at: float | None = None,
                            evidence_ids: list[str] | None = None) -> None:
        """Transition a claim. Intended ONLY for the verifier (Phase 5)."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE claims SET status=?, verified_at=COALESCE(?, verified_at), "
                "evidence_ids=? WHERE claim_id=?",
                (status, verified_at, json.dumps(evidence_ids or [], ensure_ascii=False), claim_id),
            )
            self._conn.commit()
            return cur.rowcount

    def claims(self, run_id: str = "", status: str = "", limit: int = 200) -> list[dict[str, Any]]:
        q = "SELECT * FROM claims"
        cond, args = [], []
        if run_id:
            cond.append("run_id=?"); args.append(run_id)
        if status:
            cond.append("status=?"); args.append(status)
        if cond:
            q += " WHERE " + " AND ".join(cond)
        q += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            cur = self._conn.execute(q, args)
            rows = cur.fetchall()
        return [_row_to_dict(cur, r) for r in rows]

    def count_claims(self, run_id: str = "", status: str = "") -> int:
        q = "SELECT COUNT(*) FROM claims"
        cond, args = [], []
        if run_id:
            cond.append("run_id=?"); args.append(run_id)
        if status:
            cond.append("status=?"); args.append(status)
        if cond:
            q += " WHERE " + " AND ".join(cond)
        with self._lock:
            return int(self._conn.execute(q, args).fetchone()[0])

    def count(self, table: str) -> int:
        if table not in {"runs", "events", "claims", "evidence", "state_versions"}:
            raise ValueError(f"unsupported table {table!r}")
        with self._lock:
            return int(self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def health_ping(self) -> None:
        with self._lock:
            self._conn.execute("SELECT 1").fetchone()

    # ---- derived current-state snapshots (append-only) ---------------------
    def save_state_version(self, version_type: str, payload: dict[str, Any], *, run_id: str = "",
                           floor_id: str = "", attempt_id: int = 0) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO state_versions(ts, version_type, run_id, floor_id, attempt_id, payload) "
                "VALUES(?,?,?,?,?,?)",
                (_now(), version_type, run_id, floor_id, attempt_id,
                 json.dumps(payload, ensure_ascii=False)),
            )
            self._conn.commit()

    def latest_state_version(self, version_type: str, run_id: str = "") -> dict[str, Any] | None:
        q = "SELECT payload FROM state_versions WHERE version_type=?"
        args: list[Any] = [version_type]
        if run_id:
            q += " AND run_id=?"
            args.append(run_id)
        q += " ORDER BY seq DESC LIMIT 1"
        with self._lock:
            row = self._conn.execute(q, args).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None


def _now() -> float:
    import time
    return time.time()


def _row_to_dict(cursor, row) -> dict[str, Any]:
    cols = [d[0] for d in cursor.description or []]
    out = dict(zip(cols, row))
    for key in ("payload", "evidence_ids"):
        if key in out and isinstance(out[key], str):
            try:
                out[key] = json.loads(out[key])
            except Exception:
                pass
    return out


_default: Persistence | None = None
_default_lock = threading.Lock()


def get_db() -> Persistence:
    global _default
    with _default_lock:
        if _default is None:
            _default = Persistence()
        return _default
