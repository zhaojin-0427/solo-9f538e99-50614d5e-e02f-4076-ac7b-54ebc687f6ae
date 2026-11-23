"""SQLite connection management and schema for the reconciliation service.

The database runs in WAL mode so that a long-running reconciliation read
transaction (the required "one committed state" for a batch) never blocks the
continuous back-fill writes.  Every request gets its own connection; SQLite
connections are not shared between threads.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS wind_readings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    turbine_id    TEXT    NOT NULL,
    sample_minute TEXT    NOT NULL,
    revision      INTEGER NOT NULL CHECK (revision >= 0),
    ingested_at   TEXT    NOT NULL,
    wind_speed    REAL,
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL,
    UNIQUE (turbine_id, sample_minute, revision)
);

CREATE INDEX IF NOT EXISTS idx_wind_lookup
    ON wind_readings (turbine_id, sample_minute, revision);

CREATE INDEX IF NOT EXISTS idx_wind_ingest
    ON wind_readings (turbine_id, sample_minute, ingested_at);

CREATE TABLE IF NOT EXISTS receipts (
    receipt_id         TEXT    PRIMARY KEY,
    turbine_id         TEXT    NOT NULL,
    score_minute       TEXT    NOT NULL,
    used_sample_minute TEXT    NOT NULL,
    used_revision      INTEGER NOT NULL CHECK (used_revision >= 0),
    created_at         TEXT    NOT NULL,
    updated_at         TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_receipts_turbine
    ON receipts (turbine_id, score_minute);

CREATE TABLE IF NOT EXISTS reports (
    report_id            TEXT    PRIMARY KEY,
    status               TEXT    NOT NULL,
    created_at           TEXT    NOT NULL,
    receipt_count        INTEGER NOT NULL,
    usage_finding_count  INTEGER NOT NULL,
    drift_finding_count  INTEGER NOT NULL,
    items_with_findings  INTEGER NOT NULL,
    snapshot_json        TEXT    NOT NULL,
    request_json         TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS report_items (
    report_id            TEXT    NOT NULL,
    seq                  INTEGER NOT NULL,
    receipt_id           TEXT    NOT NULL,
    turbine_id           TEXT    NOT NULL,
    score_minute         TEXT    NOT NULL,
    used_sample_minute   TEXT    NOT NULL,
    used_revision        INTEGER NOT NULL,
    usage_json           TEXT    NOT NULL,
    drift_json           TEXT    NOT NULL,
    evidence_json        TEXT    NOT NULL,
    usage_finding_count  INTEGER NOT NULL,
    drift_finding_count  INTEGER NOT NULL,
    PRIMARY KEY (report_id, seq),
    FOREIGN KEY (report_id) REFERENCES reports (report_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_report_items_receipt
    ON report_items (report_id, receipt_id);
"""


class Database:
    """Owns the SQLite file and hands out configured connections."""

    def __init__(self, path: str | Path, *, trace: bool = False) -> None:
        self.path = str(path)
        self._keeper: sqlite3.Connection | None = None
        self._memory_uri: str | None = None
        self._trace_enabled = trace
        # Diagnostic counter: number of SELECT statements issued.  Tests use
        # it to prove a batch does not scan the warehouse per receipt.
        self.select_count = 0
        if self.path == ":memory:":
            # Plain ``:memory:`` databases are private to each connection.
            # A shared-cache URI plus a keeper connection gives every request
            # connection the same in-memory database (handy for tests).
            self._memory_uri = (
                "file:windscore_" + uuid.uuid4().hex + "?mode=memory&cache=shared"
            )
            self._keeper = self._open(uri=True)
        else:
            Path(self.path).expanduser().resolve().parent.mkdir(
                parents=True, exist_ok=True
            )

    def _trace(self, sql: str) -> None:
        if sql.lstrip().upper().startswith("SELECT"):
            self.select_count += 1

    # -- connections -----------------------------------------------------
    def _open(self, *, uri: bool = False) -> sqlite3.Connection:
        target = self._memory_uri if uri else self.path
        conn = sqlite3.connect(
            target,
            uri=uri,
            timeout=30.0,
            isolation_level=None,  # explicit transaction control
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA temp_store = MEMORY")
        if self._trace_enabled:
            conn.set_trace_callback(self._trace)
        return conn

    def connect(self) -> sqlite3.Connection:
        if self._memory_uri is not None:
            return self._open(uri=True)
        return self._open()

    def initialize(self) -> None:
        conn = self.connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    def close(self) -> None:
        if self._keeper is not None:
            self._keeper.close()
            self._keeper = None
