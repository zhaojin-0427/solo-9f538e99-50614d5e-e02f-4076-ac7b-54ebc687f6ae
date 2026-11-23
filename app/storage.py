"""Import, persistence and retrieval of warehouse data and reports.

All writes are wrapped in a single ``BEGIN IMMEDIATE`` transaction: a report
is either stored completely (header + every item) or not at all, so a failed
batch never leaves a half-written report behind.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Sequence

from pydantic import ValidationError as PydanticValidationError

from .db import Database
from .domain import (
    ValidationError,
    format_instant,
    parse_instant,
    parse_minute,
    parse_revision,
    parse_text,
    parse_wind,
)
from .schemas import ReadingIn, ReceiptIn


def _now() -> str:
    return format_instant(datetime.now(timezone.utc))


def _row_error(index: int, exc: Exception) -> dict[str, Any]:
    if isinstance(exc, PydanticValidationError):
        errors = []
        for item in exc.errors():
            field = ".".join(str(part) for part in item.get("loc", ())) or "<row>"
            errors.append({"field": field, "message": item.get("msg", "invalid value")})
        return {"row": index, "errors": errors}
    return {"row": index, "errors": [{"field": "<row>", "message": str(exc)}]}


def to_reading(row: ReadingIn) -> tuple[str, str, int, str, float | None]:
    return (
        parse_text(row.turbine_id, field_name="turbine_id"),
        parse_minute(row.sample_minute, field_name="sample_minute"),
        parse_revision(row.revision),
        parse_instant(row.ingested_at, field_name="ingested_at"),
        parse_wind(row.wind_speed),
    )


def to_receipt(row: ReceiptIn) -> tuple[str, str, str, str, int]:
    return (
        parse_text(row.receipt_id, field_name="receipt_id"),
        parse_text(row.turbine_id, field_name="turbine_id"),
        parse_minute(row.score_minute, field_name="score_minute"),
        parse_minute(row.used_sample_minute, field_name="used_sample_minute"),
        parse_revision(row.used_revision, field_name="used_revision"),
    )


def validate_reading_rows(rows: Sequence[Any]) -> list[tuple[str, str, int, str, float | None]]:
    parsed = []
    errors = []
    for index, raw in enumerate(rows):
        try:
            parsed.append(to_reading(ReadingIn.model_validate(raw)))
        except (PydanticValidationError, ValidationError) as exc:
            errors.append(_row_error(index, exc))
    if errors:
        raise ImportValidationError(errors)
    return parsed


def validate_receipt_rows(rows: Sequence[Any]) -> list[tuple[str, str, str, str, int]]:
    parsed = []
    errors = []
    for index, raw in enumerate(rows):
        try:
            parsed.append(to_receipt(ReceiptIn.model_validate(raw)))
        except (PydanticValidationError, ValidationError) as exc:
            errors.append(_row_error(index, exc))
    if errors:
        raise ImportValidationError(errors)
    return parsed


class ImportValidationError(ValueError):
    """Row-level import validation failures, reported with 1-based row numbers."""

    def __init__(self, errors: list[dict[str, Any]]) -> None:
        self.errors = errors
        super().__init__(f"{len(errors)} row(s) failed validation")

    def payload(self) -> dict[str, Any]:
        return {
            "message": self.args[0] if self.args else "validation failed",
            "errors": self.errors,
        }


def _dedupe_payload(payload: list[Any]) -> tuple[list[Any], int]:
    """Collapse byte-identical duplicate dict rows inside one request."""
    seen: set[str] = set()
    unique: list[Any] = []
    identical = 0
    for raw in payload:
        try:
            key = json.dumps(raw, sort_keys=True, ensure_ascii=False, default=str)
        except TypeError:
            unique.append(raw)
            continue
        if key in seen:
            identical += 1
            continue
        seen.add(key)
        unique.append(raw)
    return unique, identical


def import_readings(db: Database, payload: list[Any]) -> dict[str, Any]:
    unique, identical = _dedupe_payload(payload)
    parsed = validate_reading_rows(unique)
    # Last occurrence of a duplicated key wins inside the request.
    latest: dict[tuple[str, str, int], tuple[str, str, int, str, float | None]] = {}
    for row in parsed:
        latest[(row[0], row[1], row[2])] = row
    now = _now()
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                """
                INSERT INTO wind_readings
                    (turbine_id, sample_minute, revision, ingested_at,
                     wind_speed, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (turbine_id, sample_minute, revision) DO UPDATE SET
                    ingested_at = excluded.ingested_at,
                    wind_speed  = excluded.wind_speed,
                    updated_at  = excluded.updated_at
                """,
                [
                    (t, m, rev, ing, wind, now, now)
                    for (t, m, rev, ing, wind) in latest.values()
                ],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        total = conn.execute("SELECT COUNT(*) AS n FROM wind_readings").fetchone()["n"]
    finally:
        conn.close()
    return {
        "accepted": len(latest),
        "total": int(total),
        "identical_duplicates": identical,
    }


def import_receipts(db: Database, payload: list[Any]) -> dict[str, Any]:
    unique, identical = _dedupe_payload(payload)
    parsed = validate_receipt_rows(unique)
    latest: dict[str, tuple[str, str, str, str, int]] = {}
    for row in parsed:
        latest[row[0]] = row
    now = _now()
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                """
                INSERT INTO receipts
                    (receipt_id, turbine_id, score_minute, used_sample_minute,
                     used_revision, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (receipt_id) DO UPDATE SET
                    turbine_id         = excluded.turbine_id,
                    score_minute       = excluded.score_minute,
                    used_sample_minute = excluded.used_sample_minute,
                    used_revision      = excluded.used_revision,
                    updated_at         = excluded.updated_at
                """,
                [
                    (rid, t, sm, usm, urev, now, now)
                    for (rid, t, sm, usm, urev) in latest.values()
                ],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        total = conn.execute("SELECT COUNT(*) AS n FROM receipts").fetchone()["n"]
    finally:
        conn.close()
    return {
        "accepted": len(latest),
        "total": int(total),
        "identical_duplicates": identical,
    }


def persist_report(
    db: Database,
    items: Sequence[dict[str, Any]],
    snapshot: dict[str, Any],
    request: dict[str, Any],
    report_id: str,
) -> dict[str, Any]:
    """Store a report atomically.  A failure leaves no report row behind."""
    created_at = format_instant(datetime.now(timezone.utc))
    usage_count = sum(len(item["usage"]) for item in items)
    drift_count = sum(len(item["drift"]) for item in items)
    items_with_findings = sum(
        1 for item in items if item["usage"] or item["drift"]
    )
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                """
                INSERT INTO reports
                    (report_id, status, created_at, receipt_count,
                     usage_finding_count, drift_finding_count,
                     items_with_findings, snapshot_json, request_json)
                VALUES (?, 'complete', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    created_at,
                    len(items),
                    usage_count,
                    drift_count,
                    items_with_findings,
                    json.dumps(snapshot, ensure_ascii=False),
                    json.dumps(request, ensure_ascii=False),
                ),
            )
            conn.executemany(
                """
                INSERT INTO report_items
                    (report_id, seq, receipt_id, turbine_id, score_minute,
                     used_sample_minute, used_revision, usage_json, drift_json,
                     evidence_json, usage_finding_count, drift_finding_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        report_id,
                        seq,
                        item["receipt_id"],
                        item["turbine_id"],
                        item["score_minute"],
                        item["used_sample_minute"],
                        item["used_revision"],
                        json.dumps(item["usage"], ensure_ascii=False),
                        json.dumps(item["drift"], ensure_ascii=False),
                        json.dumps(item["evidence"], ensure_ascii=False),
                        len(item["usage"]),
                        len(item["drift"]),
                    )
                    for seq, item in enumerate(items)
                ],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()
    return {
        "report_id": report_id,
        "status": "complete",
        "created_at": created_at,
        "receipt_count": len(items),
        "usage_finding_count": usage_count,
        "drift_finding_count": drift_count,
        "items_with_findings": items_with_findings,
    }


def _summary(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "report_id": row["report_id"],
        "status": row["status"],
        "created_at": row["created_at"],
        "receipt_count": row["receipt_count"],
        "usage_finding_count": row["usage_finding_count"],
        "drift_finding_count": row["drift_finding_count"],
        "items_with_findings": row["items_with_findings"],
    }


SUMMARY_COLUMNS = """
    report_id, status, created_at, receipt_count, usage_finding_count,
    drift_finding_count, items_with_findings
"""


def list_reports(db: Database, offset: int, limit: int) -> tuple[list[dict], int]:
    conn = db.connect()
    try:
        total = conn.execute("SELECT COUNT(*) AS n FROM reports").fetchone()["n"]
        rows = conn.execute(
            f"""
            SELECT {SUMMARY_COLUMNS}
              FROM reports
             ORDER BY created_at DESC, report_id DESC
             LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
    finally:
        conn.close()
    return [_summary(row) for row in rows], int(total)


def get_report_row(db: Database, report_id: str) -> sqlite3.Row | None:
    conn = db.connect()
    try:
        return conn.execute(
            f"SELECT {SUMMARY_COLUMNS}, snapshot_json, request_json "
            "FROM reports WHERE report_id = ?",
            (report_id,),
        ).fetchone()
    finally:
        conn.close()


def get_report_items(
    db: Database,
    report_id: str,
    *,
    offset: int,
    limit: int | None,
    receipt_id: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    conn = db.connect()
    try:
        where = "report_id = ?"
        params: list[Any] = [report_id]
        if receipt_id is not None:
            where += " AND receipt_id = ?"
            params.append(receipt_id)
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM report_items WHERE {where}", params
        ).fetchone()["n"]
        clause = ""
        page_params = list(params)
        if limit is not None:
            clause = " LIMIT ? OFFSET ?"
            page_params.extend([limit, offset])
        elif offset:
            clause = " LIMIT -1 OFFSET ?"
            page_params.append(offset)
        rows = conn.execute(
            f"""
            SELECT receipt_id, turbine_id, score_minute, used_sample_minute,
                   used_revision, usage_json, drift_json, evidence_json
              FROM report_items
             WHERE {where}
             ORDER BY seq
            {clause}
            """,
            page_params,
        ).fetchall()
    finally:
        conn.close()
    items = [
        {
            "receipt_id": row["receipt_id"],
            "turbine_id": row["turbine_id"],
            "score_minute": row["score_minute"],
            "used_sample_minute": row["used_sample_minute"],
            "used_revision": row["used_revision"],
            "usage": json.loads(row["usage_json"]),
            "drift": json.loads(row["drift_json"]),
            "evidence": json.loads(row["evidence_json"]),
        }
        for row in rows
    ]
    return items, int(total)


def reports_for_receipt(db: Database, receipt_id: str) -> list[dict[str, Any]]:
    conn = db.connect()
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT {', '.join('r.' + c.strip() for c in SUMMARY_COLUMNS.split(','))}
              FROM reports r
              JOIN report_items i ON i.report_id = r.report_id
             WHERE i.receipt_id = ?
             ORDER BY r.created_at DESC, r.report_id DESC
            """,
            (receipt_id,),
        ).fetchall()
    finally:
        conn.close()
    return [_summary(row) for row in rows]


def list_readings(
    db: Database,
    *,
    turbine_id: str | None,
    offset: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    where: list[str] = []
    params: list[Any] = []
    if turbine_id is not None:
        where.append("turbine_id = ?")
        params.append(turbine_id)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    conn = db.connect()
    try:
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM wind_readings{clause}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"""
            SELECT turbine_id, sample_minute, revision, ingested_at, wind_speed
              FROM wind_readings{clause}
             ORDER BY turbine_id, sample_minute, revision
             LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows], int(total)


def list_receipts(
    db: Database,
    *,
    turbine_id: str | None,
    offset: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    where: list[str] = []
    params: list[Any] = []
    if turbine_id is not None:
        where.append("turbine_id = ?")
        params.append(turbine_id)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    conn = db.connect()
    try:
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM receipts{clause}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"""
            SELECT receipt_id, turbine_id, score_minute, used_sample_minute,
                   used_revision
              FROM receipts{clause}
             ORDER BY score_minute, receipt_id
             LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows], int(total)
