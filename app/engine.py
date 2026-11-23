"""Core reconciliation engine.

For a batch of scoring receipts the engine pins **one** committed SQLite
snapshot, loads the warehouse rows for every (turbine, sample minute) the
batch touches in a **single** joined query, and evaluates two lists per
receipt:

``usage``  实际用数 vs 当时应取
    The reading the score actually consumed (receipt's used revision) against
    the reading that should have been used at scoring time.

``drift``  当时应取 vs 当前最高修订
    The reading that should have been used at scoring time against the
    highest revision currently in the warehouse - i.e. what changed later.

Selection rule ("当时应取"):

* The window is the 60 sampling minutes immediately before the scoring
  minute: ``score_minute - 60min <= s < score_minute``.  A sample at the
  scoring minute itself is not "before" the score.
* Inside the window, only revisions **already ingested** by the score minute
  count: ``ingested_at < score_minute`` (strictly, at second precision).
* Of those, the one with the **highest revision** is used.
* If nothing qualifies the expected reading is missing (缺测留空).

No query is issued per receipt: the batch costs O(batch + warehouse rows for
the touched minutes).
"""

from __future__ import annotations

import sqlite3
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from .db import Database
from .domain import (
    MAX_BATCH_SIZE,
    BatchTooLarge,
    Receipt,
    UnknownReceipts,
    format_instant,
    format_minute,
    minute_datetime,
    parse_minute,
    parse_text,
)
from .schemas import ReconcileRequest

RULE_TEXT = "打分前一小时内，同一采样分钟取打分前已入仓的最高修订；缺测留空。"

WINDOW_START_INCLUSIVE = True
WINDOW_END_INCLUSIVE = False
WINDOW_HOURS = 1


@dataclass(frozen=True)
class _Row:
    revision: int
    ingested_at: str  # canonical instant string
    wind_speed: float | None


@dataclass
class _PairData:
    rows: list[_Row]
    by_revision: dict[int, _Row]

    @property
    def highest(self) -> _Row | None:
        return self.rows[-1] if self.rows else None


# Public aliases for the offline replayer (app.replay): it re-runs the exact
# same evaluation logic on desensitised bundle data, so it builds these row
# shapes directly instead of loading them from SQLite.
Row = _Row
PairData = _PairData


def _instant_cutoff(score_minute: str) -> str:
    """The scoring minute as an instant, for a strict ``<`` comparison."""
    return format_instant(minute_datetime(score_minute))


def _window_bounds(score_minute: str) -> tuple[str, str]:
    start = minute_datetime(score_minute) - timedelta(hours=WINDOW_HOURS)
    return format_minute(start), score_minute


def _in_window(sample_minute: str, window_start: str, window_end: str) -> bool:
    if WINDOW_START_INCLUSIVE:
        if sample_minute < window_start:
            return False
    elif sample_minute <= window_start:
        return False
    if WINDOW_END_INCLUSIVE:
        return sample_minute <= window_end
    return sample_minute < window_end


def _window_text(window_start: str, window_end: str) -> str:
    left = "[" if WINDOW_START_INCLUSIVE else "("
    right = "]" if WINDOW_END_INCLUSIVE else ")"
    return f"{left}{window_start}, {window_end}{right}"


def _side(
    label: str,
    row: _Row | None,
    *,
    sample_minute: str | None,
    ingested_before_score: bool | None = None,
) -> dict[str, Any]:
    return {
        "label": label,
        "present": row is not None,
        "sample_minute": sample_minute,
        "revision": None if row is None else row.revision,
        "wind_speed": None if row is None else row.wind_speed,
        "ingested_at": None if row is None else row.ingested_at,
        "ingested_before_score": ingested_before_score,
    }


# --------------------------------------------------------------------------
# loading
# --------------------------------------------------------------------------
def _receipt_from_row(row: sqlite3.Row) -> Receipt:
    return Receipt(
        receipt_id=row["receipt_id"],
        turbine_id=row["turbine_id"],
        score_minute=row["score_minute"],
        used_sample_minute=row["used_sample_minute"],
        used_revision=row["used_revision"],
    )


def _load_receipts(conn: sqlite3.Connection, request: ReconcileRequest) -> list[Receipt]:
    if request.receipt_ids is not None:
        ids: list[str] = []
        seen: set[str] = set()
        for raw in request.receipt_ids:
            rid = parse_text(raw, field_name="receipt_id")
            if rid not in seen:
                seen.add(rid)
                ids.append(rid)
        if len(ids) > MAX_BATCH_SIZE:
            raise BatchTooLarge(len(ids))
        if not ids:
            return []
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _batch_ids (receipt_id TEXT PRIMARY KEY)"
        )
        conn.execute("DELETE FROM _batch_ids")
        conn.executemany(
            "INSERT INTO _batch_ids (receipt_id) VALUES (?)", [(rid,) for rid in ids]
        )
        rows = conn.execute(
            """
            SELECT r.receipt_id, r.turbine_id, r.score_minute,
                   r.used_sample_minute, r.used_revision
              FROM receipts r
              JOIN _batch_ids b ON b.receipt_id = r.receipt_id
             ORDER BY r.receipt_id
            """
        ).fetchall()
        found = {row["receipt_id"] for row in rows}
        missing = [rid for rid in ids if rid not in found]
        if missing:
            raise UnknownReceipts(missing)
        return [_receipt_from_row(row) for row in rows]

    where: list[str] = []
    params: list[Any] = []
    if request.turbine_id is not None:
        where.append("turbine_id = ?")
        params.append(parse_text(request.turbine_id, field_name="turbine_id"))
    if request.score_minute_from is not None:
        where.append("score_minute >= ?")
        params.append(parse_minute(request.score_minute_from, field_name="score_minute_from"))
    if request.score_minute_to is not None:
        where.append("score_minute <= ?")
        params.append(parse_minute(request.score_minute_to, field_name="score_minute_to"))
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(
        f"SELECT COUNT(*) AS n FROM receipts{clause}", params
    ).fetchone()["n"]
    if total > MAX_BATCH_SIZE:
        raise BatchTooLarge(total)
    rows = conn.execute(
        f"""
        SELECT receipt_id, turbine_id, score_minute,
               used_sample_minute, used_revision
          FROM receipts{clause}
         ORDER BY receipt_id
        """,
        params,
    ).fetchall()
    return [_receipt_from_row(row) for row in rows]


def _load_pairs(
    conn: sqlite3.Connection, receipts: Sequence[Receipt]
) -> dict[tuple[str, str], _PairData]:
    """Load every warehouse row the batch can need, in one joined query."""
    if not receipts:
        return {}
    conn.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS _batch_pairs (
            turbine_id TEXT NOT NULL,
            sample_minute TEXT NOT NULL,
            PRIMARY KEY (turbine_id, sample_minute)
        )
        """
    )
    conn.execute("DELETE FROM _batch_pairs")
    pairs = {(r.turbine_id, r.used_sample_minute) for r in receipts}
    conn.executemany(
        "INSERT OR IGNORE INTO _batch_pairs (turbine_id, sample_minute) VALUES (?, ?)",
        sorted(pairs),
    )
    rows = conn.execute(
        """
        SELECT w.turbine_id, w.sample_minute, w.revision, w.ingested_at, w.wind_speed
          FROM _batch_pairs p
         CROSS JOIN wind_readings w
         WHERE w.turbine_id = p.turbine_id
           AND w.sample_minute = p.sample_minute
        """
    ).fetchall()
    grouped: dict[tuple[str, str], list[_Row]] = defaultdict(list)
    for row in rows:
        grouped[(row["turbine_id"], row["sample_minute"])].append(
            _Row(
                revision=int(row["revision"]),
                ingested_at=row["ingested_at"],
                wind_speed=row["wind_speed"],
            )
        )
    result: dict[tuple[str, str], _PairData] = {}
    for key, items in grouped.items():
        items.sort(key=lambda item: item.revision)
        result[key] = _PairData(
            rows=items, by_revision={item.revision: item for item in items}
        )
    return result


# --------------------------------------------------------------------------
# evaluation
# --------------------------------------------------------------------------
def _missing_reason(
    sample_minute: str,
    window_start: str,
    window_end: str,
    pair: _PairData | None,
    cutoff: str,
) -> str:
    if not _in_window(sample_minute, window_start, window_end):
        return (
            f"采样分钟 {sample_minute} 不在打分前一小时窗口 {_window_text(window_start, window_end)} 内，"
            "按规则当时应缺测留空"
        )
    if pair is None or not pair.rows:
        return f"采样分钟 {sample_minute} 在窗口内于数仓无任何读数（缺测）"
    if not any(row.ingested_at < cutoff for row in pair.rows):
        earliest = min(row.ingested_at for row in pair.rows)
        return (
            f"采样分钟 {sample_minute} 的全部修订均在打分时刻之后才入仓"
            f"（最早入仓 {earliest}），打分时按规则应缺测留空"
        )
    return f"采样分钟 {sample_minute} 在打分时刻之前无已入仓读数（缺测）"


def _usage_findings(
    receipt: Receipt,
    actual: _Row | None,
    expected: _Row | None,
    window_start: str,
    window_end: str,
    pair: _PairData | None,
    cutoff: str,
) -> list[dict[str, Any]]:
    if actual is not None and expected is not None and actual.revision == expected.revision:
        return []
    actual_side = _side(
        "实际用数",
        actual,
        sample_minute=receipt.used_sample_minute,
        ingested_before_score=None if actual is None else actual.ingested_at < cutoff,
    )
    expected_side = _side(
        "当时应取",
        expected,
        sample_minute=receipt.used_sample_minute,
        ingested_before_score=None if expected is None else True,
    )
    in_window = _in_window(receipt.used_sample_minute, window_start, window_end)
    if actual is None:
        detail = f"缺行：回执引用的修订 {receipt.used_revision} 在数仓中不存在；"
        detail += (
            f"打分时按规则应取修订 {expected.revision}（入仓 {expected.ingested_at}）"
            if expected is not None
            else _missing_reason(
                receipt.used_sample_minute, window_start, window_end, pair, cutoff
            )
        )
        kind = "missing"
    elif not in_window:
        detail = (
            f"采样分钟越界：实际使用了窗口外的采样分钟 {receipt.used_sample_minute} 的修订 "
            f"{actual.revision}（入仓 {actual.ingested_at}）；"
            f"窗口 {_window_text(window_start, window_end)} 内按规则应缺测留空"
        )
        kind = "window"
    elif expected is None:
        detail = (
            f"缺测：实际使用了修订 {actual.revision}（入仓 {actual.ingested_at}）；"
            + _missing_reason(
                receipt.used_sample_minute, window_start, window_end, pair, cutoff
            )
        )
        kind = "missing"
    else:
        detail = (
            f"换版：实际使用修订 {actual.revision}（入仓 {actual.ingested_at}），"
            f"打分时应取已入仓的最高修订 {expected.revision}（入仓 {expected.ingested_at}）"
        )
        kind = "revision"
    return [{"kind": kind, "detail": detail, "left": actual_side, "right": expected_side}]


def _drift_findings(
    receipt: Receipt,
    expected: _Row | None,
    current: _Row | None,
    *,
    in_window: bool,
) -> list[dict[str, Any]]:
    if not in_window:
        # The sample minute was never eligible, so "当时应取" is missing by
        # rule rather than by warehouse state; comparing it with today's
        # reading of an out-of-window minute would be noise.
        return []
    expected_side = _side(
        "当时应取",
        expected,
        sample_minute=receipt.used_sample_minute,
        ingested_before_score=None if expected is None else True,
    )
    current_side = _side(
        "当前最高修订", current, sample_minute=receipt.used_sample_minute
    )
    if expected is None and current is None:
        return []
    if expected is None:
        return [
            {
                "kind": "missing",
                "detail": (
                    "当时应缺测留空，当前数仓已有最高修订 "
                    f"{current.revision}（入仓 {current.ingested_at}），读数事后才补录"
                ),
                "left": expected_side,
                "right": current_side,
            }
        ]
    if current is None:
        return [
            {
                "kind": "missing",
                "detail": f"当时应取修订 {expected.revision}，该读数当前已不在数仓中（缺行）",
                "left": expected_side,
                "right": current_side,
            }
        ]
    if expected.revision != current.revision:
        return [
            {
                "kind": "revision",
                "detail": (
                    f"换版：当时应取修订 {expected.revision}，"
                    f"当前最高修订已变为 {current.revision}（入仓 {current.ingested_at}）"
                ),
                "left": expected_side,
                "right": current_side,
            }
        ]
    return []


def _evidence(
    receipt: Receipt,
    actual: _Row | None,
    expected: _Row | None,
    current: _Row | None,
    window_start: str,
    window_end: str,
    pair: _PairData | None,
    cutoff: str,
) -> dict[str, Any]:
    in_window = _in_window(receipt.used_sample_minute, window_start, window_end)
    trail = []
    if pair is not None:
        for row in pair.rows:
            trail.append(
                {
                    "revision": row.revision,
                    "ingested_at": row.ingested_at,
                    "wind_speed": row.wind_speed,
                    "ingested_before_score": row.ingested_at < cutoff,
                    "eligible": in_window and row.ingested_at < cutoff,
                }
            )
    reasons = []
    if not in_window:
        reasons.append(
            f"采样分钟不在窗口 {_window_text(window_start, window_end)} 内"
        )
    elif expected is None:
        reasons.append(
            _missing_reason(
                receipt.used_sample_minute, window_start, window_end, pair, cutoff
            )
        )
    if actual is None:
        reasons.append(f"回执引用的修订 {receipt.used_revision} 在数仓中不存在（缺行）")
    if current is None:
        reasons.append("该采样分钟当前在数仓中无读数")
    return {
        "rule": RULE_TEXT,
        "window": {
            "kind": "rolling_hour_before_score",
            "score_minute": receipt.score_minute,
            "start": window_start,
            "end": window_end,
            "start_inclusive": WINDOW_START_INCLUSIVE,
            "end_inclusive": WINDOW_END_INCLUSIVE,
            "display": _window_text(window_start, window_end),
            "length": "PT1H",
            "turbine_id": receipt.turbine_id,
            "sample_minute": receipt.used_sample_minute,
            "sample_minute_in_window": in_window,
        },
        "selection": {
            "revision_rule": "窗口内、入仓时刻早于打分时刻的最高修订",
            "cutoff_instant": cutoff,
            "score_minute": receipt.score_minute,
        },
        "used_revision": receipt.used_revision,
        "actual": _side(
            "实际用数",
            actual,
            sample_minute=receipt.used_sample_minute,
            ingested_before_score=None if actual is None else actual.ingested_at < cutoff,
        ),
        "should_use_at_score": _side(
            "当时应取",
            expected,
            sample_minute=receipt.used_sample_minute,
            ingested_before_score=None if expected is None else True,
        ),
        "current_highest": _side(
            "当前最高修订", current, sample_minute=receipt.used_sample_minute
        ),
        "revisions": trail,
        "reasons": reasons,
    }


def _evaluate(
    receipts: Sequence[Receipt],
    pairs: dict[tuple[str, str], _PairData],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for receipt in receipts:
        pair = pairs.get((receipt.turbine_id, receipt.used_sample_minute))
        cutoff = _instant_cutoff(receipt.score_minute)
        window_start, window_end = _window_bounds(receipt.score_minute)
        in_window = _in_window(
            receipt.used_sample_minute, window_start, window_end
        )
        actual = pair.by_revision.get(receipt.used_revision) if pair else None
        eligible = (
            [row for row in pair.rows if row.ingested_at < cutoff]
            if pair is not None and in_window
            else []
        )
        expected = max(eligible, key=lambda row: row.revision) if eligible else None
        current = pair.highest if pair else None
        usage = _usage_findings(
            receipt, actual, expected, window_start, window_end, pair, cutoff
        )
        drift = _drift_findings(receipt, expected, current, in_window=in_window)
        evidence = _evidence(
            receipt,
            actual,
            expected,
            current,
            window_start,
            window_end,
            pair,
            cutoff,
        )
        items.append(
            {
                "receipt_id": receipt.receipt_id,
                "turbine_id": receipt.turbine_id,
                "score_minute": receipt.score_minute,
                "used_sample_minute": receipt.used_sample_minute,
                "used_revision": receipt.used_revision,
                "usage": usage,
                "drift": drift,
                "evidence": evidence,
            }
        )
    return items


def evaluate_receipts(
    receipts: Sequence[Receipt],
    pairs: dict[tuple[str, str], _PairData],
) -> list[dict[str, Any]]:
    """Public wrapper around the batch evaluator.

    The offline replayer calls this with synthetic receipts/pairs rebuilt
    from a replay bundle so that online and offline runs share one code path.
    """
    return _evaluate(receipts, pairs)


def reconcile_snapshot(
    db: Database, request: ReconcileRequest
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run one batch against a single committed snapshot.

    A read transaction is opened explicitly and held for the entire batch, so
    every receipt sees the same warehouse state.  Concurrent back-fill writes
    keep working (WAL) and simply land in a later snapshot.
    """
    conn = db.connect()
    committed = False
    try:
        conn.execute("BEGIN")
        try:
            receipts = _load_receipts(conn, request)
            wind_max_id = conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS max_id FROM wind_readings"
            ).fetchone()["max_id"]
            pairs = _load_pairs(conn, receipts)
            snapshot = {
                "mode": "single_committed_snapshot",
                "isolation": "SQLite WAL read transaction",
                "computed_at": format_instant(datetime.now(timezone.utc)),
                "wind_max_id": int(wind_max_id),
                "receipt_count": len(receipts),
                "rule": RULE_TEXT,
                "window": {
                    "hours": WINDOW_HOURS,
                    "start_inclusive": WINDOW_START_INCLUSIVE,
                    "end_inclusive": WINDOW_END_INCLUSIVE,
                },
            }
            items = _evaluate(receipts, pairs)
            conn.execute("COMMIT")
            committed = True
        finally:
            if not committed:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
    finally:
        conn.close()
    return items, snapshot


def request_document(request: ReconcileRequest) -> dict[str, Any]:
    return {
        "receipt_ids": None
        if request.receipt_ids is None
        else [parse_text(rid, field_name="receipt_id") for rid in request.receipt_ids],
        "turbine_id": request.turbine_id,
        "score_minute_from": request.score_minute_from,
        "score_minute_to": request.score_minute_to,
    }


def new_report_id() -> str:
    return "rpt_" + uuid.uuid4().hex
