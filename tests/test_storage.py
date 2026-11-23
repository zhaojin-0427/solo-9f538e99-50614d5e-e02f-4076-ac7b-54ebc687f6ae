"""Persistence-level tests: atomicity, idempotence and snapshot evidence."""

from __future__ import annotations

import sqlite3

import pytest

from app.engine import reconcile_snapshot
from app.schemas import ReconcileRequest
from app.storage import (
    get_report_items,
    get_report_row,
    import_readings,
    import_receipts,
    persist_report,
)


def seed(db):
    import_readings(
        db,
        [
            {
                "turbine_id": "WT",
                "sample_minute": "2026-01-01T10:00:00Z",
                "revision": 1,
                "ingested_at": "2026-01-01T10:01:00Z",
                "wind_speed": 6.0,
            }
        ],
    )
    import_receipts(
        db,
        [
            {
                "receipt_id": "R1",
                "turbine_id": "WT",
                "score_minute": "2026-01-01T11:00:00Z",
                "used_sample_minute": "2026-01-01T10:00:00Z",
                "used_revision": 1,
            }
        ],
    )


def test_persist_is_atomic_when_the_header_insert_fails(db):
    seed(db)
    items, snapshot = reconcile_snapshot(db, ReconcileRequest(receipt_ids=["R1"]))
    persist_report(db, items, snapshot, {}, "rpt_first")
    # Same id again: header insert violates the PK after items were planned.
    with pytest.raises(sqlite3.IntegrityError):
        persist_report(db, items, snapshot, {}, "rpt_first")
    conn = db.connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 1
        # The failed attempt must not have duplicated the item rows.
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM report_items WHERE report_id='rpt_first'"
            ).fetchone()[0]
            == 1
        )
    finally:
        conn.close()


def test_persist_is_atomic_when_an_item_insert_fails(db):
    seed(db)
    items, snapshot = reconcile_snapshot(db, ReconcileRequest(receipt_ids=["R1"]))
    # A NULL receipt_id violates report_items.receipt_id NOT NULL, i.e. the
    # failure happens after the report header has already been inserted.
    broken = [dict(items[0], receipt_id=None)]
    with pytest.raises(sqlite3.IntegrityError):
        persist_report(db, broken, snapshot, {}, "rpt_broken")
    conn = db.connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM report_items").fetchone()[0] == 0
    finally:
        conn.close()


def test_deleting_a_report_cascades_to_items(db):
    seed(db)
    items, snapshot = reconcile_snapshot(db, ReconcileRequest(receipt_ids=["R1"]))
    persist_report(db, items, snapshot, {}, "rpt_a")
    conn = db.connect()
    try:
        conn.execute("DELETE FROM reports WHERE report_id='rpt_a'")
        assert conn.execute("SELECT COUNT(*) FROM report_items").fetchone()[0] == 0
    finally:
        conn.close()


def test_report_keeps_its_own_snapshot_after_later_backfill(db):
    seed(db)
    items, snapshot = reconcile_snapshot(db, ReconcileRequest(receipt_ids=["R1"]))
    persist_report(db, items, snapshot, {}, "rpt_old")
    import_readings(
        db,
        [
            {
                "turbine_id": "WT",
                "sample_minute": "2026-01-01T10:00:00Z",
                "revision": 2,
                "ingested_at": "2026-01-01T11:30:00Z",
                "wind_speed": 7.0,
            }
        ],
    )
    row = get_report_row(db, "rpt_old")
    assert row["snapshot_json"]
    old_items, _ = get_report_items(db, "rpt_old", offset=0, limit=None)
    assert old_items[0]["drift"] == []
    assert old_items[0]["evidence"]["current_highest"]["revision"] == 1

    new_items, new_snapshot = reconcile_snapshot(
        db, ReconcileRequest(receipt_ids=["R1"])
    )
    assert new_items[0]["evidence"]["current_highest"]["revision"] == 2
    assert new_snapshot["wind_max_id"] > snapshot["wind_max_id"]


def test_report_items_paginate_stably(db):
    import_receipts(
        db,
        [
            {
                "receipt_id": f"R{i:02d}",
                "turbine_id": "WT",
                "score_minute": "2026-01-01T11:00:00Z",
                "used_sample_minute": "2026-01-01T10:00:00Z",
                "used_revision": 1,
            }
            for i in range(25)
        ],
    )
    items, snapshot = reconcile_snapshot(db, ReconcileRequest(turbine_id="WT"))
    persist_report(db, items, snapshot, {}, "rpt_pages")
    page1, total = get_report_items(db, "rpt_pages", offset=0, limit=10)
    page2, _ = get_report_items(db, "rpt_pages", offset=10, limit=10)
    page3, _ = get_report_items(db, "rpt_pages", offset=20, limit=10)
    assert total == 25
    assert [len(page1), len(page2), len(page3)] == [10, 10, 5]
    assert len({i["receipt_id"] for i in page1 + page2 + page3}) == 25
