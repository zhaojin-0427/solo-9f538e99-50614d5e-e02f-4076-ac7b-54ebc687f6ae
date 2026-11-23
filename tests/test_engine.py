"""Domain-level tests for the reconciliation rules (no HTTP)."""

from __future__ import annotations

import pytest

from app.engine import reconcile_snapshot
from app.schemas import ReconcileRequest
from app.storage import import_readings, import_receipts


def reading(turbine, minute, revision, ingested, wind=5.0):
    return {
        "turbine_id": turbine,
        "sample_minute": minute,
        "revision": revision,
        "ingested_at": ingested,
        "wind_speed": wind,
    }


def receipt(rid, turbine, score, sample, revision):
    return {
        "receipt_id": rid,
        "turbine_id": turbine,
        "score_minute": score,
        "used_sample_minute": sample,
        "used_revision": revision,
    }


def run(db, **kwargs):
    items, snapshot = reconcile_snapshot(db, ReconcileRequest(**kwargs))
    return {item["receipt_id"]: item for item in items}, snapshot


def kinds(findings):
    return [f["kind"] for f in findings]


def test_highest_revision_ingested_before_score_is_expected(db):
    import_readings(
        db,
        [
            reading("WT", "2026-01-01T10:00:00Z", 1, "2026-01-01T10:01:00Z"),
            reading("WT", "2026-01-01T10:00:00Z", 2, "2026-01-01T10:02:00Z"),
            # ingested after scoring: must be ignored for "当时应取"
            reading("WT", "2026-01-01T10:00:00Z", 3, "2026-01-01T11:05:00Z"),
        ],
    )
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 2)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    item = items["R1"]
    assert item["usage"] == []  # used the right revision
    assert kinds(item["drift"]) == ["revision"]  # but rev 3 arrived later
    evidence = item["evidence"]
    assert evidence["should_use_at_score"]["revision"] == 2
    assert evidence["current_highest"]["revision"] == 3
    trail = {row["revision"]: row for row in evidence["revisions"]}
    assert trail[3]["ingested_before_score"] is False
    assert trail[2]["ingested_before_score"] is True


def test_wrong_revision_used_is_a_usage_revision_finding(db):
    import_readings(
        db,
        [
            reading("WT", "2026-01-01T10:00:00Z", 1, "2026-01-01T10:01:00Z"),
            reading("WT", "2026-01-01T10:00:00Z", 2, "2026-01-01T10:02:00Z"),
        ],
    )
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 1)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    usage = items["R1"]["usage"]
    assert kinds(usage) == ["revision"]
    assert usage[0]["left"]["revision"] == 1
    assert usage[0]["right"]["revision"] == 2
    # Current highest == should-use, so no drift.
    assert items["R1"]["drift"] == []


def test_missing_reading_referenced_by_receipt(db):
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 9)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    item = items["R1"]
    assert kinds(item["usage"]) == ["missing"]
    assert item["usage"][0]["left"]["present"] is False
    assert item["usage"][0]["right"]["present"] is False
    assert item["drift"] == []


def test_reading_ingested_after_score_cannot_be_used(db):
    import_readings(
        db, [reading("WT", "2026-01-01T10:00:00Z", 1, "2026-01-01T11:30:00Z")]
    )
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 1)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    item = items["R1"]
    # Actual row exists but had not been ingested at score time -> usage missing.
    assert kinds(item["usage"]) == ["missing"]
    assert item["usage"][0]["left"]["present"] is True
    assert item["usage"][0]["right"]["present"] is False
    # Later back-fill shows up as drift (missing -> present).
    assert kinds(item["drift"]) == ["missing"]
    assert item["drift"][0]["right"]["present"] is True


def test_sample_outside_hour_window_is_flagged(db):
    import_readings(
        db, [reading("WT", "2026-01-01T09:30:00Z", 1, "2026-01-01T09:31:00Z")]
    )
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T09:30:00Z", 1)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    item = items["R1"]
    assert kinds(item["usage"]) == ["window"]
    assert item["evidence"]["window"]["sample_minute_in_window"] is False
    assert "窗口" in item["usage"][0]["detail"]
    # An out-of-window sample is missing by rule, so there is no drift to report.
    assert item["drift"] == []


def test_window_boundaries(db):
    """Window is [score-1h, score): start inclusive, end exclusive."""
    import_readings(
        db,
        [
            reading("WT", "2026-01-01T10:00:00Z", 1, "2026-01-01T10:00:30Z"),
            reading("WT", "2026-01-01T11:00:00Z", 1, "2026-01-01T11:00:30Z"),
        ],
    )
    import_receipts(
        db,
        [
            receipt("AT_START", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 1),
            receipt("AT_END", "WT", "2026-01-01T11:00:00Z", "2026-01-01T11:00:00Z", 1),
        ],
    )
    items, _ = run(db, receipt_ids=["AT_START", "AT_END"])
    assert items["AT_START"]["usage"] == []
    assert kinds(items["AT_END"]["usage"]) == ["window"]


def test_ingest_exactly_at_score_minute_is_not_before(db):
    import_readings(
        db, [reading("WT", "2026-01-01T10:00:00Z", 1, "2026-01-01T11:00:00Z")]
    )
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 1)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    assert kinds(items["R1"]["usage"]) == ["missing"]


def test_missing_measurement_keeps_null_wind_speed(db):
    import_readings(
        db,
        [
            {
                "turbine_id": "WT",
                "sample_minute": "2026-01-01T10:00:00Z",
                "revision": 1,
                "ingested_at": "2026-01-01T10:01:00Z",
                "wind_speed": None,
            }
        ],
    )
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 1)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    item = items["R1"]
    assert item["usage"] == []
    assert item["drift"] == []
    assert item["evidence"]["actual"]["wind_speed"] is None
    assert item["evidence"]["actual"]["present"] is True


def test_blank_wind_speed_string_is_null(db):
    import_readings(
        db,
        [
            {
                "turbine_id": "WT",
                "sample_minute": "2026-01-01T10:00:00Z",
                "修订号": 1,
                "生产入仓时刻": "2026-01-01T10:01:00Z",
                "风速": "",
            }
        ],
    )
    from app.storage import list_readings

    rows, _ = list_readings(db, turbine_id=None, offset=0, limit=10)
    assert rows[0]["wind_speed"] is None


def test_chinese_column_names_are_accepted(db):
    result = import_readings(
        db,
        [
            {
                "机组": "WT-9",
                "采样分钟": "2026-01-01T10:00:00Z",
                "修订号": 2,
                "生产入仓时刻": "2026-01-01T10:01:00Z",
                "风速": 6.5,
            }
        ],
    )
    assert result["accepted"] == 1
    result = import_receipts(
        db,
        [
            {
                "编号": "R-中文",
                "机组": "WT-9",
                "打分分钟": "2026-01-01T11:00:00Z",
                "实际使用的采样分钟": "2026-01-01T10:00:00Z",
                "实际使用修订号": 2,
            }
        ],
    )
    assert result["accepted"] == 1
    items, _ = run(db, receipt_ids=["R-中文"])
    assert items["R-中文"]["usage"] == []


def test_naive_and_offset_timestamps_normalise_to_utc(db):
    import_readings(
        db,
        [
            reading(
                "WT",
                "2026-01-01T18:00:00+08:00",
                1,
                "2026-01-01T18:02:00+08:00",
            )
        ],
    )
    import_receipts(
        db,
        [
            receipt(
                "R1",
                "WT",
                "2026-01-01T19:00:00+08:00",
                "2026-01-01T18:00:00+08:00",
                1,
            )
        ],
    )
    items, _ = run(db, receipt_ids=["R1"])
    assert items["R1"]["score_minute"] == "2026-01-01T11:00:00Z"
    assert items["R1"]["used_sample_minute"] == "2026-01-01T10:00:00Z"
    assert items["R1"]["usage"] == []


def test_sub_minute_sample_is_floored_to_minute(db):
    import_readings(
        db,
        [reading("WT", "2026-01-01T10:00:45Z", 1, "2026-01-01T10:00:50Z")],
    )
    from app.storage import list_readings

    stored, _ = list_readings(db, turbine_id=None, offset=0, limit=10)
    assert stored[0]["sample_minute"] == "2026-01-01T10:00:00Z"


def test_unknown_receipt_id_is_rejected(db):
    from app.domain import UnknownReceipts

    with pytest.raises(UnknownReceipts) as excinfo:
        run(db, receipt_ids=["nope", "also-nope"])
    assert excinfo.value.missing == ["nope", "also-nope"]


def test_batch_limit_is_enforced_for_filter_queries(db):
    from app.domain import BatchTooLarge
    from app.engine import MAX_BATCH_SIZE

    # Filter path counts before loading; simulate an oversized table cheaply.
    assert MAX_BATCH_SIZE == 10_000
    import_receipts(
        db,
        [
            receipt(
                f"R{i}",
                "WT",
                "2026-01-01T11:00:00Z",
                "2026-01-01T10:00:00Z",
                1,
            )
            for i in range(MAX_BATCH_SIZE + 1)
        ],
    )
    with pytest.raises(BatchTooLarge) as excinfo:
        run(db, turbine_id="WT")
    assert excinfo.value.count == MAX_BATCH_SIZE + 1
    with pytest.raises(BatchTooLarge):
        run(db, receipt_ids=[f"R{i}" for i in range(MAX_BATCH_SIZE + 1)])


def test_other_turbines_readings_do_not_leak(db):
    import_readings(
        db,
        [
            reading("WT-A", "2026-01-01T10:00:00Z", 7, "2026-01-01T10:01:00Z"),
            reading("WT-B", "2026-01-01T10:00:00Z", 1, "2026-01-01T10:02:00Z"),
        ],
    )
    import_receipts(
        db, [receipt("R1", "WT-B", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 1)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    item = items["R1"]
    assert item["usage"] == []
    assert item["evidence"]["current_highest"]["revision"] == 1


def test_revision_used_at_a_different_sample_minute_is_a_missing_row(db):
    import_readings(
        db,
        [
            reading("WT", "2026-01-01T10:00:00Z", 1, "2026-01-01T10:01:00Z"),
            reading("WT", "2026-01-01T10:05:00Z", 2, "2026-01-01T10:06:00Z"),
        ],
    )
    # Receipt claims it used rev 2 at 10:00, but rev 2 lives at 10:05.
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 2)]
    )
    items, _ = run(db, receipt_ids=["R1"])
    item = items["R1"]
    assert kinds(item["usage"]) == ["missing"]
    assert item["usage"][0]["left"]["present"] is False
    assert item["usage"][0]["right"]["revision"] == 1


def test_one_query_for_all_receipts_regardless_of_batch_size(db):
    readings = [
        reading(
            "WT",
            f"2026-01-01T10:{minute:02d}:00Z",
            1,
            f"2026-01-01T10:{minute:02d}:30Z",
        )
        for minute in range(60)
    ]
    import_readings(db, readings)
    import_receipts(
        db,
        [
            receipt(
                f"R{minute}",
                "WT",
                "2026-01-01T11:00:00Z",
                f"2026-01-01T10:{minute:02d}:00Z",
                1,
            )
            for minute in range(60)
        ],
    )
    db.select_count = 0
    items, _ = run(db, turbine_id="WT")
    assert len(items) == 60
    # begin/commit/select receipts/select max id/paired join == a constant.
    assert db.select_count <= 6, db.select_count


def test_snapshot_is_stable_when_backfill_happens_during_batch(db, monkeypatch):
    """Writes committed mid-batch must not leak into the running snapshot."""
    import_readings(
        db, [reading("WT", "2026-01-01T10:00:00Z", 1, "2026-01-01T10:01:00Z")]
    )
    import_receipts(
        db, [receipt("R1", "WT", "2026-01-01T11:00:00Z", "2026-01-01T10:00:00Z", 1)]
    )
    from app import engine

    original = engine._evaluate

    def evaluate_and_backfill(receipts, pairs):
        import_readings(
            db,
            [reading("WT", "2026-01-01T10:00:00Z", 2, "2026-01-01T11:30:00Z")],
        )
        return original(receipts, pairs)

    monkeypatch.setattr(engine, "_evaluate", evaluate_and_backfill)
    items, _ = engine.reconcile_snapshot(db, ReconcileRequest(receipt_ids=["R1"]))
    # rev 2 was written after the snapshot opened: no drift may be reported.
    assert items[0]["drift"] == []
    # A later batch does see it.
    items2, _ = engine.reconcile_snapshot(db, ReconcileRequest(receipt_ids=["R1"]))
    assert kinds(items2[0]["drift"]) == ["revision"]
