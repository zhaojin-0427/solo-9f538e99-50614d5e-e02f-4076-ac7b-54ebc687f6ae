"""Tests for the desensitised replay-bundle export (POST /api/export/replay-bundle)."""

from __future__ import annotations

import json

READINGS = [
    {
        "turbine_id": "WT-001",
        "sample_minute": "2026-01-01T10:00:00Z",
        "revision": 1,
        "ingested_at": "2026-01-01T10:01:00Z",
        "wind_speed": 6.0,
    },
    {
        "turbine_id": "WT-001",
        "sample_minute": "2026-01-01T10:00:00Z",
        "revision": 2,
        "ingested_at": "2026-01-01T10:02:00Z",
        "wind_speed": 6.4,
    },
]

RECEIPTS = [
    {
        "receipt_id": "R-1",
        "turbine_id": "WT-001",
        "score_minute": "2026-01-01T11:00:00Z",
        "used_sample_minute": "2026-01-01T10:00:00Z",
        "used_revision": 1,
    },
    {
        "receipt_id": "R-2",
        "turbine_id": "WT-001",
        "score_minute": "2026-01-01T11:00:00Z",
        "used_sample_minute": "2026-01-01T10:00:00Z",
        "used_revision": 2,
    },
]


def seed(client, readings=None, receipts=None):
    assert client.post("/api/readings", json=readings or READINGS).status_code == 200
    assert client.post("/api/receipts", json=receipts or RECEIPTS).status_code == 200


def reconcile(client, receipt_ids=None):
    body = {"receipt_ids": receipt_ids} if receipt_ids else {}
    response = client.post("/api/reconcile", json=body)
    assert response.status_code == 200, response.text
    return response.json()["report_id"]


def export(client, report_ids, receipt_ids=None):
    payload = {"report_ids": report_ids}
    if receipt_ids is not None:
        payload["receipt_ids"] = receipt_ids
    return client.post("/api/export/replay-bundle", json=payload)


def two_reports(client):
    seed(client)
    first = reconcile(client)
    second = reconcile(client)
    return first, second


def test_export_requires_two_different_reports(client):
    first, _ = two_reports(client)
    assert export(client, [first, first]).status_code == 422
    assert export(client, [first]).status_code == 422
    assert export(client, [first, "rpt_other", "rpt_third"]).status_code == 422


def test_export_unknown_report_is_404(client):
    first, _ = two_reports(client)
    response = export(client, [first, "rpt_missing"])
    assert response.status_code == 404


def test_export_receipt_must_be_shared(client):
    seed(client)
    first = reconcile(client, ["R-1"])
    second = reconcile(client, ["R-2"])
    response = export(client, [first, second], ["R-1"])
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["not_shared"][0]["receipt_id"] == "R-1"
    assert detail["not_shared"][0]["missing_in"] == [second]


def test_export_without_shared_receipts_is_422(client):
    seed(client)
    first = reconcile(client, ["R-1"])
    second = reconcile(client, ["R-2"])
    response = export(client, [first, second])
    assert response.status_code == 422


def test_export_rejects_empty_receipt_list(client):
    first, second = two_reports(client)
    assert export(client, [first, second], []).status_code == 422


def test_export_caps_receipts_at_100(client):
    first, second = two_reports(client)
    response = export(client, [first, second], [f"R-{i}" for i in range(101)])
    assert response.status_code == 413
    assert response.json()["detail"]["max_receipts"] == 100


def test_export_implicit_intersection_over_100_is_413(client):
    receipts = [
        {
            "receipt_id": f"R-{index:03d}",
            "turbine_id": "WT-001",
            "score_minute": "2026-01-01T11:00:00Z",
            "used_sample_minute": "2026-01-01T10:00:00Z",
            "used_revision": 1,
        }
        for index in range(101)
    ]
    seed(client, receipts=receipts)
    first = reconcile(client)
    second = reconcile(client)
    response = export(client, [first, second])
    assert response.status_code == 413
    assert response.json()["detail"]["shared"] == 101
    # Explicitly narrowing to 100 works.
    ok = export(client, [first, second], [f"R-{i:03d}" for i in range(100)])
    assert ok.status_code == 200, ok.text
    assert len(ok.json()["receipts"]) == 100


def test_export_download_headers(client):
    first, second = two_reports(client)
    response = export(client, [first, second])
    assert response.status_code == 200
    assert "attachment" in response.headers["content-disposition"]
    assert response.headers["content-type"].startswith("application/json")


def _walk_keys(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _walk_keys(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_keys(item)


def test_bundle_is_desensitised(client):
    first, second = two_reports(client)
    bundle = export(client, [first, second]).json()
    text = json.dumps(bundle, ensure_ascii=False)
    # No real identifiers anywhere.
    assert "WT-001" not in text
    assert "R-1" not in text and "R-2" not in text
    assert first not in text and second not in text
    # No absolute timestamps.
    assert "2026-01-01" not in text
    # No wind speed values.
    assert "6.0" not in text and "6.4" not in text
    # No sensitive field names either.
    keys = set(_walk_keys(bundle))
    assert "wind_speed" not in keys
    assert "ingested_at" not in keys
    assert "receipt_id" not in keys
    assert "turbine_id" not in keys
    assert "report_id" not in keys
    # Codes and offsets are used instead.
    assert {report["code"] for report in bundle["reports"]} == {"R1", "R2"}
    receipt = bundle["receipts"][0]
    assert receipt["code"].startswith("RC")
    entry = receipt["reports"]["R1"]
    assert isinstance(entry["score_minute_offset"], int)
    assert isinstance(entry["used_sample_minute_offset"], int)
    pooled = bundle["revision_pool"][0]
    assert isinstance(pooled["ingested_offset_us"], int)
    assert isinstance(pooled["wind_missing"], bool)


def test_identical_desensitised_revisions_are_stored_once(client):
    first, second = two_reports(client)
    bundle = export(client, [first, second]).json()
    # Both reports saw the same two revisions: pooled once, shared by both tracks.
    assert len(bundle["revision_pool"]) == 2
    assert len(bundle["tracks"]) == 2
    first_track, second_track = bundle["tracks"]
    assert first_track["report"] == "R1" and second_track["report"] == "R2"
    assert first_track["revisions"] == second_track["revisions"]
    # Each report still has its own track (its own view of the minute).
    assert first_track["id"] != second_track["id"]


def test_rows_missing_before_backfill_stay_missing(client):
    seed(client)
    first = reconcile(client)
    # Back-fill a higher revision *after* the first report was stored.
    assert client.post(
        "/api/readings",
        json=[
            {
                "turbine_id": "WT-001",
                "sample_minute": "2026-01-01T10:00:00Z",
                "revision": 9,
                "ingested_at": "2026-01-01T11:20:00Z",
                "wind_speed": 7.7,
            }
        ],
    ).status_code == 200
    second = reconcile(client)
    bundle = export(client, [first, second]).json()
    tracks = {track["report"]: track for track in bundle["tracks"]}
    pool = {entry["id"]: entry for entry in bundle["revision_pool"]}
    first_revisions = {pool[pid]["revision"] for pid in tracks["R1"]["revisions"]}
    second_revisions = {pool[pid]["revision"] for pid in tracks["R2"]["revisions"]}
    assert first_revisions == {1, 2}  # rev 9 did not exist for the first report
    assert second_revisions == {1, 2, 9}


def test_export_uses_only_stored_report_data(client):
    first, second = two_reports(client)
    bundle_before = export(client, [first, second]).json()
    # Mutate the live warehouse after the reports were stored: the export
    # must not change, because it only reads the stored report items.
    assert client.post(
        "/api/readings",
        json=[
            {
                "turbine_id": "WT-001",
                "sample_minute": "2026-01-01T10:00:00Z",
                "revision": 2,
                "ingested_at": "2026-01-01T15:00:00Z",
                "wind_speed": 9.9,
            }
        ],
    ).status_code == 200
    bundle_after = export(client, [first, second]).json()
    assert bundle_before == bundle_after


def test_receipt_corrected_between_reports_keeps_both_versions(client):
    seed(client)
    first = reconcile(client, ["R-1"])
    # The receipt itself is corrected (re-imported) between the two reports.
    assert client.post(
        "/api/receipts",
        json=[
            {
                "receipt_id": "R-1",
                "turbine_id": "WT-001",
                "score_minute": "2026-01-01T11:00:00Z",
                "used_sample_minute": "2026-01-01T10:00:00Z",
                "used_revision": 2,
            }
        ],
    ).status_code == 200
    second = reconcile(client, ["R-1"])
    bundle = export(client, [first, second], ["R-1"]).json()
    receipt = bundle["receipts"][0]
    assert receipt["reports"]["R1"]["used_revision"] == 1
    assert receipt["reports"]["R2"]["used_revision"] == 2


def test_offsets_share_one_origin(client):
    first, second = two_reports(client)
    bundle = export(client, [first, second]).json()
    offsets = [entry["ingested_offset_us"] for entry in bundle["revision_pool"]]
    minute_offsets = [
        entry["used_sample_minute_offset"]
        for receipt in bundle["receipts"]
        for entry in receipt["reports"].values()
    ] + [
        entry["score_minute_offset"]
        for receipt in bundle["receipts"]
        for entry in receipt["reports"].values()
    ]
    # The earliest timestamp of the package sits exactly at the shared origin.
    assert min(offsets + [m * 60_000_000 for m in minute_offsets]) == 0
    # Score minute is one hour after the used sample minute in this fixture.
    receipt = bundle["receipts"][0]
    entry = receipt["reports"]["R1"]
    assert entry["score_minute_offset"] - entry["used_sample_minute_offset"] == 60


def test_wind_null_marker_is_exported(client):
    readings = [
        {
            "turbine_id": "WT-001",
            "sample_minute": "2026-01-01T10:00:00Z",
            "revision": 1,
            "ingested_at": "2026-01-01T10:01:00Z",
            "wind_speed": None,
        }
    ]
    seed(client, readings=readings, receipts=RECEIPTS[:1])
    first = reconcile(client, ["R-1"])
    second = reconcile(client, ["R-1"])
    bundle = export(client, [first, second], ["R-1"]).json()
    assert bundle["revision_pool"][0]["wind_missing"] is True
