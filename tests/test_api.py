"""HTTP-level tests for the reconciliation API."""

from __future__ import annotations

import json

import pytest

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
    {
        "turbine_id": "WT-001",
        "sample_minute": "2026-01-01T10:00:00Z",
        "revision": 5,
        "ingested_at": "2026-01-01T11:20:00Z",
        "wind_speed": 7.1,
    },
    {
        "turbine_id": "WT-001",
        "sample_minute": "2026-01-01T10:02:00Z",
        "revision": 1,
        "ingested_at": "2026-01-01T10:03:00Z",
        "wind_speed": 5.5,
    },
]

RECEIPTS = [
    {
        "receipt_id": "R-OK",
        "turbine_id": "WT-001",
        "score_minute": "2026-01-01T11:00:00Z",
        "used_sample_minute": "2026-01-01T10:00:00Z",
        "used_revision": 2,
    },
    {
        "receipt_id": "R-WRONG-REV",
        "turbine_id": "WT-001",
        "score_minute": "2026-01-01T11:00:00Z",
        "used_sample_minute": "2026-01-01T10:00:00Z",
        "used_revision": 1,
    },
    {
        "receipt_id": "R-MISSING",
        "turbine_id": "WT-001",
        "score_minute": "2026-01-01T11:00:00Z",
        "used_sample_minute": "2026-01-01T10:02:00Z",
        "used_revision": 3,
    },
]


def seed(client):
    assert client.post("/api/readings", json=READINGS).status_code == 200
    assert client.post("/api/receipts", json=RECEIPTS).status_code == 200


def test_docs_and_openapi_available(client):
    assert client.get("/docs").status_code == 200
    schema = client.get("/openapi.json").json()
    assert schema["info"]["title"]
    assert "/api/reconcile" in schema["paths"]
    assert "/api/reports/{report_id}" in schema["paths"]


def test_health(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"


def test_import_and_list_data(client):
    seed(client)
    readings = client.get("/api/readings").json()
    assert readings["total"] == 4
    receipts = client.get("/api/receipts").json()
    assert receipts["total"] == 3


def test_import_rejects_bad_rows_with_row_numbers(client):
    response = client.post(
        "/api/readings",
        json=[{"turbine_id": "WT", "sample_minute": "not-a-time", "revision": -1,
               "ingested_at": "2026-01-01T00:00:00Z", "wind_speed": 1}],
    )
    assert response.status_code == 422
    payload = response.json()["detail"]
    assert payload["errors"][0]["row"] == 0
    fields = {err["field"] for err in payload["errors"][0]["errors"]}
    assert fields  # at least one field error reported


def test_import_failure_stores_nothing(client):
    bad = [
        dict(READINGS[0]),
        {"turbine_id": "WT", "sample_minute": "bad", "revision": 1,
         "ingested_at": "2026-01-01T00:00:00Z", "wind_speed": 1},
    ]
    assert client.post("/api/readings", json=bad).status_code == 422
    assert client.get("/api/readings").json()["total"] == 0


def test_csv_upload(client):
    csv_text = (
        "机组,采样分钟,修订号,生产入仓时刻,风速\n"
        "WT-001,2026-01-01T10:00:00Z,1,2026-01-01T10:01:00Z,6.0\n"
        "WT-001,2026-01-01T10:00:00Z,2,2026-01-01T10:02:00Z,6.4\n"
    )
    response = client.post(
        "/api/readings/import",
        files={"file": ("readings.csv", csv_text.encode("utf-8"), "text/csv")},
    )
    assert response.status_code == 200, response.text
    assert response.json()["accepted"] == 2

    receipts_csv = (
        "编号,机组,打分分钟,实际使用的采样分钟,实际使用修订号\n"
        "R1,WT-001,2026-01-01T11:00:00Z,2026-01-01T10:00:00Z,2\n"
    )
    response = client.post(
        "/api/receipts/import",
        files={"file": ("r.csv", receipts_csv.encode("gb18030"), "text/csv")},
    )
    assert response.status_code == 200, response.text
    assert response.json()["accepted"] == 1


def test_json_file_upload(client):
    body = json.dumps({"items": READINGS}).encode()
    response = client.post(
        "/api/readings/import",
        files={"file": ("readings.json", body, "application/json")},
    )
    assert response.status_code == 200, response.text
    assert response.json()["accepted"] == 4


def test_reconcile_produces_two_lists_and_persists_report(client):
    seed(client)
    response = client.post("/api/reconcile", json={"receipt_ids": ["R-WRONG-REV"]})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "complete"
    assert body["receipt_count"] == 1
    item = body["items"][0]
    assert [f["kind"] for f in item["usage"]] == ["revision"]
    assert [f["kind"] for f in item["drift"]] == ["revision"]
    assert item["evidence"]["should_use_at_score"]["revision"] == 2
    assert item["evidence"]["current_highest"]["revision"] == 5
    report_id = body["report_id"]

    fetched = client.get(f"/api/reports/{report_id}")
    assert fetched.status_code == 200
    stored = fetched.json()
    assert stored["items"][0]["usage"] == item["usage"]
    assert stored["items"][0]["drift"] == item["drift"]
    assert stored["items"][0]["evidence"]["rule"]
    assert stored["snapshot"]["mode"] == "single_committed_snapshot"


def test_report_summary_counts_and_listing(client):
    seed(client)
    body = client.post("/api/reconcile", json={}).json()
    assert body["receipt_count"] == 3
    # R-OK used the right revision (usage clean) but rev 5 arrived later, so
    # it still has a drift finding; R-WRONG-REV has both; R-MISSING's drift is
    # clean because the revision that should have been used is still the
    # current highest.
    assert body["usage_finding_count"] == 2
    assert body["drift_finding_count"] == 2
    assert body["items_with_findings"] == 3

    listing = client.get("/api/reports").json()
    assert listing["total"] == 1
    assert listing["reports"][0]["report_id"] == body["report_id"]


def test_reports_are_queryable_by_receipt_id(client):
    seed(client)
    report_id = client.post("/api/reconcile", json={}).json()["report_id"]
    response = client.get("/api/receipts/R-MISSING/reports")
    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert response.json()["reports"][0]["report_id"] == report_id

    filtered = client.get(f"/api/reports/{report_id}", params={"receipt_id": "R-OK"})
    assert filtered.status_code == 200
    assert filtered.json()["item_total"] == 1


def test_report_not_found(client):
    assert client.get("/api/reports/rpt_missing").status_code == 404


def test_unknown_receipt_id_returns_404(client):
    seed(client)
    response = client.post("/api/reconcile", json={"receipt_ids": ["R-OK", "R-NOPE"]})
    assert response.status_code == 404
    assert response.json()["detail"]["unknown_receipt_ids"] == ["R-NOPE"]
    # No report may be left behind by the failed request.
    assert client.get("/api/reports").json()["total"] == 0


def test_batch_limit_rejects_oversized_request(client):
    seed(client)
    response = client.post(
        "/api/reconcile",
        json={"receipt_ids": [f"R{i}" for i in range(10_001)]},
    )
    assert response.status_code == 413
    assert response.json()["detail"]["max_batch_size"] == 10_000


def test_filters_select_receipts(client):
    seed(client)
    body = client.post(
        "/api/reconcile",
        json={
            "turbine_id": "WT-001",
            "score_minute_from": "2026-01-01T11:00:00Z",
            "score_minute_to": "2026-01-01T11:00:00Z",
        },
    ).json()
    assert body["receipt_count"] == 3


def test_duplicate_imports_do_not_duplicate_rows(client):
    seed(client)
    seed(client)
    assert client.get("/api/readings").json()["total"] == 4
    assert client.get("/api/receipts").json()["total"] == 3


def test_concurrent_backfill_does_not_change_in_flight_report(client, monkeypatch):
    seed(client)
    from app import engine

    original = engine._evaluate

    def evaluate_then_backfill(receipts, pairs):
        client.post(
            "/api/readings",
            json=[
                {
                    "turbine_id": "WT-001",
                    "sample_minute": "2026-01-01T10:02:00Z",
                    "revision": 4,
                    "ingested_at": "2026-01-01T11:30:00Z",
                    "wind_speed": 9.9,
                }
            ],
        )
        return original(receipts, pairs)

    monkeypatch.setattr(engine, "_evaluate", evaluate_then_backfill)
    first = client.post("/api/reconcile", json={"receipt_ids": ["R-MISSING"]}).json()
    monkeypatch.setattr(engine, "_evaluate", original)

    # The running batch must not see the mid-flight back-fill.
    assert first["items"][0]["evidence"]["current_highest"]["revision"] == 1
    assert first["items"][0]["drift"] == []
    # A later batch does.
    second = client.post("/api/reconcile", json={"receipt_ids": ["R-MISSING"]}).json()
    assert second["items"][0]["evidence"]["current_highest"]["revision"] == 4
    assert [f["kind"] for f in second["items"][0]["drift"]] == ["revision"]
    # Both reports remain available by id, with their own frozen evidence.
    assert client.get(f"/api/reports/{first['report_id']}").json()["items"][0][
        "evidence"
    ]["current_highest"]["revision"] == 1
    assert client.get(f"/api/reports/{second['report_id']}").json()["items"][0][
        "evidence"
    ]["current_highest"]["revision"] == 4


def test_failed_persist_leaves_no_report(client, monkeypatch):
    seed(client)
    from app import main as main_module

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(main_module, "persist_report", boom)
    with pytest.raises(RuntimeError, match="disk full"):
        client.post("/api/reconcile", json={"receipt_ids": ["R-OK"]})
    assert client.get("/api/reports").json()["total"] == 0
