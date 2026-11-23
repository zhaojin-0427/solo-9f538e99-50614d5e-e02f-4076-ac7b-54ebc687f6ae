"""Tests for the offline replay service (app.replay_main)."""

from __future__ import annotations

import copy
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
]

RECEIPT = {
    "receipt_id": "R-X",
    "turbine_id": "WT-001",
    "score_minute": "2026-01-01T11:00:00Z",
    "used_sample_minute": "2026-01-01T10:00:00Z",
    "used_revision": 1,
}


def _reconcile(client, receipt_ids):
    response = client.post("/api/reconcile", json={"receipt_ids": receipt_ids})
    assert response.status_code == 200, response.text
    return response.json()


def correction_bundle(client):
    """Two reports of the same receipt around a same-number correction.

    Report A: rev 2 was ingested before scoring -> the receipt should have
    used rev 2 (usage=revision).  Then rev 2 is re-imported with the same
    number but a corrected, later ingested_at (after scoring).  Report B:
    rev 2 no longer counts at score time -> usage clean, drift=revision.
    """
    assert client.post("/api/readings", json=READINGS).status_code == 200
    assert client.post("/api/receipts", json=[RECEIPT]).status_code == 200
    report_a = _reconcile(client, ["R-X"])
    assert client.post(
        "/api/readings",
        json=[
            {
                "turbine_id": "WT-001",
                "sample_minute": "2026-01-01T10:00:00Z",
                "revision": 2,
                "ingested_at": "2026-01-01T11:30:00Z",
                "wind_speed": 6.4,
            }
        ],
    ).status_code == 200
    report_b = _reconcile(client, ["R-X"])
    response = client.post(
        "/api/export/replay-bundle",
        json={
            "report_ids": [report_a["report_id"], report_b["report_id"]],
            "receipt_ids": ["R-X"],
        },
    )
    assert response.status_code == 200, response.text
    return response.json(), report_a, report_b


def test_same_number_correction_replays_offline(client, replay_client):
    bundle, report_a, report_b = correction_bundle(client)
    # Sanity: the two stored reports really disagree about the same receipt.
    item_a = client.get(f"/api/reports/{report_a['report_id']}").json()["items"][0]
    item_b = client.get(f"/api/reports/{report_b['report_id']}").json()["items"][0]
    assert [f["kind"] for f in item_a["usage"]] == ["revision"]
    assert item_b["usage"] == []

    response = replay_client.post("/api/replay", content=json.dumps(bundle))
    assert response.status_code == 200, response.text
    result = response.json()

    assert result["receipt_count"] == 1
    entry = result["results"][0]
    assert set(entry["reports"]) == {"R1", "R2"}  # both reports delivered

    first, second = entry["reports"]["R1"], entry["reports"]["R2"]
    # The replay reproduces each stored report exactly (integrity ok).
    assert first["stored_vs_replay"] == []
    assert second["stored_vs_replay"] == []
    assert result["summary"]["integrity"] == "ok"
    assert result["summary"]["stored_vs_replay_mismatches"] == 0

    # Two finding lists, types listed.
    assert [f["kind"] for f in first["usage"]] == ["revision"]
    assert first["drift"] == []
    assert second["usage"] == []
    assert [f["kind"] for f in second["drift"]] == ["revision"]

    # Three sides per report.
    assert first["three_sides"] == {"actual": 1, "should_use": 2, "current": 2}
    assert second["three_sides"] == {"actual": 1, "should_use": 1, "current": 2}

    # Per-revision ingest/selection markers, and how they changed.
    markers_first = {m["revision"]: m for m in first["revision_markers"]}
    markers_second = {m["revision"]: m for m in second["revision_markers"]}
    assert markers_first[2]["ingested_before_score"] is True
    assert markers_first[2]["eligible"] is True
    assert markers_second[2]["ingested_before_score"] is False
    assert markers_second[2]["eligible"] is False

    changes = entry["report_changes"]
    assert changes["changed"] is True
    assert changes["usage"]["before"][0]["kind"] == "revision"
    assert changes["usage"]["after"] == []
    assert changes["drift"]["before"] == []
    assert changes["drift"]["after"][0]["kind"] == "revision"
    assert changes["three_sides"]["should_use"] == {"before": 2, "after": 1}
    (rev_change,) = [
        c for c in changes["revision_changes"] if c["revision"] == 2
    ]
    assert rev_change["change"] == "reingested"
    assert rev_change["before"]["eligible"] is True
    assert rev_change["after"]["eligible"] is False
    assert (
        rev_change["after"]["ingested_offset_us"]
        > rev_change["before"]["ingested_offset_us"]
    )
    assert result["summary"]["receipts_changed_between_reports"] == 1


def test_replay_via_file_upload(client, replay_client):
    bundle, _, _ = correction_bundle(client)
    body = json.dumps(bundle).encode("utf-8")
    response = replay_client.post(
        "/api/replay/import",
        files={"file": ("bundle.json", body, "application/json")},
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["summary"]["integrity"] == "ok"
    assert result["receipt_count"] == 1


def test_replay_service_has_no_database(replay_client):
    assert replay_client.get("/health").json()["status"] == "ok"
    root = replay_client.get("/").json()
    assert root["database"] is None
    assert root["detached"] is True


def test_replay_rejects_non_json(replay_client):
    response = replay_client.post("/api/replay", content=b"not json{")
    assert response.status_code == 422


def test_replay_rejects_wrong_format_and_version(client, replay_client):
    bundle, _, _ = correction_bundle(client)
    bad = copy.deepcopy(bundle)
    bad["format"] = "something-else"
    assert replay_client.post("/api/replay", json=bad).status_code == 422
    bad = copy.deepcopy(bundle)
    bad["format_version"] = 99
    assert replay_client.post("/api/replay", json=bad).status_code == 422


def test_replay_rejects_foreign_window(client, replay_client):
    bundle, _, _ = correction_bundle(client)
    bad = copy.deepcopy(bundle)
    bad["window"]["hours"] = 2
    response = replay_client.post("/api/replay", json=bad)
    assert response.status_code == 422


def _assert_error_only(response, status):
    assert response.status_code == status
    payload = response.json()
    assert set(payload) == {"detail"}
    assert "results" not in payload


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b["receipts"][0]["reports"]["R1"].update({"track": "K999"}),
        lambda b: b["tracks"][0].update({"revisions": ["V999"]}),
        lambda b: b["receipts"][0]["reports"]["R1"].update({"turbine": "T999"}),
        lambda b: b["receipts"][0]["reports"].pop("R2"),
        lambda b: b["tracks"][0].update({"report": "R2"}),
        lambda b: b["revision_pool"][0].update({"turbine": "T999"}),
    ],
    ids=[
        "missing-track",
        "missing-pool-entry",
        "missing-turbine",
        "missing-report-entry",
        "track-of-other-report",
        "pool-turbine-unknown",
    ],
)
def test_replay_dangling_reference_is_error_only(client, replay_client, mutate):
    bundle, _, _ = correction_bundle(client)
    bad = copy.deepcopy(bundle)
    mutate(bad)
    response = replay_client.post("/api/replay", json=bad)
    _assert_error_only(response, 422)


def test_replay_compute_failure_is_error_only(client, replay_client, monkeypatch):
    bundle, _, _ = correction_bundle(client)
    from app import replay as replay_module

    def boom(*args, **kwargs):
        raise RuntimeError("engine blew up")

    monkeypatch.setattr(replay_module, "evaluate_receipts", boom)
    response = replay_client.post("/api/replay", json=bundle)
    _assert_error_only(response, 500)
    assert "重算失败" in response.json()["detail"]


def test_replay_reports_stored_mismatch(client, replay_client):
    """A tampered stored conclusion is flagged in stored_vs_replay."""
    bundle, _, _ = correction_bundle(client)
    tampered = copy.deepcopy(bundle)
    tampered["receipts"][0]["reports"]["R1"]["stored"]["three_sides"]["current"] = 99
    response = replay_client.post("/api/replay", json=tampered)
    assert response.status_code == 200, response.text
    result = response.json()
    entry = result["results"][0]["reports"]["R1"]
    aspects = [change["aspect"] for change in entry["stored_vs_replay"]]
    assert aspects == ["three_sides"]
    assert result["summary"]["integrity"] == "mismatch"
    assert result["summary"]["stored_vs_replay_mismatches"] == 1


def test_replay_added_and_removed_revisions(client, replay_client):
    """Back-filled rows appear as 'added' between the two reports."""
    assert client.post("/api/readings", json=READINGS[:1]).status_code == 200
    assert client.post("/api/receipts", json=[RECEIPT]).status_code == 200
    report_a = client.post("/api/reconcile", json={"receipt_ids": ["R-X"]}).json()
    assert client.post("/api/readings", json=READINGS[1:]).status_code == 200
    report_b = client.post("/api/reconcile", json={"receipt_ids": ["R-X"]}).json()
    bundle = client.post(
        "/api/export/replay-bundle",
        json={
            "report_ids": [report_a["report_id"], report_b["report_id"]],
            "receipt_ids": ["R-X"],
        },
    ).json()
    result = replay_client.post("/api/replay", json=bundle).json()
    entry = result["results"][0]
    first = entry["reports"]["R1"]
    second = entry["reports"]["R2"]
    assert {m["revision"] for m in first["revision_markers"]} == {1}
    assert {m["revision"] for m in second["revision_markers"]} == {1, 2}
    changes = entry["report_changes"]["revision_changes"]
    assert changes == [
        {
            "revision": 2,
            "change": "added",
            "after": {
                "revision": 2,
                "ingested_offset_us": changes[0]["after"]["ingested_offset_us"],
                "ingested_before_score": True,
                "eligible": True,
            },
        }
    ]
    assert entry["report_changes"]["changed"] is True
