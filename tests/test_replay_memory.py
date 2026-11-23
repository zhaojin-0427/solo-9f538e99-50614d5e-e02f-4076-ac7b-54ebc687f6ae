"""Stress test for the 128 MiB replay-container requirement.

100 shared receipts all point at the same sampling minute, and each of the
two reports holds 4,000 revisions of that minute.  The replay must still
return the complete result while its own memory stays bounded.

The expensive fixture (two 100-receipt reports over 4,000 revisions and the
exported bundle) is built once per module and shared by the tests.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.replay_main import create_replay_app

REVISIONS = 4_000
RECEIPT_COUNT = 100
MIB = 1024 * 1024


def _readings(ingested_hour: int) -> list[dict]:
    return [
        {
            "turbine_id": "WT-001",
            "sample_minute": "2026-01-01T10:00:00Z",
            "revision": revision,
            "ingested_at": f"2026-01-01T{ingested_hour:02d}:{revision % 60:02d}:00Z",
            "wind_speed": 5.0,
        }
        for revision in range(1, REVISIONS + 1)
    ]


def _receipts() -> list[dict]:
    return [
        {
            "receipt_id": f"R-{index:03d}",
            "turbine_id": "WT-001",
            "score_minute": "2026-01-01T11:00:00Z",
            "used_sample_minute": "2026-01-01T10:00:00Z",
            "used_revision": 1,
        }
        for index in range(RECEIPT_COUNT)
    ]


@pytest.fixture(scope="module")
def stress_bundle(tmp_path_factory):
    """Two reports over one minute; every revision re-ingested (same numbers)."""
    db_path = str(tmp_path_factory.mktemp("stress") / "windscore.db")
    with TestClient(create_app(db_path)) as client:
        assert client.post("/api/readings", json=_readings(10)).status_code == 200
        assert client.post("/api/receipts", json=_receipts()).status_code == 200
        first = client.post("/api/reconcile", json={}).json()
        assert first["receipt_count"] == RECEIPT_COUNT
        # Same-number corrections: all 4,000 revisions re-ingested after scoring.
        assert client.post("/api/readings", json=_readings(13)).status_code == 200
        second = client.post("/api/reconcile", json={}).json()
        assert second["receipt_count"] == RECEIPT_COUNT
        response = client.post(
            "/api/export/replay-bundle",
            json={"report_ids": [first["report_id"], second["report_id"]]},
        )
        assert response.status_code == 200, response.text
        return response.json()


@pytest.fixture(scope="module")
def stress_replay_client():
    with TestClient(create_replay_app()) as replay_client:
        yield replay_client


def test_stress_bundle_is_compact(stress_bundle):
    assert len(stress_bundle["receipts"]) == RECEIPT_COUNT
    # One track per report (all receipts share the minute); 4,000 revisions
    # each, all corrected -> 8,000 pooled entries, stored once.
    assert len(stress_bundle["tracks"]) == 2
    assert len(stress_bundle["revision_pool"]) == 2 * REVISIONS
    for track in stress_bundle["tracks"]:
        assert len(track["revisions"]) == REVISIONS


def test_stress_replay_returns_complete_result(stress_bundle, stress_replay_client):
    response = stress_replay_client.post("/api/replay", content=json.dumps(stress_bundle))
    assert response.status_code == 200, response.text
    result = response.json()

    assert result["receipt_count"] == RECEIPT_COUNT
    assert len(result["results"]) == RECEIPT_COUNT
    for entry in result["results"]:
        assert set(entry["reports"]) == {"R1", "R2"}  # both reports complete
        first = entry["reports"]["R1"]
        second = entry["reports"]["R2"]
        assert len(first["revision_markers"]) == REVISIONS
        assert len(second["revision_markers"]) == REVISIONS
        assert all(m["eligible"] for m in first["revision_markers"])
        assert not any(m["eligible"] for m in second["revision_markers"])
        assert first["stored_vs_replay"] == []
        assert second["stored_vs_replay"] == []
        assert [f["kind"] for f in first["usage"]] == ["revision"]
        assert [f["kind"] for f in second["usage"]] == ["missing"]
        assert first["three_sides"]["should_use"] == REVISIONS
        assert second["three_sides"]["should_use"] is None
        assert entry["report_changes"]["changed"] is True
    summary = result["summary"]
    assert summary["integrity"] == "ok"
    assert summary["receipts_changed_between_reports"] == RECEIPT_COUNT
    assert all(r["receipt_count"] == RECEIPT_COUNT for r in summary["reports"])


def test_stress_replay_under_128mib_rlimit(stress_bundle, tmp_path):
    """Replay computes and streams completely with RLIMIT_AS = 128 MiB.

    The child process runs the full stress replay under a hard 128 MiB
    address-space limit (interpreter + app imports included) and reports its
    own peak RSS (VmHWM — getrusage would inherit the parent's high-water
    mark across fork/exec).  Same budget the replay container gets via
    mem_limit.
    """
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(json.dumps(stress_bundle), encoding="utf-8")
    child = tmp_path / "replay_child.py"
    child.write_text(
        """
import json
import resource
import sys

resource.setrlimit(resource.RLIMIT_AS, (128 * 1024 * 1024, 128 * 1024 * 1024))
sys.path.insert(0, sys.argv[1])

from app.bundle import parse_replay_bundle
from app.replay import compute_replay, stream_replay_result

with open(sys.argv[2], encoding="utf-8") as handle:
    bundle = parse_replay_bundle(json.load(handle))
spool, summary, chunk_count = compute_replay(bundle)
streamed = 0
for part in stream_replay_result(bundle, spool, summary, chunk_count):
    streamed += len(part)
assert chunk_count == 100, chunk_count
assert summary["receipt_count"] == 100
assert summary["integrity"] == "ok"
assert all(r["receipt_count"] == 100 for r in summary["reports"])
peak_kb = 0
with open("/proc/self/status", encoding="ascii") as status:
    for line in status:
        if line.startswith("VmHWM:"):
            peak_kb = int(line.split()[1])
print(json.dumps({
    "ok": True,
    "chunks": chunk_count,
    "streamed_bytes": streamed,
    "peak_rss_bytes": peak_kb * 1024,
}))
""",
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            str(child),
            str(Path(__file__).resolve().parents[1]),
            str(bundle_path),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stderr
    stats = json.loads(completed.stdout.strip().splitlines()[-1])
    assert stats["ok"] is True
    assert stats["chunks"] == RECEIPT_COUNT
    assert stats["streamed_bytes"] > 0
    # Complete result computed and streamed inside the 128 MiB budget.
    assert stats["peak_rss_bytes"] < 128 * MIB
