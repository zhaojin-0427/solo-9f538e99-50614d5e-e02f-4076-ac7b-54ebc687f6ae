"""Offline replay of a desensitised replay bundle.

The replayer is detached from the production database: it rebuilds synthetic
receipts and warehouse rows from the bundle's offsets and re-runs the
**same** evaluation code (:func:`app.engine.evaluate_receipts`) that produced
the stored reports, receipt by receipt.

Memory stays bounded no matter how large a track is: results are computed
one receipt at a time and spooled to a temporary file (memory buffer first,
disk past ``SPOOL_MEM_BYTES``); the HTTP response is then streamed from the
spool.  Everything is computed before the response starts, so a failure
anywhere means the request returns only an error — never a partial result.
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone
from typing import IO, Any, Iterator

from .bundle import (
    BUNDLE_FORMAT,
    BUNDLE_VERSION,
    ReceiptEntry,
    ReplayBundle,
    StoredView,
)
from .domain import Receipt, format_instant, format_minute
from .engine import PairData, Row, evaluate_receipts

RESULT_FORMAT = "windscore-replay-result"

#: Keep at most this much of the spooled result in RAM; the rest goes to a
#: temp file.  Well below the 128 MiB replay-container budget together with
#: the interpreter and the parsed bundle.
SPOOL_MEM_BYTES = 8 * 1024 * 1024

#: Synthetic epoch the bundle offsets are mapped onto.  Any fixed instant
#: works: only relative distances matter for the reconciliation rules.
SYNTH_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


class ReplayComputationError(RuntimeError):
    """Re-running a receipt from the bundle failed."""


def _syn_minute(offset_minutes: int) -> str:
    return format_minute(SYNTH_EPOCH + timedelta(minutes=offset_minutes))


def _syn_instant(offset_us: int) -> str:
    return format_instant(SYNTH_EPOCH + timedelta(microseconds=offset_us))


def _finding_view(finding: dict[str, Any]) -> dict[str, Any]:
    left = finding.get("left") or {}
    right = finding.get("right") or {}
    return {
        "kind": finding.get("kind"),
        "left_revision": left.get("revision"),
        "right_revision": right.get("revision"),
    }


def _track_pair(
    bundle: ReplayBundle,
    track_id: str,
    cache: dict[str, tuple[PairData, dict[int, int]]],
) -> tuple[PairData, dict[int, int]]:
    """Synthetic warehouse rows for one track, built once and shared.

    Receipts of a report that point at the same sample minute share the
    track, so the (possibly thousands of) rows are rebuilt only once per
    track, not once per receipt.
    """
    cached = cache.get(track_id)
    if cached is not None:
        return cached
    track = bundle.tracks[track_id]
    rows = []
    for pid in track.revision_ids:
        pooled = bundle.pool[pid]
        rows.append(
            Row(
                revision=pooled.revision,
                ingested_at=_syn_instant(pooled.ingested_offset_us),
                # The bundle keeps only the null marker; the value itself is
                # never used by the reconciliation rules.
                wind_speed=None if pooled.wind_missing else 0.0,
            )
        )
    rows.sort(key=lambda row: row.revision)
    pair = PairData(rows=rows, by_revision={row.revision: row for row in rows})
    ingested_us = {
        pooled.revision: pooled.ingested_offset_us
        for pooled in (bundle.pool[pid] for pid in track.revision_ids)
    }
    cache[track_id] = (pair, ingested_us)
    return cache[track_id]


def _replay_entry(
    bundle: ReplayBundle,
    receipt_code: str,
    report_code: str,
    entry: ReceiptEntry,
    pair_cache: dict[str, tuple[PairData, dict[int, int]]],
) -> dict[str, Any]:
    """Re-run one receipt of one report with the original engine logic."""
    pair, ingested_us = _track_pair(bundle, entry.track_id, pair_cache)
    score_minute = _syn_minute(entry.score_minute_offset)
    used_minute = _syn_minute(entry.used_sample_minute_offset)
    receipt = Receipt(
        receipt_id=receipt_code,
        turbine_id=entry.turbine,
        score_minute=score_minute,
        used_sample_minute=used_minute,
        used_revision=entry.used_revision,
    )
    try:
        item = evaluate_receipts([receipt], {(entry.turbine, used_minute): pair})[0]
    except Exception as exc:  # pragma: no cover - defensive, reported as 500
        raise ReplayComputationError(
            f"回执 {receipt_code} 在报告 {report_code} 重算失败: {exc}"
        ) from exc

    evidence = item["evidence"]
    markers = [
        {
            "revision": trail["revision"],
            "ingested_offset_us": ingested_us[trail["revision"]],
            "ingested_before_score": trail["ingested_before_score"],
            "eligible": trail["eligible"],
        }
        for trail in evidence["revisions"]
    ]
    return {
        "usage": [_finding_view(f) for f in item["usage"]],
        "drift": [_finding_view(f) for f in item["drift"]],
        "three_sides": {
            "actual": evidence["actual"]["revision"],
            "should_use": evidence["should_use_at_score"]["revision"],
            "current": evidence["current_highest"]["revision"],
        },
        "revision_markers": markers,
    }


def _stored_findings_view(findings: tuple) -> list[dict[str, Any]]:
    return [
        {
            "kind": finding.kind,
            "left_revision": finding.left_revision,
            "right_revision": finding.right_revision,
        }
        for finding in findings
    ]


def _diff_stored(stored: StoredView, replayed: dict[str, Any]) -> list[dict[str, Any]]:
    """Changes between the stored report conclusion and the fresh replay."""
    changes: list[dict[str, Any]] = []
    stored_usage = _stored_findings_view(stored.usage)
    if stored_usage != replayed["usage"]:
        changes.append(
            {"aspect": "usage", "stored": stored_usage, "replayed": replayed["usage"]}
        )
    stored_drift = _stored_findings_view(stored.drift)
    if stored_drift != replayed["drift"]:
        changes.append(
            {"aspect": "drift", "stored": stored_drift, "replayed": replayed["drift"]}
        )
    if stored.three_sides != replayed["three_sides"]:
        changes.append(
            {
                "aspect": "three_sides",
                "stored": stored.three_sides,
                "replayed": replayed["three_sides"],
            }
        )
    return changes


def _diff_reports(
    first: dict[str, Any], second: dict[str, Any]
) -> dict[str, Any]:
    """Before/after changes between the two reports for one receipt."""
    changes: dict[str, Any] = {
        "changed": False,
        "usage": None,
        "drift": None,
        "three_sides": {},
        "revision_changes": [],
    }
    if first["usage"] != second["usage"]:
        changes["usage"] = {"before": first["usage"], "after": second["usage"]}
    if first["drift"] != second["drift"]:
        changes["drift"] = {"before": first["drift"], "after": second["drift"]}
    for side in ("actual", "should_use", "current"):
        before = first["three_sides"][side]
        after = second["three_sides"][side]
        if before != after:
            changes["three_sides"][side] = {"before": before, "after": after}

    markers_first = {m["revision"]: m for m in first["revision_markers"]}
    markers_second = {m["revision"]: m for m in second["revision_markers"]}
    for revision in sorted(set(markers_first) | set(markers_second)):
        before = markers_first.get(revision)
        after = markers_second.get(revision)
        if before is None:
            changes["revision_changes"].append(
                {"revision": revision, "change": "added", "after": after}
            )
        elif after is None:
            changes["revision_changes"].append(
                {"revision": revision, "change": "removed", "before": before}
            )
        elif before != after:
            kind = (
                "reingested"
                if before["ingested_offset_us"] != after["ingested_offset_us"]
                else "markers"
            )
            changes["revision_changes"].append(
                {
                    "revision": revision,
                    "change": kind,
                    "before": before,
                    "after": after,
                }
            )

    changes["changed"] = bool(
        changes["usage"]
        or changes["drift"]
        or changes["three_sides"]
        or changes["revision_changes"]
    )
    return changes


def compute_replay(bundle: ReplayBundle) -> tuple[IO[bytes], dict[str, Any], int]:
    """Compute every receipt of both reports into a spool file.

    Returns ``(spool, summary, chunk_count)`` with the spool rewound-ready
    (caller seeks).  Any failure raises before a single response byte is
    produced, so the request only ever reports an error.
    """
    spool = tempfile.SpooledTemporaryFile(max_size=SPOOL_MEM_BYTES, mode="w+b")
    chunk_count = 0
    changed_between_reports = 0
    stored_mismatches = 0
    first_code, second_code = bundle.report_codes
    pair_cache: dict[str, tuple[PairData, dict[int, int]]] = {}
    try:
        for bundle_receipt in bundle.receipts:
            sections: dict[str, Any] = {}
            views: dict[str, dict[str, Any]] = {}
            for report_code in bundle.report_codes:
                entry = bundle_receipt.per_report[report_code]
                view = _replay_entry(
                    bundle, bundle_receipt.code, report_code, entry, pair_cache
                )
                views[report_code] = view
                stored_changes = _diff_stored(entry.stored, view)
                if stored_changes:
                    stored_mismatches += 1
                sections[report_code] = {
                    "report": report_code,
                    "receipt": bundle_receipt.code,
                    "turbine": entry.turbine,
                    "score_minute_offset": entry.score_minute_offset,
                    "used_sample_minute_offset": entry.used_sample_minute_offset,
                    "used_revision": entry.used_revision,
                    "usage": view["usage"],
                    "drift": view["drift"],
                    "three_sides": view["three_sides"],
                    "revision_markers": view["revision_markers"],
                    "stored_vs_replay": stored_changes,
                }
            report_changes = _diff_reports(views[first_code], views[second_code])
            if report_changes["changed"]:
                changed_between_reports += 1
            chunk = {
                "receipt": bundle_receipt.code,
                "reports": sections,
                "report_changes": report_changes,
            }
            spool.write(json.dumps(chunk, ensure_ascii=False).encode("utf-8"))
            spool.write(b"\n")
            chunk_count += 1
    except BaseException:
        spool.close()
        raise
    summary = {
        "receipt_count": chunk_count,
        "reports": [
            {"code": code, "receipt_count": chunk_count} for code in bundle.report_codes
        ],
        "receipts_changed_between_reports": changed_between_reports,
        "stored_vs_replay_mismatches": stored_mismatches,
        "integrity": "ok" if stored_mismatches == 0 else "mismatch",
    }
    spool.seek(0)
    return spool, summary, chunk_count


def stream_replay_result(
    bundle: ReplayBundle,
    spool: IO[bytes],
    summary: dict[str, Any],
    chunk_count: int,
) -> Iterator[bytes]:
    """Yield the final JSON document from the spool, closing it at the end."""
    head = {
        "format": RESULT_FORMAT,
        "format_version": BUNDLE_VERSION,
        "bundle_format": BUNDLE_FORMAT,
        "rule": bundle.rule,
        "window": bundle.window,
        "reports": [{"code": code} for code in bundle.report_codes],
        "receipt_count": chunk_count,
    }
    try:
        yield json.dumps(head, ensure_ascii=False).encode("utf-8")[:-1] + b',"results":['
        for index in range(chunk_count):
            line = spool.readline()
            if not line:
                break
            yield line.rstrip(b"\n")
            yield b"," if index + 1 < chunk_count else b""
        yield b'],"summary":' + json.dumps(summary, ensure_ascii=False).encode("utf-8") + b"}"
    finally:
        spool.close()
