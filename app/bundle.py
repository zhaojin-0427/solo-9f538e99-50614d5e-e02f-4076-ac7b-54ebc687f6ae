"""Replay bundles: desensitised export of two stored reports, and validation.

A **replay bundle** is a self-contained JSON package that lets the turbine
vendor re-run two stored reports offline and see why the same receipt came
out differently.  Everything in the bundle is desensitised:

* report ids, turbine ids and receipt ids are replaced by package-local
  codes (``R1``/``R2``, ``T1``…, ``RC0001``…);
* sampling / ingestion / scoring times become offsets from one shared,
  undisclosed epoch (minutes for sampling/scoring, microseconds for
  ingestion instants);
* wind speeds keep only a null marker (``wind_missing``);
* identical desensitised revisions are stored once in a shared pool and
  referenced by id, while each report keeps its own tracks and receipts.

Only data frozen into the stored reports (``report_items``) is exported —
never the live warehouse tables — so rows that were missing before a
back-fill stay missing in the bundle.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

from .db import Database
from .domain import (
    INSTANT_FORMAT,
    MINUTE_FORMAT,
    parse_text,
)
from .engine import RULE_TEXT, WINDOW_END_INCLUSIVE, WINDOW_HOURS, WINDOW_START_INCLUSIVE
from .storage import (
    get_report_items_for_receipts,
    get_report_row,
    list_report_receipt_ids,
)

BUNDLE_FORMAT = "windscore-replay-bundle"
BUNDLE_VERSION = 1

#: Maximum number of shared receipts one bundle may carry.
MAX_BUNDLE_RECEIPTS = 100

FINDING_KINDS = {"missing", "revision", "window"}


class ExportError(ValueError):
    """The bundle cannot be built; carries an HTTP status and a payload."""

    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self.payload = payload
        super().__init__(payload.get("message", "export failed"))


class BundleError(ValueError):
    """An uploaded bundle is malformed or contains dangling references."""


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------
def _instant_dt(value: str) -> datetime:
    return datetime.strptime(value, INSTANT_FORMAT).replace(tzinfo=timezone.utc)


def _minute_dt(value: str) -> datetime:
    return datetime.strptime(value, MINUTE_FORMAT).replace(tzinfo=timezone.utc)


def _delta_minutes(delta) -> int:
    return (delta.days * 86_400 + delta.seconds) // 60


def _delta_microseconds(delta) -> int:
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _stored_finding(finding: dict[str, Any]) -> dict[str, Any]:
    left = finding.get("left") or {}
    right = finding.get("right") or {}
    return {
        "kind": finding.get("kind"),
        "left_revision": left.get("revision"),
        "right_revision": right.get("revision"),
    }


def _stored_view(item: dict[str, Any]) -> dict[str, Any]:
    evidence = item["evidence"]
    actual = evidence.get("actual") or {}
    should = evidence.get("should_use_at_score") or {}
    current = evidence.get("current_highest") or {}
    return {
        "usage": [_stored_finding(f) for f in item["usage"]],
        "drift": [_stored_finding(f) for f in item["drift"]],
        "three_sides": {
            "actual": actual.get("revision"),
            "should_use": should.get("revision"),
            "current": current.get("revision"),
        },
    }


def _select_receipts(
    db: Database,
    report_ids: Sequence[str],
    receipt_ids: Sequence[str] | None,
) -> list[str]:
    per_report = {rid: set(list_report_receipt_ids(db, rid)) for rid in report_ids}
    if receipt_ids is not None:
        if not receipt_ids:
            raise ExportError(422, {"message": "receipt_ids 为空；请给出共有回执号或省略该字段"})
        seen: set[str] = set()
        selected: list[str] = []
        for raw in receipt_ids:
            rid = parse_text(raw, field_name="receipt_id")
            if rid not in seen:
                seen.add(rid)
                selected.append(rid)
        if len(selected) > MAX_BUNDLE_RECEIPTS:
            raise ExportError(
                413,
                {
                    "message": f"最多导出 {MAX_BUNDLE_RECEIPTS} 个共有回执，本次 {len(selected)} 个",
                    "max_receipts": MAX_BUNDLE_RECEIPTS,
                    "requested": len(selected),
                },
            )
        missing = []
        for rid in selected:
            absent = [rep for rep in report_ids if rid not in per_report[rep]]
            if absent:
                missing.append({"receipt_id": rid, "missing_in": absent})
        if missing:
            raise ExportError(
                422,
                {
                    "message": "部分回执号不是两份报告的共有回执",
                    "not_shared": missing,
                },
            )
        return sorted(selected)
    shared = sorted(set.intersection(*per_report.values())) if per_report else []
    if not shared:
        raise ExportError(422, {"message": "两份报告没有共有回执，无法制作回放包"})
    if len(shared) > MAX_BUNDLE_RECEIPTS:
        raise ExportError(
            413,
            {
                "message": (
                    f"两份报告的共有回执共 {len(shared)} 个，超过 "
                    f"{MAX_BUNDLE_RECEIPTS} 个上限，请显式指定 receipt_ids"
                ),
                "max_receipts": MAX_BUNDLE_RECEIPTS,
                "shared": len(shared),
            },
        )
    return shared


def build_replay_bundle(
    db: Database,
    report_ids: Sequence[str],
    receipt_ids: Sequence[str] | None,
) -> dict[str, Any]:
    """Build the desensitised replay bundle for two stored reports."""
    ids = [parse_text(rid, field_name="report_id") for rid in report_ids]
    if len(ids) != 2 or ids[0] == ids[1]:
        raise ExportError(
            422, {"message": "需要恰好两份不同的已存报告（report_ids 长度为 2 且不相同）"}
        )
    rows = {}
    for rid in ids:
        row = get_report_row(db, rid)
        if row is None:
            raise ExportError(404, {"message": f"report {rid} not found", "report_id": rid})
        rows[rid] = row
    # R1 is the earlier report, R2 the later one (deterministic ordering).
    ordered = sorted(ids, key=lambda rid: (rows[rid]["created_at"], rid))
    report_codes = {ordered[0]: "R1", ordered[1]: "R2"}

    selected = _select_receipts(db, ids, receipt_ids)
    receipt_code = {rid: f"RC{index:04d}" for index, rid in enumerate(selected, start=1)}

    items_by_report = {
        rid: {item["receipt_id"]: item for item in get_report_items_for_receipts(db, rid, selected)}
        for rid in ids
    }

    turbine_ids = sorted(
        {
            item["turbine_id"]
            for items in items_by_report.values()
            for item in items.values()
        }
    )
    turbine_code = {tid: f"T{index}" for index, tid in enumerate(turbine_ids, start=1)}

    pool: dict[tuple, dict[str, Any]] = {}
    tracks: dict[tuple, dict[str, Any]] = {}
    receipts: dict[str, dict[str, Any]] = {
        receipt_code[rid]: {"code": receipt_code[rid], "reports": {}}
        for rid in selected
    }
    min_minute: str | None = None
    min_instant: str | None = None

    try:
        for rid in ordered:
            rcode = report_codes[rid]
            for receipt_id in selected:
                item = items_by_report[rid][receipt_id]
                tcode = turbine_code[item["turbine_id"]]
                minute = item["used_sample_minute"]
                score_minute = item["score_minute"]
                min_minute = (
                    minute if min_minute is None else min(min_minute, minute)
                )
                min_minute = min(min_minute, score_minute)
                track_key = (rcode, tcode, minute)
                track = tracks.get(track_key)
                if track is None:
                    track = {
                        "id": f"K{len(tracks) + 1}",
                        "report": rcode,
                        "turbine": tcode,
                        "sample_minute": minute,
                        "revision_ids": [],
                    }
                    tracks[track_key] = track
                    # The trail of a (report, turbine, minute) is identical
                    # for every receipt of that report (same frozen snapshot),
                    # so it is pooled into the track exactly once.
                    trail = sorted(
                        item["evidence"].get("revisions") or [],
                        key=lambda row: row["revision"],
                    )
                    for entry in trail:
                        ingested = entry["ingested_at"]
                        min_instant = (
                            ingested
                            if min_instant is None
                            else min(min_instant, ingested)
                        )
                        pool_key = (
                            tcode,
                            minute,
                            int(entry["revision"]),
                            ingested,
                            entry.get("wind_speed") is None,
                        )
                        pooled = pool.get(pool_key)
                        if pooled is None:
                            pooled = {
                                "id": f"V{len(pool) + 1}",
                                "turbine": tcode,
                                "sample_minute": minute,
                                "revision": int(entry["revision"]),
                                "ingested_at": ingested,
                                "wind_missing": entry.get("wind_speed") is None,
                            }
                            pool[pool_key] = pooled
                        track["revision_ids"].append(pooled["id"])
                receipts[receipt_code[receipt_id]]["reports"][rcode] = {
                    "turbine": tcode,
                    "score_minute": score_minute,
                    "used_sample_minute": minute,
                    "used_revision": int(item["used_revision"]),
                    "track": track["id"],
                    "stored": _stored_view(item),
                }
    except (KeyError, TypeError, AttributeError, ValueError) as exc:
        raise ExportError(
            500, {"message": f"报告内保存的条目数据损坏，无法导出回放包: {exc}"}
        ) from exc

    # One shared epoch for the whole bundle; itself never disclosed.
    candidates = []
    if min_minute is not None:
        candidates.append(_minute_dt(min_minute))
    if min_instant is not None:
        candidates.append(_instant_dt(min_instant))
    epoch = min(candidates)

    minute_cache: dict[str, int] = {}

    def minute_offset(value: str) -> int:
        if value not in minute_cache:
            minute_cache[value] = _delta_minutes(_minute_dt(value) - epoch)
        return minute_cache[value]

    instant_cache: dict[str, int] = {}

    def instant_offset(value: str) -> int:
        if value not in instant_cache:
            instant_cache[value] = _delta_microseconds(_instant_dt(value) - epoch)
        return instant_cache[value]

    return {
        "format": BUNDLE_FORMAT,
        "format_version": BUNDLE_VERSION,
        "rule": RULE_TEXT,
        "window": {
            "hours": WINDOW_HOURS,
            "start_inclusive": WINDOW_START_INCLUSIVE,
            "end_inclusive": WINDOW_END_INCLUSIVE,
        },
        "time_model": {
            "origin": "package-local epoch (desensitised, not disclosed)",
            "minute_offset_unit": "minutes",
            "instant_offset_unit": "microseconds",
        },
        "reports": [{"code": report_codes[rid]} for rid in ordered],
        "turbines": [{"code": turbine_code[tid]} for tid in turbine_ids],
        "revision_pool": [
            {
                "id": entry["id"],
                "turbine": entry["turbine"],
                "sample_minute_offset": minute_offset(entry["sample_minute"]),
                "revision": entry["revision"],
                "ingested_offset_us": instant_offset(entry["ingested_at"]),
                "wind_missing": entry["wind_missing"],
            }
            for entry in pool.values()
        ],
        "tracks": [
            {
                "id": track["id"],
                "report": track["report"],
                "turbine": track["turbine"],
                "sample_minute_offset": minute_offset(track["sample_minute"]),
                "revisions": track["revision_ids"],
            }
            for track in tracks.values()
        ],
        "receipts": [
            {
                "code": receipt["code"],
                "reports": {
                    rcode: {
                        "turbine": entry["turbine"],
                        "score_minute_offset": minute_offset(entry["score_minute"]),
                        "used_sample_minute_offset": minute_offset(
                            entry["used_sample_minute"]
                        ),
                        "used_revision": entry["used_revision"],
                        "track": entry["track"],
                        "stored": entry["stored"],
                    }
                    for rcode, entry in receipt["reports"].items()
                },
            }
            for receipt in receipts.values()
        ],
    }


# ---------------------------------------------------------------------------
# parsing / validation
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PoolEntry:
    ident: str
    turbine: str
    minute_offset: int
    revision: int
    ingested_offset_us: int
    wind_missing: bool


@dataclass(frozen=True)
class Track:
    ident: str
    report: str
    turbine: str
    minute_offset: int
    revision_ids: tuple[str, ...]


@dataclass(frozen=True)
class StoredFinding:
    kind: str
    left_revision: int | None
    right_revision: int | None


@dataclass(frozen=True)
class StoredView:
    usage: tuple[StoredFinding, ...]
    drift: tuple[StoredFinding, ...]
    three_sides: dict[str, int | None]


@dataclass(frozen=True)
class ReceiptEntry:
    turbine: str
    score_minute_offset: int
    used_sample_minute_offset: int
    used_revision: int
    track_id: str
    stored: StoredView


@dataclass(frozen=True)
class BundleReceipt:
    code: str
    per_report: dict[str, ReceiptEntry]


@dataclass
class ReplayBundle:
    report_codes: list[str]
    turbines: list[str]
    pool: dict[str, PoolEntry]
    tracks: dict[str, Track]
    receipts: list[BundleReceipt]
    rule: str = ""
    window: dict[str, Any] = field(default_factory=dict)


def _is_str(value: Any) -> bool:
    return type(value) is str


def _is_int(value: Any) -> bool:
    return type(value) is int


def _is_bool(value: Any) -> bool:
    return type(value) is bool


def _is_opt_int(value: Any) -> bool:
    return value is None or type(value) is int


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise BundleError(message)


def _parse_stored(raw: Any, where: str) -> StoredView:
    _require(isinstance(raw, dict), f"{where}: stored 必须是对象")

    def findings(key: str) -> tuple[StoredFinding, ...]:
        value = raw.get(key)
        _require(isinstance(value, list), f"{where}: stored.{key} 必须是数组")
        parsed = []
        for index, item in enumerate(value):
            _require(isinstance(item, dict), f"{where}: stored.{key}[{index}] 必须是对象")
            kind = item.get("kind")
            _require(kind in FINDING_KINDS, f"{where}: stored.{key}[{index}].kind 非法: {kind!r}")
            left = item.get("left_revision")
            right = item.get("right_revision")
            _require(
                _is_opt_int(left) and _is_opt_int(right),
                f"{where}: stored.{key}[{index}] 的修订号必须是整数或 null",
            )
            parsed.append(StoredFinding(kind=kind, left_revision=left, right_revision=right))
        return tuple(parsed)

    sides = raw.get("three_sides")
    _require(isinstance(sides, dict), f"{where}: stored.three_sides 必须是对象")
    three: dict[str, int | None] = {}
    for key in ("actual", "should_use", "current"):
        value = sides.get(key)
        _require(_is_opt_int(value), f"{where}: stored.three_sides.{key} 必须是整数或 null")
        three[key] = value
    return StoredView(usage=findings("usage"), drift=findings("drift"), three_sides=three)


def parse_replay_bundle(raw: Any) -> ReplayBundle:
    """Validate an uploaded bundle and resolve every internal reference.

    Any malformed shape or dangling reference raises :class:`BundleError`;
    the caller turns that into a single error response for the whole request.
    """
    _require(isinstance(raw, dict), "回放包必须是 JSON 对象")
    _require(raw.get("format") == BUNDLE_FORMAT, f"format 必须是 {BUNDLE_FORMAT!r}")
    _require(raw.get("format_version") == BUNDLE_VERSION, "format_version 必须是 1")

    window = raw.get("window")
    _require(isinstance(window, dict), "window 必须是对象")
    _require(
        window.get("hours") == WINDOW_HOURS
        and window.get("start_inclusive") is WINDOW_START_INCLUSIVE
        and window.get("end_inclusive") is WINDOW_END_INCLUSIVE,
        "包内窗口参数与回放规则不一致，无法按原核对逻辑回放",
    )

    reports_raw = raw.get("reports")
    _require(isinstance(reports_raw, list), "reports 必须是数组")
    report_codes: list[str] = []
    for item in reports_raw:
        _require(isinstance(item, dict) and _is_str(item.get("code")), "reports 条目缺 code")
        _require(item["code"] not in report_codes, f"报告代号重复: {item['code']}")
        report_codes.append(item["code"])
    _require(len(report_codes) == 2, "回放包必须恰好包含两份报告")

    turbines_raw = raw.get("turbines")
    _require(isinstance(turbines_raw, list), "turbines 必须是数组")
    turbines: list[str] = []
    for item in turbines_raw:
        _require(isinstance(item, dict) and _is_str(item.get("code")), "turbines 条目缺 code")
        _require(item["code"] not in turbines, f"机组代号重复: {item['code']}")
        turbines.append(item["code"])
    turbine_set = set(turbines)

    pool_raw = raw.get("revision_pool")
    _require(isinstance(pool_raw, list), "revision_pool 必须是数组")
    pool: dict[str, PoolEntry] = {}
    for index, item in enumerate(pool_raw):
        where = f"revision_pool[{index}]"
        _require(isinstance(item, dict), f"{where} 必须是对象")
        ident = item.get("id")
        _require(_is_str(ident), f"{where} 缺 id")
        _require(ident not in pool, f"{where} id 重复: {ident}")
        turbine = item.get("turbine")
        _require(
            _is_str(turbine) and turbine in turbine_set,
            f"{where} 引用的机组代号 {turbine!r} 在包内不存在",
        )
        minute_offset = item.get("sample_minute_offset")
        revision = item.get("revision")
        ingested = item.get("ingested_offset_us")
        wind_missing = item.get("wind_missing")
        _require(_is_int(minute_offset), f"{where}.sample_minute_offset 必须是整数")
        _require(_is_int(revision) and revision >= 0, f"{where}.revision 必须是非负整数")
        _require(_is_int(ingested), f"{where}.ingested_offset_us 必须是整数")
        _require(_is_bool(wind_missing), f"{where}.wind_missing 必须是布尔值")
        pool[ident] = PoolEntry(
            ident=ident,
            turbine=turbine,
            minute_offset=minute_offset,
            revision=revision,
            ingested_offset_us=ingested,
            wind_missing=wind_missing,
        )

    tracks_raw = raw.get("tracks")
    _require(isinstance(tracks_raw, list), "tracks 必须是数组")
    tracks: dict[str, Track] = {}
    for index, item in enumerate(tracks_raw):
        where = f"tracks[{index}]"
        _require(isinstance(item, dict), f"{where} 必须是对象")
        ident = item.get("id")
        _require(_is_str(ident), f"{where} 缺 id")
        _require(ident not in tracks, f"{where} id 重复: {ident}")
        report = item.get("report")
        _require(
            _is_str(report) and report in report_codes,
            f"{where} 引用的报告代号 {report!r} 在包内不存在",
        )
        turbine = item.get("turbine")
        _require(
            _is_str(turbine) and turbine in turbine_set,
            f"{where} 引用的机组代号 {turbine!r} 在包内不存在",
        )
        minute_offset = item.get("sample_minute_offset")
        _require(_is_int(minute_offset), f"{where}.sample_minute_offset 必须是整数")
        revision_ids = item.get("revisions")
        _require(
            isinstance(revision_ids, list) and all(_is_str(v) for v in revision_ids),
            f"{where}.revisions 必须是代号数组",
        )
        seen_revisions: set[int] = set()
        for pid in revision_ids:
            entry = pool.get(pid)
            _require(entry is not None, f"{where} 引用的修订 {pid!r} 在修订池中不存在")
            _require(
                entry.turbine == turbine and entry.minute_offset == minute_offset,
                f"{where} 引用的修订 {pid!r} 与轨迹的机组/采样分钟不一致",
            )
            _require(
                entry.revision not in seen_revisions,
                f"{where} 内修订号 {entry.revision} 重复",
            )
            seen_revisions.add(entry.revision)
        tracks[ident] = Track(
            ident=ident,
            report=report,
            turbine=turbine,
            minute_offset=minute_offset,
            revision_ids=tuple(revision_ids),
        )

    receipts_raw = raw.get("receipts")
    _require(isinstance(receipts_raw, list) and receipts_raw, "receipts 必须是非空数组")
    _require(
        len(receipts_raw) <= MAX_BUNDLE_RECEIPTS,
        f"回放包最多 {MAX_BUNDLE_RECEIPTS} 张回执，本包 {len(receipts_raw)} 张",
    )
    receipts: list[BundleReceipt] = []
    seen_codes: set[str] = set()
    expected_reports = set(report_codes)
    for index, item in enumerate(receipts_raw):
        where = f"receipts[{index}]"
        _require(isinstance(item, dict), f"{where} 必须是对象")
        code = item.get("code")
        _require(_is_str(code), f"{where} 缺 code")
        _require(code not in seen_codes, f"回执代号重复: {code}")
        seen_codes.add(code)
        per_report_raw = item.get("reports")
        _require(isinstance(per_report_raw, dict), f"{where}.reports 必须是对象")
        _require(
            set(per_report_raw.keys()) == expected_reports,
            f"{where} 必须同时包含两份报告 {sorted(expected_reports)} 的条目",
        )
        per_report: dict[str, ReceiptEntry] = {}
        for rcode in report_codes:
            entry_raw = per_report_raw[rcode]
            ewhere = f"{where}.reports.{rcode}"
            _require(isinstance(entry_raw, dict), f"{ewhere} 必须是对象")
            turbine = entry_raw.get("turbine")
            _require(
                _is_str(turbine) and turbine in turbine_set,
                f"{ewhere} 引用的机组代号 {turbine!r} 在包内不存在",
            )
            score_offset = entry_raw.get("score_minute_offset")
            used_offset = entry_raw.get("used_sample_minute_offset")
            used_revision = entry_raw.get("used_revision")
            _require(_is_int(score_offset), f"{ewhere}.score_minute_offset 必须是整数")
            _require(_is_int(used_offset), f"{ewhere}.used_sample_minute_offset 必须是整数")
            _require(
                _is_int(used_revision) and used_revision >= 0,
                f"{ewhere}.used_revision 必须是非负整数",
            )
            track_id = entry_raw.get("track")
            track = tracks.get(track_id) if _is_str(track_id) else None
            _require(track is not None, f"{ewhere} 引用的修订轨迹 {track_id!r} 在包内不存在")
            _require(
                track.report == rcode,
                f"{ewhere} 引用的轨迹 {track_id!r} 属于另一份报告，每份报告只能用自己的修订",
            )
            _require(
                track.turbine == turbine and track.minute_offset == used_offset,
                f"{ewhere} 引用的轨迹 {track_id!r} 与回执的机组/采样分钟不一致",
            )
            per_report[rcode] = ReceiptEntry(
                turbine=turbine,
                score_minute_offset=score_offset,
                used_sample_minute_offset=used_offset,
                used_revision=used_revision,
                track_id=track_id,
                stored=_parse_stored(entry_raw.get("stored"), ewhere),
            )
        receipts.append(BundleReceipt(code=code, per_report=per_report))

    return ReplayBundle(
        report_codes=report_codes,
        turbines=turbines,
        pool=pool,
        tracks=tracks,
        receipts=receipts,
        rule=raw.get("rule") if _is_str(raw.get("rule")) else "",
        window=window,
    )
