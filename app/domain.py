"""Domain rules and canonical value shapes for wind-score reconciliation.

Everything is UTC.  Two timestamp shapes are used:

* **minute**   ``YYYY-MM-DDTHH:MM:00Z``  - sampling / scoring minutes.
* **instant**  ``YYYY-MM-DDTHH:MM:SS.ffffffZ`` - warehouse ingestion times.

Both are fixed-width so that plain string comparison in SQLite and in Python
has the same meaning as chronological comparison.  Ingestion times keep
microsecond precision on purpose: an ingest at ``10:05:30`` must *not* count
as "before" a score at minute ``10:05``.

A **revision** (修订号) is a non-negative integer that grows when the data
warehouse back-fills or corrects a reading for a (turbine, sample minute).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

MINUTE_FORMAT = "%Y-%m-%dT%H:%M:00Z"
INSTANT_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

#: Maximum number of receipts a single reconciliation batch may cover.
MAX_BATCH_SIZE = 10_000


class ValidationError(ValueError):
    """An uploaded row/value cannot be interpreted."""


class BatchTooLarge(ValueError):
    """More than :data:`MAX_BATCH_SIZE` receipts were requested."""

    def __init__(self, count: int) -> None:
        self.count = count
        super().__init__(f"batch has {count} receipts; maximum is {MAX_BATCH_SIZE}")


class UnknownReceipts(LookupError):
    """Some explicitly requested receipt ids do not exist."""

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        super().__init__("unknown receipt ids: " + ", ".join(missing[:20]))


def parse_utc(
    value: Any,
    *,
    field_name: str = "timestamp",
    truncate_to_minute: bool = False,
) -> datetime:
    """Parse an ISO-8601 timestamp into an aware UTC ``datetime``.

    Naive input is interpreted as UTC because the whole service is UTC-only.
    With ``truncate_to_minute`` the value is floored to the minute (used for
    sampling/scoring minutes; sub-minute noise would never join otherwise).
    """
    if value is None:
        raise ValidationError(f"{field_name} is required")
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            raise ValidationError(f"{field_name} is empty")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValidationError(
                f"{field_name} is not a valid ISO-8601 timestamp: {value!r}"
            ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    if truncate_to_minute:
        dt = dt.replace(second=0, microsecond=0)
    return dt


def format_minute(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(MINUTE_FORMAT)


def format_instant(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(INSTANT_FORMAT)


def parse_minute(value: Any, *, field_name: str) -> str:
    return format_minute(parse_utc(value, field_name=field_name, truncate_to_minute=True))


def parse_instant(value: Any, *, field_name: str) -> str:
    return format_instant(parse_utc(value, field_name=field_name, truncate_to_minute=False))


def minute_datetime(minute: str) -> datetime:
    """Inverse of :func:`format_minute` (used for window arithmetic)."""
    return datetime.strptime(minute, MINUTE_FORMAT).replace(tzinfo=timezone.utc)


def parse_revision(value: Any, *, field_name: str = "revision") -> int:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValidationError(f"{field_name} is required")
    if isinstance(value, bool):
        raise ValidationError(f"{field_name} must be a non-negative integer: {value!r}")
    try:
        revision = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"{field_name} must be a non-negative integer: {value!r}"
        ) from exc
    if revision < 0:
        raise ValidationError(f"{field_name} must be non-negative: {revision}")
    return revision


def parse_text(value: Any, *, field_name: str) -> str:
    if value is None:
        raise ValidationError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        raise ValidationError(f"{field_name} is empty")
    return text


def parse_wind(value: Any) -> float | None:
    """Parse a wind speed in m/s.  Blank / null means a missing measurement."""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"wind_speed must be numeric: {value!r}") from exc
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        raise ValidationError(f"wind_speed must be finite: {value!r}")
    return parsed


@dataclass(frozen=True)
class WindReading:
    turbine_id: str
    sample_minute: str
    revision: int
    ingested_at: str
    wind_speed: float | None


@dataclass(frozen=True)
class Receipt:
    receipt_id: str
    turbine_id: str
    score_minute: str
    used_sample_minute: str
    used_revision: int
