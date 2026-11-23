"""Pydantic request/response models.

The import models accept both the canonical English field names and common
Chinese / legacy aliases, so the Swagger UI at ``/docs`` can be driven with the
same column headers that the production exports use.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

TURBINE_ALIASES = (
    "turbine_id",
    "turbine",
    "unit_id",
    "unit",
    "machine_id",
    "机组",
    "机组编号",
    "风机",
    "风机编号",
)
SAMPLE_MINUTE_ALIASES = (
    "sample_minute",
    "sample_time",
    "sample_ts",
    "minute",
    "utc_sample_minute",
    "采样分钟",
    "采样时间",
    "采样时刻",
)
REVISION_ALIASES = (
    "revision",
    "rev",
    "version",
    "修订号",
    "版本号",
    "修订",
)
INGESTED_AT_ALIASES = (
    "ingested_at",
    "ingest_time",
    "ingested_time",
    "warehouse_ingested_at",
    "warehouse_time",
    "入仓时刻",
    "生产入仓时刻",
    "入仓时间",
)
WIND_SPEED_ALIASES = (
    "wind_speed",
    "wind",
    "speed",
    "value",
    "风速",
)
RECEIPT_ID_ALIASES = (
    "receipt_id",
    "receipt_no",
    "id",
    "score_id",
    "编号",
    "回执编号",
    "打分编号",
)
SCORE_MINUTE_ALIASES = (
    "score_minute",
    "score_time",
    "scoring_minute",
    "打分分钟",
    "打分时间",
    "打分时刻",
)
USED_SAMPLE_MINUTE_ALIASES = (
    "used_sample_minute",
    "used_minute",
    "used_sample_time",
    "actual_sample_minute",
    "sample_minute",
    "实际使用的采样分钟",
    "实际使用采样分钟",
    "使用的采样分钟",
    "使用采样分钟",
    "实际采样分钟",
)
USED_REVISION_ALIASES = (
    "used_revision",
    "used_rev",
    "actual_revision",
    "revision",
    "实际使用的修订号",
    "实际使用修订号",
    "使用的修订号",
    "使用修订号",
    "实际修订号",
)


class ReadingIn(BaseModel):
    """One minute-level wind reading from the data warehouse."""

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "example": {
                "turbine_id": "WT-001",
                "sample_minute": "2026-01-01T10:05:00Z",
                "revision": 3,
                "ingested_at": "2026-01-01T10:07:12Z",
                "wind_speed": 7.4,
            }
        },
    )

    turbine_id: Any = Field(
        ...,
        validation_alias=AliasChoices(*TURBINE_ALIASES),
        description="机组 / turbine id (e.g. WT-001)",
    )
    sample_minute: Any = Field(
        ...,
        validation_alias=AliasChoices(*SAMPLE_MINUTE_ALIASES),
        description="UTC sampling minute, ISO-8601 (e.g. 2026-01-01T10:05:00Z)",
    )
    revision: Any = Field(
        ...,
        validation_alias=AliasChoices(*REVISION_ALIASES),
        description="修订号 / non-negative revision number",
    )
    ingested_at: Any = Field(
        ...,
        validation_alias=AliasChoices(*INGESTED_AT_ALIASES),
        description="生产入仓时刻 / UTC instant the revision entered the warehouse",
    )
    wind_speed: Any = Field(
        None,
        validation_alias=AliasChoices(*WIND_SPEED_ALIASES),
        description="风速 m/s; null/blank means 缺测 (missing measurement)",
    )


class ReceiptIn(BaseModel):
    """One scoring receipt (打分回执)."""

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "example": {
                "receipt_id": "R-20260101-0001",
                "turbine_id": "WT-001",
                "score_minute": "2026-01-01T11:00:00Z",
                "used_sample_minute": "2026-01-01T10:55:00Z",
                "used_revision": 2,
            }
        },
    )

    receipt_id: Any = Field(
        ...,
        validation_alias=AliasChoices(*RECEIPT_ID_ALIASES),
        description="回执编号 / receipt id",
    )
    turbine_id: Any = Field(
        ...,
        validation_alias=AliasChoices(*TURBINE_ALIASES),
        description="机组 / turbine id",
    )
    score_minute: Any = Field(
        ...,
        validation_alias=AliasChoices(*SCORE_MINUTE_ALIASES),
        description="UTC scoring minute, ISO-8601",
    )
    used_sample_minute: Any = Field(
        ...,
        validation_alias=AliasChoices(*USED_SAMPLE_MINUTE_ALIASES),
        description="实际使用的采样分钟 (UTC, ISO-8601)",
    )
    used_revision: Any = Field(
        ...,
        validation_alias=AliasChoices(*USED_REVISION_ALIASES),
        description="实际使用的修订号",
    )


class ReadingImportEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore")
    items: list[ReadingIn] = Field(
        validation_alias=AliasChoices("items", "readings", "data", "rows")
    )


class ReceiptImportEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore")
    items: list[ReceiptIn] = Field(
        validation_alias=AliasChoices("items", "receipts", "data", "rows")
    )


class ImportResult(BaseModel):
    accepted: int = Field(description="rows accepted (after de-duplicating identical rows)")
    total: int = Field(description="total rows now stored in the table")
    identical_duplicates: int = Field(
        default=0, description="identical duplicated rows collapsed inside the request"
    )


class ReconcileRequest(BaseModel):
    """Which receipts to reconcile.  ``receipt_ids`` wins over filters."""

    model_config = ConfigDict(extra="forbid")

    receipt_ids: list[str] | None = Field(
        default=None,
        validation_alias=AliasChoices("receipt_ids", "receipts", "ids"),
        description="Explicit receipt ids (max 10,000). Takes precedence over filters.",
    )
    turbine_id: str | None = Field(
        default=None, description="Only receipts of this turbine"
    )
    score_minute_from: Any = Field(
        default=None, description="Inclusive lower bound of score minute (UTC)"
    )
    score_minute_to: Any = Field(
        default=None, description="Inclusive upper bound of score minute (UTC)"
    )


class Side(BaseModel):
    """One side of a comparison, with its provenance."""

    label: str
    present: bool = Field(description="row exists in the warehouse now")
    sample_minute: str | None = None
    revision: int | None = None
    wind_speed: float | None = None
    ingested_at: str | None = None
    ingested_before_score: bool | None = Field(
        default=None,
        description="whether this row had entered the warehouse by the score minute",
    )


class Finding(BaseModel):
    kind: Literal["missing", "revision", "window"] = Field(
        description=(
            "missing=缺行/缺测, revision=换版, "
            "window=实际采样分钟不在打分前一小时窗口内"
        )
    )
    detail: str
    left: Side | None = None
    right: Side | None = None


class ReconcileItem(BaseModel):
    receipt_id: str
    turbine_id: str
    score_minute: str
    used_sample_minute: str
    used_revision: int
    usage: list[Finding] = Field(
        description="实际用数 vs 当时应取 (actual used vs should-use-at-score)"
    )
    drift: list[Finding] = Field(
        description="当时应取 vs 当前最高修订 (should-use-at-score vs current highest revision)"
    )
    evidence: dict[str, Any] = Field(
        description="Side-by-side basis for both lists (rule, window, all revisions)"
    )


class ReplayBundleRequest(BaseModel):
    """Which two stored reports (and which shared receipts) to export."""

    model_config = ConfigDict(extra="forbid")

    report_ids: list[str] = Field(
        ...,
        min_length=2,
        max_length=2,
        description="Exactly two different stored report ids",
    )
    receipt_ids: list[str] | None = Field(
        default=None,
        description=(
            "Shared receipt ids to include (max 100). "
            "Omit to use every receipt present in both reports."
        ),
    )


class ReportSummary(BaseModel):
    report_id: str
    status: str
    created_at: str
    receipt_count: int
    usage_finding_count: int
    drift_finding_count: int
    items_with_findings: int


class ReportDetail(ReportSummary):
    request: dict[str, Any]
    snapshot: dict[str, Any]
    item_total: int
    offset: int = 0
    limit: int = 0
    items: list[ReconcileItem]


class ReportList(BaseModel):
    reports: list[ReportSummary]
    total: int
    offset: int
    limit: int


class ReadingList(BaseModel):
    readings: list[dict[str, Any]]
    total: int
    offset: int
    limit: int


class ReceiptList(BaseModel):
    receipts: list[dict[str, Any]]
    total: int
    offset: int
    limit: int
