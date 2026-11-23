"""FastAPI application for wind-score reconciliation.

Run locally:

    uvicorn app.main:app --reload

Interactive API docs: http://127.0.0.1:8000/docs
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Body, Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from . import __version__
from .bundle import ExportError, build_replay_bundle
from .db import Database
from .domain import (
    MAX_BATCH_SIZE,
    BatchTooLarge,
    UnknownReceipts,
)
from .domain import ValidationError as DomainValidationError
from .engine import new_report_id, reconcile_snapshot, request_document
from .importer import parse_reading_body, parse_receipt_body, parse_upload
from .schemas import (
    ImportResult,
    ReconcileRequest,
    ReplayBundleRequest,
    ReportDetail,
    ReportList,
)
from .storage import (
    ImportValidationError,
    get_report_items,
    get_report_row,
    import_readings,
    import_receipts,
    list_readings,
    list_receipts,
    list_reports,
    persist_report,
    reports_for_receipt,
)

DESCRIPTION = """
风电机组打分用的风速读数与数仓回填对账 API。

**业务规则**

* 打分前 **一小时** 内（窗口 `[打分分钟 - 1h, 打分分钟)`）对同一采样分钟，
  只认 **打分时刻之前已入仓** 的修订，取其中 **最高修订号** 作为「当时应取」。
* 窗口内没有符合条件的读数时，当时应取 **缺测留空**。
* 每张回执输出两份清单：
  1. `usage` — **实际用数 vs 当时应取**：列出缺行与换版，附实际侧 / 应取侧依据。
  2. `drift` — **当时应取 vs 当前最高修订**：列出当时缺行、当时取数后读数换版等。
* 批量核对固定使用 **同一个已提交快照**，核对期间数仓可继续补录；补录只会体现在下一批。
* 单批最多 **10,000** 张回执；整批只发一次风速表关联查询，不按回执逐条扫描。
* 报告（两份清单 + 依据）整体写入 SQLite：失败则不留报告，历史报告可按编号查询。
"""


def create_app(database_path: str | None = None) -> FastAPI:
    db_path = database_path or os.environ.get(
        "WINDSCORE_DB",
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "data", "windscore.db"
        ),
    )
    database = Database(db_path)
    database.initialize()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        database.close()

    app = FastAPI(
        title="风机打分数仓对账 API",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
        openapi_tags=[
            {"name": "导入", "description": "导入分钟风速与打分回执（JSON 或 CSV/JSON 文件）"},
            {"name": "核对", "description": "批量核对并落库报告"},
            {"name": "报告", "description": "按编号查询历史报告"},
            {"name": "导出", "description": "导出两份报告的脱敏回放包"},
            {"name": "数据", "description": "查看已导入的数仓数据与回执"},
            {"name": "运维", "description": "健康检查"},
        ],
    )
    app.state.db = database
    app.state.database_path = db_path

    @app.exception_handler(DomainValidationError)
    async def _domain_error(_: Request, exc: DomainValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(ImportValidationError)
    async def _import_error(_: Request, exc: ImportValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": exc.payload()})

    @app.exception_handler(BatchTooLarge)
    async def _batch_too_large(_: Request, exc: BatchTooLarge) -> JSONResponse:
        return JSONResponse(
            status_code=413,
            content={
                "detail": {
                    "message": f"单批最多 {MAX_BATCH_SIZE} 张回执，本次 {exc.count} 张",
                    "max_batch_size": MAX_BATCH_SIZE,
                    "requested": exc.count,
                }
            },
        )

    @app.exception_handler(UnknownReceipts)
    async def _unknown_receipts(_: Request, exc: UnknownReceipts) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={
                "detail": {
                    "message": "存在未导入的回执编号",
                    "unknown_receipt_ids": exc.missing[:100],
                    "unknown_count": len(exc.missing),
                }
            },
        )

    @app.exception_handler(ExportError)
    async def _export_error(_: Request, exc: ExportError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.payload})

    def get_db() -> Database:
        return app.state.db

    # ------------------------------------------------------------------
    # meta
    # ------------------------------------------------------------------
    @app.get("/", tags=["运维"], summary="服务信息")
    def root() -> dict[str, Any]:
        return {
            "service": "wind-score-reconciliation",
            "version": __version__,
            "docs": "/docs",
            "database": app.state.database_path,
        }

    @app.get("/health", tags=["运维"], summary="健康检查")
    def health(db: Database = Depends(get_db)) -> dict[str, Any]:
        conn = db.connect()
        try:
            conn.execute("SELECT 1").fetchone()
        finally:
            conn.close()
        return {"status": "ok", "database": app.state.database_path}

    # ------------------------------------------------------------------
    # imports
    # ------------------------------------------------------------------
    @app.post(
        "/api/readings",
        response_model=ImportResult,
        tags=["导入"],
        summary="导入分钟风速（JSON）",
        description=(
            "请求体可为行数组、`{\"items\": [...]}` 或单行对象。"
            "字段：机组、UTC 采样分钟、修订号、生产入仓时刻、风速。"
            "同一 (机组, 采样分钟, 修订号) 重复导入时按后到覆盖（用于修订入仓时刻/读数更正）。"
        ),
    )
    def post_readings(
        payload: list[dict[str, Any]] | dict[str, Any] = Body(
            ...,
            openapi_examples={
                "rows": {
                    "summary": "行数组",
                    "value": [
                        {
                            "turbine_id": "WT-001",
                            "sample_minute": "2026-01-01T10:05:00Z",
                            "revision": 3,
                            "ingested_at": "2026-01-01T10:07:12Z",
                            "wind_speed": 7.4,
                        }
                    ],
                },
                "chinese_headers": {
                    "summary": "中文列名",
                    "value": {
                        "items": [
                            {
                                "机组": "WT-001",
                                "采样分钟": "2026-01-01T10:05:00Z",
                                "修订号": 3,
                                "生产入仓时刻": "2026-01-01T10:07:12Z",
                                "风速": 7.4,
                            }
                        ]
                    },
                },
            },
        ),
        db: Database = Depends(get_db),
    ) -> dict[str, Any]:
        rows = parse_reading_body(payload)
        return import_readings(db, rows)

    @app.post(
        "/api/readings/import",
        response_model=ImportResult,
        tags=["导入"],
        summary="导入分钟风速（CSV/JSON 文件）",
    )
    async def upload_readings(
        file: UploadFile = File(..., description="CSV（UTF-8/GBK）或 JSON 文件"),
        db: Database = Depends(get_db),
    ) -> dict[str, Any]:
        content = await file.read()
        rows = parse_upload(file.filename, content)
        return import_readings(db, rows)

    @app.post(
        "/api/receipts",
        response_model=ImportResult,
        tags=["导入"],
        summary="导入打分回执（JSON）",
        description=(
            "字段：回执编号、机组、UTC 打分分钟、实际使用的采样分钟、实际使用的修订号。"
        ),
    )
    def post_receipts(
        payload: list[dict[str, Any]] | dict[str, Any] = Body(
            ...,
            openapi_examples={
                "rows": {
                    "summary": "行数组",
                    "value": [
                        {
                            "receipt_id": "R-20260101-0001",
                            "turbine_id": "WT-001",
                            "score_minute": "2026-01-01T11:00:00Z",
                            "used_sample_minute": "2026-01-01T10:55:00Z",
                            "used_revision": 2,
                        }
                    ],
                },
                "chinese_headers": {
                    "summary": "中文列名",
                    "value": {
                        "items": [
                            {
                                "编号": "R-20260101-0001",
                                "机组": "WT-001",
                                "打分分钟": "2026-01-01T11:00:00Z",
                                "实际使用的采样分钟": "2026-01-01T10:55:00Z",
                                "实际使用修订号": 2,
                            }
                        ]
                    },
                },
            },
        ),
        db: Database = Depends(get_db),
    ) -> dict[str, Any]:
        rows = parse_receipt_body(payload)
        return import_receipts(db, rows)

    @app.post(
        "/api/receipts/import",
        response_model=ImportResult,
        tags=["导入"],
        summary="导入打分回执（CSV/JSON 文件）",
    )
    async def upload_receipts(
        file: UploadFile = File(..., description="CSV（UTF-8/GBK）或 JSON 文件"),
        db: Database = Depends(get_db),
    ) -> dict[str, Any]:
        content = await file.read()
        rows = parse_upload(file.filename, content)
        return import_receipts(db, rows)

    # ------------------------------------------------------------------
    # reconciliation
    # ------------------------------------------------------------------
    @app.post(
        "/api/reconcile",
        response_model=ReportDetail,
        tags=["核对"],
        summary="批量核对并保存报告",
        description=(
            "对指定回执（或按机组/打分分钟筛选，最多 10,000 条）在同一个已提交快照上核对，"
            "生成 usage / drift 两份清单及依据并整体写入 SQLite；失败不落报告。"
            "返回的报告可通过 `GET /api/reports/{report_id}` 重新获取。"
        ),
    )
    def reconcile(
        request: ReconcileRequest,
        db: Database = Depends(get_db),
        include_items: bool = Query(
            True, description="是否在响应中内联报告条目（报告始终落库）"
        ),
        item_limit: int = Query(
            200,
            ge=0,
            le=10_000,
            description="内联条目上限，默认 200；完整条目用 GET /api/reports/{report_id} 分页获取",
        ),
    ) -> dict[str, Any]:
        items, snapshot = reconcile_snapshot(db, request)
        report_id = new_report_id()
        request_doc = request_document(request)
        summary = persist_report(db, items, snapshot, request_doc, report_id)
        return _detail_payload(
            db,
            summary,
            snapshot=snapshot,
            request_doc=request_doc,
            include_items=include_items,
            offset=0,
            limit=item_limit,
        )

    # ------------------------------------------------------------------
    # reports
    # ------------------------------------------------------------------
    @app.get(
        "/api/reports",
        response_model=ReportList,
        tags=["报告"],
        summary="报告列表（按创建时间倒序）",
    )
    def get_reports(
        offset: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=500),
        db: Database = Depends(get_db),
    ) -> dict[str, Any]:
        reports, total = list_reports(db, offset, limit)
        return {"reports": reports, "total": total, "offset": offset, "limit": limit}

    @app.get(
        "/api/reports/{report_id}",
        response_model=ReportDetail,
        tags=["报告"],
        summary="按编号查询历史报告（含两份清单与依据）",
    )
    def get_report(
        report_id: str,
        offset: int = Query(0, ge=0),
        limit: int = Query(200, ge=1, le=10000),
        receipt_id: str | None = Query(
            None, description="只看该回执在报告中的条目"
        ),
        db: Database = Depends(get_db),
    ) -> dict[str, Any]:
        row = get_report_row(db, report_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"report {report_id} not found")
        summary = {
            "report_id": row["report_id"],
            "status": row["status"],
            "created_at": row["created_at"],
            "receipt_count": row["receipt_count"],
            "usage_finding_count": row["usage_finding_count"],
            "drift_finding_count": row["drift_finding_count"],
            "items_with_findings": row["items_with_findings"],
        }
        return _detail_payload(
            db,
            summary,
            snapshot=json.loads(row["snapshot_json"]),
            request_doc=json.loads(row["request_json"]),
            include_items=True,
            offset=offset,
            limit=limit,
            receipt_id=receipt_id,
        )

    @app.get(
        "/api/receipts/{receipt_id}/reports",
        tags=["报告"],
        summary="按回执编号查询包含它的历史报告",
    )
    def get_receipt_reports(
        receipt_id: str, db: Database = Depends(get_db)
    ) -> dict[str, Any]:
        reports = reports_for_receipt(db, receipt_id)
        return {"receipt_id": receipt_id, "reports": reports, "total": len(reports)}

    # ------------------------------------------------------------------
    # replay bundle export
    # ------------------------------------------------------------------
    @app.post(
        "/api/export/replay-bundle",
        tags=["导出"],
        summary="导出两份报告的脱敏回放包（JSON 下载）",
        description=(
            "指定两份不同的已存报告与最多 100 个共有回执号（缺省取全部共有回执），"
            "下载一个自包含的脱敏 JSON 回放包：只用报告保存的回执与修订轨迹；"
            "报告号/机组号/回执号替换为包内代号，采样/入仓/打分时间改为相对同一起点的偏移，"
            "风速只留空值标记，相同的脱敏修订只存一份。"
            "回放包可上传到离线回放服务（见 replay 容器）重跑。"
        ),
    )
    def export_replay_bundle(
        request: ReplayBundleRequest,
        db: Database = Depends(get_db),
    ) -> JSONResponse:
        bundle = build_replay_bundle(db, request.report_ids, request.receipt_ids)
        return JSONResponse(
            content=bundle,
            headers={
                "Content-Disposition": 'attachment; filename="windscore-replay-bundle.json"'
            },
        )

    # ------------------------------------------------------------------
    # data inspection
    # ------------------------------------------------------------------
    @app.get(
        "/api/readings",
        tags=["数据"],
        summary="查看已导入的分钟风速",
    )
    def get_readings(
        turbine_id: str | None = Query(None),
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        db: Database = Depends(get_db),
    ) -> dict[str, Any]:
        rows, total = list_readings(
            db, turbine_id=turbine_id, offset=offset, limit=limit
        )
        return {"readings": rows, "total": total, "offset": offset, "limit": limit}

    @app.get(
        "/api/receipts",
        tags=["数据"],
        summary="查看已导入的打分回执",
    )
    def get_receipts(
        turbine_id: str | None = Query(None),
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        db: Database = Depends(get_db),
    ) -> dict[str, Any]:
        rows, total = list_receipts(
            db, turbine_id=turbine_id, offset=offset, limit=limit
        )
        return {"receipts": rows, "total": total, "offset": offset, "limit": limit}

    return app


def _detail_payload(
    db: Database,
    summary: dict[str, Any],
    *,
    snapshot: dict[str, Any],
    request_doc: dict[str, Any],
    include_items: bool,
    offset: int,
    limit: int,
    receipt_id: str | None = None,
) -> dict[str, Any]:
    if include_items:
        items, total = get_report_items(
            db,
            summary["report_id"],
            offset=offset,
            limit=limit,
            receipt_id=receipt_id,
        )
    else:
        items = []
        total = 0 if receipt_id is not None else summary["receipt_count"]
    return {
        **summary,
        "request": request_doc,
        "snapshot": snapshot,
        "item_total": total,
        "offset": offset,
        "limit": limit,
        "items": items,
    }


app = create_app()
