"""Standalone offline replay service.

This app intentionally has **no database handle at all**: it only accepts an
uploaded replay bundle and re-runs it, so the replay container is detached
from the production warehouse by construction.  Run it with::

    uvicorn app.replay_main:app --port 8001

Interactive docs: http://127.0.0.1:8001/docs
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from . import __version__
from .bundle import BundleError, parse_replay_bundle
from .replay import ReplayComputationError, compute_replay, stream_replay_result

#: Largest accepted bundle upload.  Desensitised bundles are small by design
#: (identical revisions are pooled), so this is a sanity cap, not a target.
MAX_BUNDLE_BYTES = 64 * 1024 * 1024

DESCRIPTION = """
风机打分报告离线回放服务（脱离生产库）。

上传由生产侧 `POST /api/export/replay-bundle` 生成的 JSON 回放包，
按原核对逻辑逐回执重跑包内两份报告，返回：

* 每张回执在每份报告下的两类差异（`usage` / `drift`）类型与三侧所选修订；
* 每条修订的入仓标记（`ingested_before_score`）与入选标记（`eligible`）；
* `stored_vs_replay`：重跑结论与报告保存结论的前后变化；
* `report_changes`：同一回执在两份报告之间的前后变化（含逐修订变化）。

两份报告的结果齐全才返回；包内引用缺失或重算失败时，整次只返回错误。
本服务不连接生产数据库。
"""


def create_replay_app() -> FastAPI:
    app = FastAPI(
        title="风机打分报告离线回放",
        version=__version__,
        description=DESCRIPTION,
        openapi_tags=[
            {"name": "回放", "description": "上传回放包并重跑两份报告"},
            {"name": "运维", "description": "健康检查"},
        ],
    )

    @app.exception_handler(BundleError)
    async def _bundle_error(_: Request, exc: BundleError) -> JSONResponse:
        return JSONResponse(
            status_code=422, content={"detail": f"回放包无效: {exc}"}
        )

    @app.exception_handler(ReplayComputationError)
    async def _compute_error(_: Request, exc: ReplayComputationError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    @app.get("/", tags=["运维"], summary="服务信息")
    def root() -> dict[str, Any]:
        return {
            "service": "wind-score-replay",
            "version": __version__,
            "docs": "/docs",
            "database": None,
            "detached": True,
        }

    @app.get("/health", tags=["运维"], summary="健康检查")
    def health() -> dict[str, Any]:
        return {"status": "ok"}

    def _replay_response(raw: bytes) -> StreamingResponse:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BundleError(f"不是合法的 JSON 回放包: {exc}") from exc
        bundle = parse_replay_bundle(payload)
        # Everything is computed before the response starts: if anything
        # fails, the request returns only an error, never a partial result.
        spool, summary, chunk_count = compute_replay(bundle)
        return StreamingResponse(
            stream_replay_result(bundle, spool, summary, chunk_count),
            media_type="application/json",
            headers={
                "Content-Disposition": 'attachment; filename="windscore-replay-result.json"'
            },
        )

    def _check_size(raw: bytes) -> None:
        if len(raw) > MAX_BUNDLE_BYTES:
            raise HTTPException(
                status_code=413, detail=f"回放包超过 {MAX_BUNDLE_BYTES} 字节上限"
            )

    @app.post(
        "/api/replay",
        tags=["回放"],
        summary="上传回放包（JSON 请求体）并重跑两份报告",
        description=(
            "请求体为回放包 JSON。逐回执重跑两份报告并流式返回完整结果；"
            "包内引用缺失或重算失败时整次只返回错误。"
        ),
    )
    async def replay_body(request: Request) -> StreamingResponse:
        content_length = request.headers.get("content-length")
        if content_length is not None and content_length.isdigit():
            if int(content_length) > MAX_BUNDLE_BYTES:
                raise HTTPException(
                    status_code=413, detail=f"回放包超过 {MAX_BUNDLE_BYTES} 字节上限"
                )
        raw = await request.body()
        _check_size(raw)
        return _replay_response(raw)

    @app.post(
        "/api/replay/import",
        tags=["回放"],
        summary="上传回放包文件（.json）并重跑两份报告",
    )
    async def replay_file(
        file: UploadFile = File(..., description="回放包 JSON 文件"),
    ) -> StreamingResponse:
        raw = await file.read(MAX_BUNDLE_BYTES + 1)
        _check_size(raw)
        return _replay_response(raw)

    return app


app = create_replay_app()
