"""HTTP import parsing: JSON bodies and CSV/JSON file uploads."""

from __future__ import annotations

import csv
import io
import json
from typing import Any

from fastapi import HTTPException

MAX_UPLOAD_BYTES = 64 * 1024 * 1024


def _rows_from_json_object(payload: Any) -> list[Any] | None:
    """Turn a parsed JSON document into a list of row dicts, if possible."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "readings", "receipts", "data", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        # A single row object is accepted as a one-row import.
        return [payload]
    return None


def parse_json_rows(raw: bytes) -> list[Any]:
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400, detail="file is not valid UTF-8 text"
        ) from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400, detail=f"invalid JSON: {exc.msg} (line {exc.lineno})"
        ) from exc
    rows = _rows_from_json_object(payload)
    if rows is None:
        raise HTTPException(
            status_code=400,
            detail="JSON must be an array of rows or an object with an items array",
        )
    return rows


def _decode_csv(raw: bytes) -> str:
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        # Exports from older Windows tooling are often GBK.
        try:
            return raw.decode("gb18030")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status_code=400, detail="CSV file is neither UTF-8 nor GBK encoded"
            ) from exc


def parse_csv_rows(raw: bytes) -> list[Any]:
    text = _decode_csv(raw)
    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if reader.fieldnames is None:
        raise HTTPException(status_code=400, detail="CSV file has no header row")
    rows: list[Any] = []
    for row in reader:
        cleaned = {
            (key or "").strip(): (value if value is None else value.strip())
            for key, value in row.items()
            if key is not None
        }
        if not any(value for value in cleaned.values()):
            continue  # skip blank lines
        rows.append(cleaned)
    return rows


def parse_upload(filename: str | None, content: bytes) -> list[Any]:
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"upload exceeds {MAX_UPLOAD_BYTES} bytes",
        )
    name = (filename or "").lower()
    if name.endswith(".json"):
        return parse_json_rows(content)
    if name.endswith((".csv", ".tsv", ".txt")):
        return parse_csv_rows(content)
    # Fall back on content sniffing for unnamed uploads.
    stripped = content.lstrip()[:1]
    if stripped in (b"[", b"{"):
        return parse_json_rows(content)
    return parse_csv_rows(content)


def parse_reading_body(body: Any) -> list[Any]:
    """Normalise a JSON import body into a list of raw row mappings."""
    return _rows_from_json_object(body) or []


def parse_receipt_body(body: Any) -> list[Any]:
    return _rows_from_json_object(body) or []
