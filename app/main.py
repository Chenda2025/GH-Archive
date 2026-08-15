"""FastAPI benchmark UI for GitHub Archive Hadoop vs Spark comparison."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app import APP_DIR, HDFS_FILENAME, RAW_DIR
from app.benchmark import run_comparison
from app.download import prepare_dataset, preview_rows
from app.hdfs_pipeline import resolve_input_path, upload_to_hdfs

templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

app = FastAPI(
    title="GH Archive Activity & Language Profiling",
    version="1.0.0",
    description=(
        "Project 6: nested JSON from GH Archive, Hadoop MapReduce vs "
        "Spark spark.read.json(), schema discovery, and activity charts."
    ),
)
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")

_lock = threading.Lock()
_state: dict[str, Any] = {
    "status": "idle",
    "message": "Ready — download a dump, upload to HDFS, then start the engine comparison.",
    "progress": {},
    "progress_history": [],
    "dataset": None,
    "hdfs": None,
    "preview": [],
    "results": None,
    "error": None,
}


class DownloadRequest(BaseModel):
    source: str = "sample"  # sample | gharchive | generate
    max_events: int = Field(default=8000, ge=200, le=80_000)
    hours: int = Field(default=1, ge=1, le=24)
    day: str = "2023-03-15"


class CompareRequest(BaseModel):
    max_events: Optional[int] = Field(default=None, ge=200, le=80_000)


def _set(**kwargs: Any) -> None:
    with _lock:
        _state.update(kwargs)


def _progress(info: dict[str, Any]) -> None:
    with _lock:
        _state["progress"] = info
        _state["message"] = str(info.get("status") or info.get("stage") or "")
        history = _state.setdefault("progress_history", [])
        history.append(dict(info))
        _state["progress_history"] = history[-30:]


def _snapshot() -> dict[str, Any]:
    with _lock:
        return dict(_state)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/state")
async def api_state() -> JSONResponse:
    return JSONResponse(_snapshot())


@app.post("/api/download")
async def api_download(body: DownloadRequest) -> JSONResponse:
    def job() -> None:
        try:
            _set(status="running", error=None, message="Downloading dataset…")
            info = prepare_dataset(
                source=body.source,
                max_events=body.max_events,
                hours=body.hours,
                day=body.day,
                progress=_progress,
            )
            path = Path(info["path"])
            hdfs_info = upload_to_hdfs(path, progress=_progress)
            preview = preview_rows(path, limit=18)
            _set(
                status="idle",
                dataset=info,
                hdfs=hdfs_info,
                preview=preview,
                message=f"Loaded {info['events']:,} events → {hdfs_info['logical_uri']}",
            )
        except Exception as exc:
            _set(status="error", error=str(exc), message="Download failed")

    threading.Thread(target=job, daemon=True).start()
    return JSONResponse({"ok": True})


@app.post("/api/hdfs")
async def api_hdfs() -> JSONResponse:
    def job() -> None:
        try:
            local = RAW_DIR / HDFS_FILENAME
            if not local.exists():
                raise FileNotFoundError("Download or generate gh_activity_full.json first.")
            _set(status="running", error=None, message="Uploading to HDFS /data/github/ …")
            info = upload_to_hdfs(local, progress=_progress)
            _set(status="idle", hdfs=info, message=f"HDFS ready ({info['mode']}) {info['logical_uri']}")
        except Exception as exc:
            _set(status="error", error=str(exc), message="HDFS upload failed")

    threading.Thread(target=job, daemon=True).start()
    return JSONResponse({"ok": True})


@app.post("/api/compare")
async def api_compare(body: CompareRequest) -> JSONResponse:
    def job() -> None:
        try:
            path = resolve_input_path()
            _set(
                status="running",
                error=None,
                results=None,
                progress={},
                progress_history=[],
                message="Starting engine comparison…",
            )
            if not _snapshot().get("preview"):
                _set(preview=preview_rows(path, limit=18))
            result = run_comparison(str(path), max_events=body.max_events, progress=_progress)
            _set(status="idle", results=result, message=f"Done — Spark speedup {result['speedup']:.2f}×")
        except Exception as exc:
            _set(status="error", error=str(exc), message="Comparison failed")

    threading.Thread(target=job, daemon=True).start()
    return JSONResponse({"ok": True})


@app.get("/api/preview")
async def api_preview(limit: int = 18) -> JSONResponse:
    try:
        path = resolve_input_path()
    except FileNotFoundError:
        raw = RAW_DIR / HDFS_FILENAME
        if not raw.exists():
            return JSONResponse({"rows": [], "error": "No dataset yet"}, status_code=404)
        path = raw
    rows = preview_rows(path, limit=limit)
    _set(preview=rows)
    return JSONResponse({"rows": rows})
