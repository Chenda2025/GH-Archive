"""Download GH Archive hourly dumps, or fall back to a nested JSON sample."""

from __future__ import annotations

import gzip
import io
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from app import HDFS_FILENAME, RAW_DIR
from app.sample_data import generate_events, write_ndjson

ProgressFn = Optional[Callable[[dict[str, Any]], None]]
ARCHIVE_DAY = "2023-03-15"


class _GzipStream(io.RawIOBase):
    def __init__(self, iterator):
        self._iterator = iterator
        self._buf = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:  # type: ignore[override]
        want = len(b)
        while len(self._buf) < want:
            try:
                chunk = next(self._iterator)
            except StopIteration:
                break
            if not chunk:
                break
            self._buf += chunk
        if not self._buf:
            return 0
        n = min(want, len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


def _write_events(handle, lines: list[str]) -> int:
    for line in lines:
        handle.write(line if line.endswith("\n") else line + "\n")
    return len(lines)


def download_gharchive_hours(
    dest: Path,
    day: str = ARCHIVE_DAY,
    hours: int = 1,
    max_events: int = 8000,
    progress: ProgressFn = None,
) -> dict[str, Any]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    used_hours: list[int] = []
    with dest.open("w", encoding="utf-8") as out:
        for hour in range(hours):
            url = f"https://data.gharchive.org/{day}-{hour}.json.gz"
            if progress:
                progress(
                    {
                        "stage": "download",
                        "status": f"fetching {day}-{hour}.json.gz",
                        "url": url,
                        "written": written,
                    }
                )
            try:
                with httpx.Client(timeout=120.0, follow_redirects=True) as client:
                    with client.stream("GET", url) as response:
                        response.raise_for_status()
                        stream = _GzipStream(response.iter_bytes(chunk_size=64 * 1024))
                        with gzip.GzipFile(fileobj=stream, mode="rb") as gz:
                            while written < max_events:
                                raw = gz.readline()
                                if not raw:
                                    break
                                line = raw.decode("utf-8", errors="replace").strip()
                                if not line:
                                    continue
                                out.write(line + "\n")
                                written += 1
                used_hours.append(hour)
            except Exception as exc:  # network / truncated gzip
                if progress:
                    progress(
                        {
                            "stage": "download",
                            "status": f"hour {hour} failed: {exc}",
                            "written": written,
                        }
                    )
                break
            if written >= max_events:
                break
    return {
        "source": "gharchive",
        "day": day,
        "hours": used_hours,
        "events": written,
        "path": str(dest),
        "bytes": dest.stat().st_size if dest.exists() else 0,
    }


def prepare_dataset(
    source: str = "sample",
    max_events: int = 8000,
    hours: int = 1,
    day: str = ARCHIVE_DAY,
    progress: ProgressFn = None,
) -> dict[str, Any]:
    dest = RAW_DIR / HDFS_FILENAME
    source = (source or "sample").lower()

    if source == "gharchive":
        info = download_gharchive_hours(
            dest, day=day, hours=max(1, hours), max_events=max_events, progress=progress
        )
        if info["events"] >= 50:
            info["preview_ok"] = True
            return info
        if progress:
            progress(
                {
                    "stage": "download",
                    "status": "GH Archive too small or unreachable — generating sample",
                }
            )

    if progress:
        progress({"stage": "download", "status": f"generating {max_events} nested events"})
    events = generate_events(n=max_events, seed=42, day=day)
    write_ndjson(dest, events)
    return {
        "source": "generated" if source != "sample" else "sample",
        "day": day,
        "hours": list(range(24)),
        "events": len(events),
        "path": str(dest),
        "bytes": dest.stat().st_size,
        "preview_ok": True,
        "note": "Synthetic NDJSON matching GH Archive fields type, actor, repo, payload.",
    }


def preview_rows(path: Path, limit: int = 20) -> list[dict[str, Any]]:
    import json

    from app.gh_events import actor_login, extract_language, repo_name

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            rows.append(
                {
                    "type": event.get("type"),
                    "actor": actor_login(event),
                    "repo": repo_name(event),
                    "created_at": event.get("created_at"),
                    "language": extract_language(event),
                    "payload_keys": sorted((event.get("payload") or {}).keys())[:8],
                }
            )
            if len(rows) >= limit:
                break
    return rows
