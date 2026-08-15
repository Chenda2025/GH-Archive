"""HDFS pipeline: upload gh_activity_full.json to /data/github/ (real or simulated)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Optional

from app import HDFS_FILENAME, HDFS_LOGICAL_DIR, HDFS_SIM_ROOT, RAW_DIR

ProgressFn = Optional[Callable[[dict[str, Any]], None]]


def simulated_hdfs_path() -> Path:
    return HDFS_SIM_ROOT / "data" / "github" / HDFS_FILENAME


def hdfs_available() -> bool:
    return shutil.which("hdfs") is not None


def upload_to_hdfs(local_file: Path, progress: ProgressFn = None) -> dict[str, Any]:
    if not local_file.exists():
        raise FileNotFoundError(f"Local dataset missing: {local_file}")

    dest_uri = f"{HDFS_LOGICAL_DIR}/{HDFS_FILENAME}"
    size = local_file.stat().st_size
    if progress:
        progress({"stage": "hdfs", "status": "preparing", "bytes": size})

    if hdfs_available():
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_LOGICAL_DIR], check=False)
        result = subprocess.run(
            ["hdfs", "dfs", "-put", "-f", str(local_file), dest_uri],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr or "hdfs dfs -put failed")
        mode = "cluster"
        readable = dest_uri
        if progress:
            progress({"stage": "hdfs", "status": "uploaded to cluster", "uri": dest_uri})
    else:
        dest = simulated_hdfs_path()
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_file, dest)
        mode = "simulated"
        readable = str(dest)
        if progress:
            progress(
                {
                    "stage": "hdfs",
                    "status": "copied into local HDFS layout",
                    "uri": dest_uri,
                    "path": readable,
                }
            )

    return {
        "mode": mode,
        "logical_uri": dest_uri,
        "readable_path": readable,
        "bytes": size,
        "hdfs_available": mode == "cluster",
    }


def resolve_input_path() -> Path:
    """Prefer the HDFS layout copy, then the raw download."""
    sim = simulated_hdfs_path()
    if sim.exists():
        return sim
    raw = RAW_DIR / HDFS_FILENAME
    if raw.exists():
        return raw
    raise FileNotFoundError(
        "No dataset on the HDFS path or in data/raw. Download or generate first."
    )
