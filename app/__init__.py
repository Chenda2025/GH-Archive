"""Project 6 — GitHub Open-Source Archive Activity & Language Profiling."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
HDFS_SIM_ROOT = DATA_DIR / "hdfs"
HDFS_LOGICAL_DIR = "/data/github"
HDFS_FILENAME = "gh_activity_full.json"
RESULTS_DIR = DATA_DIR / "results"

for _p in (RAW_DIR, HDFS_SIM_ROOT / "data" / "github", RESULTS_DIR):
    _p.mkdir(parents=True, exist_ok=True)
