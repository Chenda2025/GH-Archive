#!/usr/bin/env bash
# Upload gh_activity_full.json to HDFS /data/github/ (or the local simulated layout).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${1:-$ROOT/data/raw/gh_activity_full.json}"
if [[ ! -f "$SRC" ]]; then
  echo "Missing $SRC — run scripts/download_gharchive.sh or the web Download action."
  exit 1
fi

if command -v hdfs >/dev/null 2>&1; then
  hdfs dfs -mkdir -p /data/github
  hdfs dfs -put -f "$SRC" /data/github/gh_activity_full.json
  hdfs dfs -ls /data/github
else
  DEST="$ROOT/data/hdfs/data/github"
  mkdir -p "$DEST"
  cp "$SRC" "$DEST/gh_activity_full.json"
  echo "Hadoop not on PATH — copied to simulated HDFS $DEST/gh_activity_full.json"
fi
