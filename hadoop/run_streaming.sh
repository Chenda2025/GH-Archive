#!/usr/bin/env bash
# Hadoop Streaming job (cluster) or local pipe simulation.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INPUT="${1:-$ROOT/data/hdfs/data/github/gh_activity_full.json}"
OUTPUT_DIR="${2:-$ROOT/data/results/hadoop_streaming}"
MAPPER="$ROOT/hadoop/mapper.py"
REDUCER="$ROOT/hadoop/reducer.py"

if command -v hadoop >/dev/null 2>&1; then
  hadoop jar "${HADOOP_HOME:?HADOOP_HOME not set}/share/hadoop/tools/lib/hadoop-streaming-"*.jar \
    -files "$MAPPER,$REDUCER" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "$INPUT" \
    -output "$OUTPUT_DIR"
else
  mkdir -p "$OUTPUT_DIR"
  python3 "$MAPPER" < "$INPUT" | sort | python3 "$REDUCER" > "$OUTPUT_DIR/part-00000"
  echo "Local streaming complete → $OUTPUT_DIR/part-00000"
fi
