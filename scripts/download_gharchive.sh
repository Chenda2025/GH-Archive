#!/usr/bin/env bash
# Download 24 consecutive GH Archive hours and concatenate (assignment recipe).
set -euo pipefail
DAY="${1:-2023-03-15}"
DEST="${2:-$(pwd)/data/raw}"
mkdir -p "$DEST"
cd "$DEST"

echo "Downloading $DAY hours 0–23 from https://data.gharchive.org/"
for hour in $(seq 0 23); do
  url="https://data.gharchive.org/${DAY}-${hour}.json.gz"
  echo "→ $url"
  curl -fL --retry 3 -o "${DAY}-${hour}.json.gz" "$url"
done

gunzip -f *.json.gz
cat ${DAY}-*.json > gh_activity_full.json
python3 - <<'PY'
import json, pathlib
p = pathlib.Path("gh_activity_full.json")
ok = 0
need = {"type", "actor", "repo", "payload"}
with p.open() as f:
    for i, line in enumerate(f):
        if i >= 20:
            break
        obj = json.loads(line)
        missing = need - obj.keys()
        if missing:
            raise SystemExit(f"Record {i} missing {missing}")
        ok += 1
print(f"Verified {ok} records contain type, actor, repo, payload")
print(f"File size: {p.stat().st_size / 1e6:.1f} MB")
PY
