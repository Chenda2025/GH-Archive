#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi
exec python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
