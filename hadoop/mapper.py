#!/usr/bin/env python3
"""Hadoop Streaming mapper — parse raw GH Archive JSON lines."""

import json
import sys


def nested(obj, *keys):
    cur = obj
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def language(event):
    payload = event.get("payload") or {}
    for path in (
        ("pull_request", "base", "repo", "language"),
        ("pull_request", "head", "repo", "language"),
        ("forkee", "language"),
        ("repository", "language"),
    ):
        value = nested(payload, *path)
        if value:
            return value
    return None


def emit(key, value=1):
    sys.stdout.write(f"{key}\t{value}\n")


def main():
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        event_type = event.get("type") or "Unknown"
        repo = (event.get("repo") or {}).get("name") or "unknown/unknown"
        user = (event.get("actor") or {}).get("login") or "unknown"
        emit(f"evt|{repo}|{event_type}")
        emit(f"user|{user}")
        emit(f"type|{event_type}")
        if event_type == "PushEvent":
            emit(f"push|{repo}")
            payload = event.get("payload") or {}
            size = payload.get("size")
            if not isinstance(size, int):
                size = len(payload.get("commits") or [])
            emit(f"commits|{repo}", size)
        lang = language(event)
        if lang:
            emit(f"lang|{lang}")
        created = str(event.get("created_at") or "")
        if len(created) >= 13:
            emit(f"hour|{created[11:13]}")


if __name__ == "__main__":
    main()
