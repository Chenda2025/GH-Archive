"""Hadoop MapReduce engine: per-record json.loads, map, shuffle, reduce."""

from __future__ import annotations

import json
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Optional

from app.gh_events import infer_schema_tree, iter_ndjson_lines, map_emissions

ProgressFn = Optional[Callable[[dict[str, Any]], None]]


def run_mapreduce(
    path: str,
    max_events: Optional[int] = None,
    progress: ProgressFn = None,
) -> dict[str, Any]:
    malformed = 0
    records = 0
    parse_s = 0.0
    emissions = 0
    sample_for_schema: list[dict[str, Any]] = []

    if progress:
        progress({"stage": "hadoop", "status": "map — json.loads each line"})

    map_file = tempfile.NamedTemporaryFile("w+", suffix=".map", delete=False, encoding="utf-8")
    map_path = Path(map_file.name)
    t_map = time.perf_counter()
    try:
        for line in iter_ndjson_lines(path, max_events=max_events):
            t0 = time.perf_counter()
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                parse_s += time.perf_counter() - t0
                malformed += 1
                continue
            parse_s += time.perf_counter() - t0
            records += 1
            if len(sample_for_schema) < 80:
                sample_for_schema.append(event)
            for key, value in map_emissions(event):
                map_file.write(f"{key}\t{value}\n")
                emissions += 1
        map_file.flush()
    finally:
        map_file.close()
    map_s = time.perf_counter() - t_map

    if progress:
        progress({"stage": "hadoop", "status": f"shuffle {emissions:,} pairs (sort spill)"})

    t_shuffle = time.perf_counter()
    sorted_path = map_path.with_suffix(".sorted")
    from subprocess import run

    run(["sort", "-o", str(sorted_path), str(map_path)], check=True)
    shuffle_s = time.perf_counter() - t_shuffle

    if progress:
        progress({"stage": "hadoop", "status": "reduce — sum by key"})

    t_reduce = time.perf_counter()
    reduced: dict[str, int] = {}
    with sorted_path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            key, _, value = raw.rstrip("\n").partition("\t")
            reduced[key] = reduced.get(key, 0) + int(value or "1")
    reduce_s = time.perf_counter() - t_reduce
    map_path.unlink(missing_ok=True)
    sorted_path.unlink(missing_ok=True)

    t_schema = time.perf_counter()
    schema_tree = infer_schema_tree(sample_for_schema)
    manual_schema_s = time.perf_counter() - t_schema

    return {
        "engine": "Hadoop MapReduce",
        "parser": "Python json.loads (per record)",
        "records": records,
        "malformed": malformed,
        "emissions": emissions,
        "keys": len(reduced),
        "map_s": map_s,
        "shuffle_s": shuffle_s,
        "reduce_s": reduce_s,
        "parse_s": parse_s,
        "manual_schema_s": manual_schema_s,
        "total_s": map_s + shuffle_s + reduce_s,
        "aggregates": _to_rankings(reduced),
        "schema_note": "No native schema — types discovered only while parsing strings.",
        "schema_tree": schema_tree,
    }


def _to_rankings(reduced: dict[str, int]) -> dict[str, Any]:
    repos: dict[str, int] = defaultdict(int)
    pushes: dict[str, int] = defaultdict(int)
    commits: dict[str, int] = defaultdict(int)
    users: dict[str, int] = defaultdict(int)
    langs: dict[str, int] = defaultdict(int)
    types: dict[str, int] = defaultdict(int)
    hours: dict[str, int] = defaultdict(int)
    repo_events: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for key, value in reduced.items():
        kind, _, rest = key.partition("|")
        if kind == "evt":
            repo, _, event_type = rest.partition("|")
            repos[repo] += value
            repo_events[repo][event_type] += value
        elif kind == "push":
            pushes[rest] += value
        elif kind == "commits":
            commits[rest] += value
        elif kind == "user":
            users[rest] += value
        elif kind == "lang":
            langs[rest] += value
        elif kind == "type":
            types[rest] += value
        elif kind == "hour":
            hours[rest] += value

    def top(d: dict[str, int], n: int = 15) -> list[dict[str, Any]]:
        items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
        return [{"name": name, "count": count} for name, count in items]

    return {
        "top_repos": top(repos),
        "top_push_repos": top(pushes),
        "top_commit_repos": top(commits),
        "top_users": top(users),
        "languages": top(langs, 20),
        "event_types": top(types, 20),
        "hours": [{"name": k, "count": hours[k]} for k in sorted(hours)],
        "repo_event_matrix": {
            repo: dict(types_map)
            for repo, types_map in list(sorted(repo_events.items(), key=lambda kv: sum(kv[1].values()), reverse=True))[:15]
        },
    }
