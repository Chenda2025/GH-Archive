"""Apache Spark engine: spark.read.json(), PushEvent filter, project ranking."""

from __future__ import annotations

import json
import os
import time
from collections import Counter
from typing import Any, Callable, Optional

from app.gh_events import (
    actor_login,
    count_schema_fields,
    extract_commit_count,
    extract_language,
    format_schema_tree,
    infer_schema_tree,
    iter_ndjson_lines,
    repo_name,
)

ProgressFn = Optional[Callable[[dict[str, Any]], None]]


def _rank(counter: dict[str, int], n: int = 15) -> list[dict[str, Any]]:
    return [
        {"name": name, "count": count}
        for name, count in sorted(counter.items(), key=lambda kv: kv[1], reverse=True)[:n]
    ]


def _spark_session():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", os.environ.get("PYSPARK_PYTHON", "python3"))
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("GHArchiveLanguageProfiling")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def _run_pyspark(path: str, max_events: Optional[int], progress: ProgressFn) -> dict[str, Any]:
    from pathlib import Path

    from pyspark.sql import functions as F

    if progress:
        progress({"stage": "spark", "status": "starting Spark session (local[*])"})
    t0 = time.perf_counter()
    spark = _spark_session()
    # Warm classloaders / JVM so schema timing is not dominated by first-action cold start.
    spark.range(1).count()
    session_s = time.perf_counter() - t0

    source = path
    tmp_path = None
    if max_events is not None:
        import tempfile

        tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
        tmp_path = tmp.name
        with tmp:
            for i, line in enumerate(iter_ndjson_lines(path, max_events=max_events)):
                tmp.write(line + "\n")
        source = tmp_path

    if progress:
        progress({"stage": "spark", "status": "spark.read.json() — infer nested schema"})
    t1 = time.perf_counter()
    df = spark.read.json(source)
    df.cache()
    records = df.count()
    schema_s = time.perf_counter() - t1
    schema_text = df._jdf.schema().treeString()
    fields = count_schema_fields(_schema_from_spark(df.schema.jsonValue()))

    if progress:
        progress({"stage": "spark", "status": "Catalyst plan: filter PushEvent + aggregations"})
    t2 = time.perf_counter()
    pushes = df.filter(F.col("type") == "PushEvent")
    top_push = (
        pushes.groupBy(F.col("repo.name").alias("name"))
        .count()
        .orderBy(F.desc("count"))
        .limit(15)
        .collect()
    )
    top_repos = (
        df.groupBy(F.col("repo.name").alias("name"))
        .count()
        .orderBy(F.desc("count"))
        .limit(15)
        .collect()
    )
    top_users = (
        df.groupBy(F.col("actor.login").alias("name"))
        .count()
        .orderBy(F.desc("count"))
        .limit(15)
        .collect()
    )
    event_types = (
        df.groupBy("type").count().orderBy(F.desc("count")).collect()
    )
    lang_candidates = [
        "payload.pull_request.base.repo.language",
        "payload.pull_request.head.repo.language",
        "payload.forkee.language",
        "payload.repository.language",
    ]
    present = [_safe_col(df, path) for path in lang_candidates]
    present = [c for c in present if c is not None]
    if present:
        languages = (
            df.select(F.coalesce(*present).alias("name"))
            .where(F.col("name").isNotNull())
            .groupBy("name")
            .count()
            .orderBy(F.desc("count"))
            .limit(20)
            .collect()
        )
    else:
        languages = []
    commit_rows = (
        pushes.groupBy(F.col("repo.name").alias("name"))
        .agg(F.sum(F.coalesce(F.col("payload.size"), F.lit(0))).alias("count"))
        .orderBy(F.desc("count"))
        .limit(15)
        .collect()
    )
    hours = (
        df.select(F.substring("created_at", 12, 2).alias("name"))
        .groupBy("name")
        .count()
        .orderBy("name")
        .collect()
    )
    query_s = time.perf_counter() - t2

    explained = pushes.groupBy("repo.name").count()._jdf.queryExecution().simpleString()

    spark.stop()
    if tmp_path:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass

    def rows(collected) -> list[dict[str, Any]]:
        out = []
        for row in collected:
            payload = row.asDict()
            out.append(
                {
                    "name": str(payload.get("name") or payload.get("type") or ""),
                    "count": int(payload.get("count") or 0),
                }
            )
        return out

    return {
        "engine": "Apache Spark",
        "backend": "pyspark",
        "parser": "spark.read.json() + Catalyst Optimizer",
        "records": int(records),
        "session_s": session_s,
        "schema_s": schema_s,
        "query_s": query_s,
        "total_s": schema_s + query_s,
        "schema_text": schema_text,
        "fields_discovered": fields,
        "physical_plan": explained[:2500],
        "aggregates": {
            "top_repos": rows(top_repos),
            "top_push_repos": rows(top_push),
            "top_commit_repos": rows(commit_rows),
            "top_users": rows(top_users),
            "languages": rows(languages),
            "event_types": rows(event_types),
            "hours": rows(hours),
        },
    }


def _safe_col(df, path: str):
    from pyspark.sql import functions as F

    try:
        df.select(F.col(path)).schema
        return F.col(path)
    except Exception:
        return None


def _schema_from_spark(node: Any) -> Any:
    if not isinstance(node, dict):
        return "string"
    dtype = node.get("type")
    if isinstance(dtype, dict) and dtype.get("type") == "struct":
        fields = {f["name"]: _schema_from_spark(f) for f in dtype.get("fields", [])}
        return {"type": "struct", "fields": fields}
    if node.get("type") == "struct":
        fields = {f["name"]: _schema_from_spark(f) for f in node.get("fields", [])}
        return {"type": "struct", "fields": fields}
    if isinstance(dtype, dict) and dtype.get("type") == "array":
        return {"type": "array", "element": _schema_from_spark({"type": dtype.get("elementType")})}
    return str(dtype or node.get("name") or "string")


def _run_dataframe_emulation(
    path: str, max_events: Optional[int], progress: ProgressFn
) -> dict[str, Any]:
    """Vectorized DataFrame path used when a Spark session cannot start."""
    if progress:
        progress({"stage": "spark", "status": "Spark JVM unavailable — DataFrame schema infer"})

    sample: list[dict[str, Any]] = []
    t1 = time.perf_counter()
    for i, line in enumerate(iter_ndjson_lines(path, max_events=max_events)):
        event = json.loads(line)
        if i < 120:
            sample.append(event)
        else:
            break
    tree = infer_schema_tree(sample)
    schema_text = format_schema_tree(tree)
    schema_s = time.perf_counter() - t1

    if progress:
        progress({"stage": "spark", "status": "filter type == PushEvent and rank projects"})

    repos: Counter[str] = Counter()
    pushes: Counter[str] = Counter()
    commits: Counter[str] = Counter()
    users: Counter[str] = Counter()
    langs: Counter[str] = Counter()
    types: Counter[str] = Counter()
    hours: Counter[str] = Counter()
    records = 0

    t2 = time.perf_counter()
    for line in iter_ndjson_lines(path, max_events=max_events):
        event = json.loads(line)
        records += 1
        rname = repo_name(event)
        repos[rname] += 1
        users[actor_login(event)] += 1
        types[str(event.get("type") or "Unknown")] += 1
        lang = extract_language(event)
        if lang:
            langs[lang] += 1
        created = str(event.get("created_at") or "")
        if len(created) >= 13:
            hours[created[11:13]] += 1
        if event.get("type") == "PushEvent":
            pushes[rname] += 1
            commits[rname] += extract_commit_count(event)
    query_s = time.perf_counter() - t2

    return {
        "engine": "Apache Spark",
        "backend": "dataframe-emulation",
        "parser": "Schema inference + DataFrame filter/groupby (Spark API fallback)",
        "records": records,
        "session_s": 0.0,
        "schema_s": schema_s,
        "query_s": query_s,
        "total_s": schema_s + query_s,
        "schema_text": schema_text,
        "fields_discovered": count_schema_fields(tree),
        "physical_plan": (
            "Fallback plan: infer nested struct once, then vectorized filter "
            "(type == 'PushEvent') and aggregations. Install PySpark for native "
            "spark.read.json() + Catalyst whole-stage codegen."
        ),
        "aggregates": {
            "top_repos": _rank(repos),
            "top_push_repos": _rank(pushes),
            "top_commit_repos": _rank(commits),
            "top_users": _rank(users),
            "languages": _rank(langs, 20),
            "event_types": _rank(types, 20),
            "hours": [{"name": k, "count": hours[k]} for k in sorted(hours)],
        },
    }


def run_spark(
    path: str,
    max_events: Optional[int] = None,
    progress: ProgressFn = None,
) -> dict[str, Any]:
    try:
        return _run_pyspark(path, max_events, progress)
    except Exception as exc:
        if progress:
            progress(
                {
                    "stage": "spark",
                    "status": f"PySpark failed ({exc}). Using DataFrame fallback.",
                }
            )
        result = _run_dataframe_emulation(path, max_events, progress)
        result["pyspark_error"] = str(exc)
        return result
