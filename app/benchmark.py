"""Run Hadoop vs Spark, compute schema metrics, speedup, and Catalyst narrative."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from app import RESULTS_DIR
from app.hadoop_engine import run_mapreduce
from app.spark_engine import run_spark

ProgressFn = Optional[Callable[[dict[str, Any]], None]]


def _speedup(hadoop_s: float, spark_s: float) -> float:
    if spark_s <= 0:
        return 0.0
    return round(hadoop_s / spark_s, 2)


def catalyst_paragraph(result: dict[str, Any], lang: str = "en") -> str:
    h = result["hadoop"]
    s = result["spark"]
    speedup = result["speedup"]
    backend = s.get("backend") or "pyspark"
    schema_fields = s.get("fields_discovered") or 0
    spark_faster = speedup >= 1
    if lang == "km":
        spark_label = (
            "spark.read.json() ដើម"
            if backend == "pyspark"
            else "ផ្លូវ infer schema បែប DataFrame ដែលតាមគំរូ spark.read.json()"
        )
        if spark_faster:
            lead = (
                f"Spark បានបញ្ចប់ការវិភាគ GH Archive ជាន់គ្នា លឿនជាង Hadoop MapReduce "
                f"{speedup:.2f}× ({s['total_s']:.3f}s ធៀបនឹង {h['total_s']:.3f}s)។ "
            )
        else:
            ratio = round(s["total_s"] / max(h["total_s"], 1e-9), 2)
            lead = (
                f"នៅលើម៉ាស៊ីននេះ Hadoop លឿនជាង Spark {ratio:.2f}× "
                f"(Hadoop {h['total_s']:.3f}s vs Spark {s['total_s']:.3f}s) — "
                f"speedup = Hadoop÷Spark = {speedup:.2f}× (< 1 មានន័យថា Spark យឺតជាង)។ "
                f"ជាញឹកញាប់កើតឡើងលើ server តូច (Docker/VPS) ព្រោះ JVM + schema infer ចំណាយច្រើន។ "
            )
        return (
            lead
            + f"Hadoop ប្រើពេល {h['parse_s']:.3f}s ក្នុង json.loads តាមកំណត់ត្រា — "
            f"គ្មាន schema រួម ដូច្នេះ mapper នីមួយៗត្រូវចំណាយ tokenizer, "
            f"ការបង្កើត object និងការដើរលើ dict ម្ដងទៀត។ Spark ប្រើ {spark_label} "
            f"រកបាន {schema_fields} វាលជាន់គ្នាក្នុង {s['schema_s']:.3f}s រួចប្រើ "
            f"struct (actor, repo, payload) សម្រាប់ការងារនៅសល់។ Catalyst Optimizer "
            f"បម្លែងតម្រង type == 'PushEvent' និង groupBy ទៅជាផែនការតែមួយ៖ "
            f"predicate pushdown, projection pruning និង Tungsten codegen "
            f"ដើម្បីរក្សាជួរក្នុងអង្គចងចាំជាជួរឈរ។ លើ dataset ធំជាង "
            f"តម្លៃ parse របស់ Hadoop កើនលីនេអ៊ែរ ខណៈ Spark រក schema ម្តងរួចប្រើឡើងវិញ។"
        )
    spark_label = (
        "native spark.read.json()"
        if backend == "pyspark"
        else "a DataFrame schema-inference path modeled on spark.read.json()"
    )
    if spark_faster:
        lead = (
            f"Spark finished nested GitHub Archive profiling {speedup:.2f}× faster than "
            f"Hadoop MapReduce ({s['total_s']:.3f}s processing vs {h['total_s']:.3f}s). "
        )
    else:
        ratio = round(s["total_s"] / max(h["total_s"], 1e-9), 2)
        lead = (
            f"On this host Hadoop finished {ratio:.2f}× faster than Spark "
            f"(Hadoop {h['total_s']:.3f}s vs Spark {s['total_s']:.3f}s) — "
            f"speedup = Hadoop÷Spark = {speedup:.2f}× (values under 1× mean Spark was slower). "
            f"This often happens on small Docker/VPS hosts where JVM startup and nested "
            f"schema inference dominate. "
        )
    return (
        lead
        + f"Hadoop spent {h['parse_s']:.3f}s inside per-record Python json.loads — "
        f"there is no shared schema, so every mapper pays tokenizer, object-allocation, "
        f"and dict-walk costs again. Spark used {spark_label} and discovered "
        f"{schema_fields} nested fields in {s['schema_s']:.3f}s, then reused that "
        f"struct type (actor, repo, payload) for the rest of the job. The Catalyst "
        f"Optimizer turns DataFrame filters such as type == 'PushEvent' and the "
        f"groupBy rankings into a single physical plan: predicate pushdown, projection "
        f"pruning of unused payload fields, and Tungsten whole-stage codegen so rows "
        f"stay in columnar memory instead of Python objects. On larger dumps Hadoop's "
        f"parse cost grows linearly, while Spark reuses the inferred schema."
    )


def names_match(left: list[dict[str, Any]], right: list[dict[str, Any]], n: int = 5) -> bool:
    a = [row["name"] for row in left[:n]]
    b = [row["name"] for row in right[:n]]
    return a == b


def run_comparison(
    path: str,
    max_events: Optional[int] = None,
    progress: ProgressFn = None,
) -> dict[str, Any]:
    hadoop = run_mapreduce(path, max_events=max_events, progress=progress)
    spark = run_spark(path, max_events=max_events, progress=progress)

    h_agg = hadoop["aggregates"]
    s_agg = spark["aggregates"]
    speedup = _speedup(hadoop["total_s"], spark["total_s"])
    schema_ratio = _speedup(hadoop["parse_s"], max(spark["schema_s"], 1e-9))

    result: dict[str, Any] = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "input": path,
        "max_events": max_events,
        "hadoop": {k: v for k, v in hadoop.items() if k != "aggregates"},
        "spark": {k: v for k, v in spark.items() if k != "aggregates"},
        "hadoop_aggregates": h_agg,
        "spark_aggregates": s_agg,
        "charts": {
            "top_repos": s_agg["top_repos"][:15] or h_agg["top_repos"][:15],
            "top_users": s_agg["top_users"][:15] or h_agg["top_users"][:15],
            "top_push_repos": s_agg.get("top_push_repos") or h_agg.get("top_push_repos"),
            "languages": s_agg.get("languages") or h_agg.get("languages"),
            "event_types": s_agg.get("event_types") or h_agg.get("event_types"),
            "hours": s_agg.get("hours") or h_agg.get("hours"),
        },
        "speedup": speedup,
        "schema_discovery": {
            "hadoop_parse_s": hadoop["parse_s"],
            "hadoop_manual_schema_s": hadoop["manual_schema_s"],
            "spark_schema_s": spark["schema_s"],
            "spark_query_s": spark["query_s"],
            "spark_session_s": spark.get("session_s") or 0.0,
            "fields_discovered": spark.get("fields_discovered") or 0,
            "schema_text": spark.get("schema_text") or "",
            "parse_vs_infer_speedup": schema_ratio,
        },
        "agreement": {
            "top_repos": names_match(h_agg["top_repos"], s_agg["top_repos"]),
            "top_users": names_match(h_agg["top_users"], s_agg["top_users"]),
        },
    }
    result["narrative"] = catalyst_paragraph(result, "en")
    result["narrative_km"] = catalyst_paragraph(result, "km")
    result["scorecard"] = {
        "winner": "Spark" if speedup >= 1 else "Hadoop",
        "speedup": speedup,
        "hadoop_s": round(hadoop["total_s"], 4),
        "spark_s": round(spark["total_s"], 4),
        "hadoop_parse_s": round(hadoop["parse_s"], 4),
        "spark_schema_s": round(spark["schema_s"], 4),
        "records": spark.get("records") or hadoop.get("records"),
        "backend": spark.get("backend"),
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / "last_comparison.json"
    serializable = json.loads(json.dumps(result, default=str))
    out.write_text(json.dumps(serializable, indent=2), encoding="utf-8")
    if progress:
        winner = "Spark" if speedup >= 1 else "Hadoop"
        progress(
            {
                "stage": "scorecard",
                "status": (
                    f"{winner} wins — Hadoop {hadoop['total_s']:.3f}s ÷ "
                    f"Spark {spark['total_s']:.3f}s = {speedup:.2f}×"
                ),
            }
        )
    return serializable
