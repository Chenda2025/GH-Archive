# Project 6: GitHub Open-Source Archive Activity & Language Profiling

Process nested GH Archive JSON to compute repository commit patterns, user activity ranking, and language distributions — and show why Spark’s schema-aware JSON path is faster than Hadoop MapReduce.

## What this project covers

| # | Requirement | Where it lives |
|---|-------------|----------------|
| 1 | **HDFS pipeline** — `gh_activity_full.json` → `/data/github/` | `scripts/hdfs_upload.sh`, UI **Upload to HDFS**, `app/hdfs_pipeline.py` |
| 2 | **Hadoop MapReduce** — `json.loads` per line; event types per repo; top users | `hadoop/mapper.py`, `hadoop/reducer.py`, `app/hadoop_engine.py` |
| 3 | **Apache Spark** — `spark.read.json()`, `type == 'PushEvent'`, rank projects | `spark/github_profile.py`, `app/spark_engine.py` |
| 4 | **Web benchmark UI** — preview table + **Start Engine Comparison** | `http://127.0.0.1:8000` |
| 5 | **Schema discovery metric** — Spark infer/query vs Hadoop parse overhead | Scorecards on the dashboard |
| 6 | **Activity charts** — top 15 repos and contributors side-by-side | Chart.js after the run |
| 7 | **Efficiency scorecard** — speedup + Catalyst Optimizer paragraph | Generated automatically |

## Quick start

```bash
cd hadoop-apack
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./run.sh
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000)

1. **Download dataset** (nested sample, or a live GH Archive hour).
2. **Upload to HDFS** (`/data/github/gh_activity_full.json`; local layout if `hdfs` is not installed).
3. Click **Start Engine Comparison**.
4. Read schema timings, charts, and the Catalyst scorecard.

PySpark needs a JDK (Java 17 is fine). If the JVM session cannot start, the app falls back to a DataFrame schema-inference path so the UI still completes.

## GH Archive download (assignment recipe)

Full day (large — many GB uncompressed):

```bash
chmod +x scripts/download_gharchive.sh scripts/hdfs_upload.sh
./scripts/download_gharchive.sh 2023-03-15
./scripts/hdfs_upload.sh
```

The script pulls 24 hourly dumps from [GH Archive](https://www.gharchive.org/), concatenates them to `gh_activity_full.json`, and checks that records contain `type`, `actor`, `repo`, and `payload`.

The web app can also stream **one hour** and keep the first *N* events so a laptop demo stays interactive.

## Cluster-style jobs

Hadoop Streaming (uses `hadoop` if present, otherwise `mapper | sort | reducer`):

```bash
chmod +x hadoop/run_streaming.sh
./hadoop/run_streaming.sh data/hdfs/data/github/gh_activity_full.json
```

Spark:

```bash
spark-submit spark/github_profile.py \
  --input data/hdfs/data/github/gh_activity_full.json \
  --output data/results/spark_profile
```

On a real cluster, point `--input` at `hdfs:///data/github/gh_activity_full.json`.

## Why Spark wins on nested JSON

Hadoop mappers treat each line as an opaque string and run `json.loads` in Python — no reused schema, lots of object allocation, then a shuffle.

Spark infers a nested struct (`actor`, `repo`, `payload`, …) once. Catalyst then:

- pushes `type == 'PushEvent'` down into the scan
- prunes unused payload fields
- runs aggregations in Tungsten (columnar, whole-stage codegen)

The UI prints that explanation with the measured speedup after every run.

## Layout

```
app/                 FastAPI UI, engines, HDFS helper
hadoop/              Streaming mapper / reducer
spark/               spark.read.json() job
scripts/             24-hour download + HDFS put
data/raw/            gh_activity_full.json
data/hdfs/data/github/   simulated HDFS layout
docs/                architecture notes
```
