# Architecture — GH Archive Hadoop vs Spark

```
GH Archive hourly NDJSON (gzip)
        │
        ▼
  gh_activity_full.json
        │
        ▼
  HDFS /data/github/          (hdfs dfs -put, or data/hdfs/data/github/)
        │
        ├──────────────────┐
        ▼                  ▼
 Hadoop Streaming      spark.read.json()
 json.loads / line     infer nested schema
 map · shuffle · reduce   Catalyst physical plan
 evt|repo|type            filter type == PushEvent
 user|login               groupBy repo.name / actor.login
 lang|language            coalesce nested language fields
        │                  │
        └────────┬─────────┘
                 ▼
     Schema timings + speedup
     Top 15 repos / users charts
     Catalyst narrative
```

## Event fields used

| Path | Use |
|------|-----|
| `type` | Event mix; PushEvent filter |
| `actor.login` | Contributor ranking |
| `repo.name` | Project ranking |
| `payload.size` / `payload.commits` | Commit volume per repo |
| `payload.pull_request.base.repo.language` | Language (PRs) |
| `payload.forkee.language` | Language (forks) |
| `payload.repository.language` | Language (issues / create) |
| `created_at` | Hourly activity |

## Fair timing

- **Hadoop total** = map (including `json.loads`) + shuffle + reduce.
- **Spark process** = schema inference (`read.json` + first action) + query. JVM session start is reported separately so a cold Spark launch is not hidden, but speedup uses processing time.
- Charts prefer Spark aggregates; Hadoop rankings are kept to check agreement on the top keys.
