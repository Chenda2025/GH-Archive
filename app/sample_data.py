"""Generate GH Archive-shaped nested JSON when the public dump is unavailable."""

from __future__ import annotations

import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPOS = [
    ("apache/spark", "Scala"),
    ("apache/hadoop", "Java"),
    ("apache/kafka", "Java"),
    ("apache/flink", "Java"),
    ("tensorflow/tensorflow", "C++"),
    ("pytorch/pytorch", "Python"),
    ("scikit-learn/scikit-learn", "Python"),
    ("pandas-dev/pandas", "Python"),
    ("microsoft/vscode", "TypeScript"),
    ("facebook/react", "JavaScript"),
    ("vercel/next.js", "JavaScript"),
    ("nodejs/node", "JavaScript"),
    ("golang/go", "Go"),
    ("rust-lang/rust", "Rust"),
    ("kubernetes/kubernetes", "Go"),
    ("helm/helm", "Go"),
    ("hashicorp/terraform", "Go"),
    ("elastic/elasticsearch", "Java"),
    ("grafana/grafana", "TypeScript"),
    ("prometheus/prometheus", "Go"),
    ("django/django", "Python"),
    ("pallets/flask", "Python"),
    ("spring-projects/spring-boot", "Java"),
    ("laravel/laravel", "PHP"),
    ("rails/rails", "Ruby"),
    ("denoland/deno", "Rust"),
    ("oven-sh/bun", "Zig"),
    ("tauri-apps/tauri", "Rust"),
    ("openai/whisper", "Python"),
    ("huggingface/transformers", "Python"),
]

USERS = [
    "octocat", "torvalds", "gaearon", "yyx990803", "thepracticaldev",
    "sindresorhus", "tj", "addyosmani", "kentcdodds", "wesbos",
    "jakewharton", "defunkt", "mojombo", "wycats", "dhh",
    "matz", "antirez", "mitsuhiko", "jashkenas", "ahejlsberg",
    "brendaneich", "gvanrossum", "jessfraz", "kelseyhightower", "lizrice",
    "bcantrill", "richhickey", "mpj", "swyx", "swyxio",
]

EVENT_TYPES = [
    ("PushEvent", 0.38),
    ("PullRequestEvent", 0.14),
    ("IssuesEvent", 0.10),
    ("IssueCommentEvent", 0.09),
    ("WatchEvent", 0.08),
    ("ForkEvent", 0.06),
    ("CreateEvent", 0.05),
    ("DeleteEvent", 0.03),
    ("PullRequestReviewEvent", 0.04),
    ("ReleaseEvent", 0.03),
]


def _pick_type(rng: random.Random) -> str:
    roll = rng.random()
    acc = 0.0
    for name, weight in EVENT_TYPES:
        acc += weight
        if roll <= acc:
            return name
    return "PushEvent"


def _sha(seed: str) -> str:
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _commit(rng: random.Random, repo: str, idx: int) -> dict[str, Any]:
    verbs = ["fix", "feat", "docs", "refactor", "test", "chore", "perf"]
    scopes = ["core", "ui", "api", "build", "sql", "json", "hdfs", "spark"]
    return {
        "sha": _sha(f"{repo}:{idx}:{rng.random()}"),
        "author": {
            "email": f"dev{rng.randint(1, 80)}@users.noreply.github.com",
            "name": rng.choice(USERS),
        },
        "message": f"{rng.choice(verbs)}({rng.choice(scopes)}): {rng.choice(['tune parser', 'avoid N+1', 'guard null payload', 'rank active users', 'infer nested schema'])}",
        "distinct": True,
        "url": f"https://api.github.com/repos/{repo}/commits/{_sha(f'{repo}:{idx}')}",
    }


def _payload(rng: random.Random, event_type: str, repo: str, language: str) -> dict[str, Any]:
    if event_type == "PushEvent":
        n = rng.randint(1, 6)
        return {
            "push_id": rng.randint(10_000_000, 99_000_000),
            "size": n,
            "distinct_size": n,
            "ref": rng.choice(["refs/heads/main", "refs/heads/master", "refs/heads/develop"]),
            "head": _sha(f"head:{repo}:{rng.random()}"),
            "before": _sha(f"before:{repo}:{rng.random()}"),
            "commits": [_commit(rng, repo, i) for i in range(n)],
        }
    if event_type == "PullRequestEvent":
        return {
            "action": rng.choice(["opened", "closed", "reopened", "synchronize"]),
            "number": rng.randint(1, 8000),
            "pull_request": {
                "id": rng.randint(1_000_000, 9_000_000),
                "state": rng.choice(["open", "closed"]),
                "title": f"Improve {language} JSON parsing path",
                "base": {
                    "ref": "main",
                    "repo": {
                        "name": repo.split("/")[-1],
                        "full_name": repo,
                        "language": language,
                    },
                },
                "head": {
                    "ref": f"feature/{rng.randint(1, 99)}",
                    "repo": {
                        "name": repo.split("/")[-1],
                        "full_name": repo,
                        "language": language,
                    },
                },
            },
        }
    if event_type == "ForkEvent":
        return {
            "forkee": {
                "id": rng.randint(1_000_000, 9_000_000),
                "full_name": f"{rng.choice(USERS)}/{repo.split('/')[-1]}",
                "fork": True,
                "language": language,
            }
        }
    if event_type in {"IssuesEvent", "IssueCommentEvent"}:
        return {
            "action": rng.choice(["opened", "closed", "created"]),
            "issue": {
                "number": rng.randint(1, 4000),
                "title": "Nested payload parse failure on large dumps",
                "state": rng.choice(["open", "closed"]),
            },
            "repository": {"full_name": repo, "language": language},
        }
    if event_type == "ReleaseEvent":
        return {
            "action": "published",
            "release": {
                "tag_name": f"v{rng.randint(1, 5)}.{rng.randint(0, 20)}.{rng.randint(0, 9)}",
                "name": f"{repo.split('/')[-1]} release",
            },
            "repository": {"full_name": repo, "language": language},
        }
    return {
        "ref": rng.choice(["main", "feature/json", None]),
        "ref_type": rng.choice(["branch", "tag", "repository"]),
        "repository": {"full_name": repo, "language": language},
    }


def generate_events(n: int = 8000, seed: int = 42, day: str = "2023-03-15") -> list[dict[str, Any]]:
    rng = random.Random(seed)
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    # Zipf-like repo popularity
    weights = [1.0 / (i + 1) ** 0.85 for i in range(len(REPOS))]
    user_w = [1.0 / (i + 1) ** 0.7 for i in range(len(USERS))]
    events: list[dict[str, Any]] = []
    for i in range(n):
        repo, language = rng.choices(REPOS, weights=weights, k=1)[0]
        user = rng.choices(USERS, weights=user_w, k=1)[0]
        event_type = _pick_type(rng)
        created = start + timedelta(seconds=rng.randint(0, 24 * 3600 - 1))
        events.append(
            {
                "id": str(10_000_000_000_000 + i),
                "type": event_type,
                "actor": {
                    "id": 1000 + USERS.index(user),
                    "login": user,
                    "display_login": user,
                    "gravatar_id": "",
                    "url": f"https://api.github.com/users/{user}",
                    "avatar_url": f"https://avatars.githubusercontent.com/{user}",
                },
                "repo": {
                    "id": 5000 + REPOS.index((repo, language)),
                    "name": repo,
                    "url": f"https://api.github.com/repos/{repo}",
                },
                "payload": _payload(rng, event_type, repo, language),
                "public": True,
                "created_at": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )
    events.sort(key=lambda e: e["created_at"])
    return events


def write_ndjson(path: Path, events: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")
    return path
