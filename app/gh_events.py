"""GH Archive event helpers shared by Hadoop MapReduce and Spark engines."""

from __future__ import annotations

from typing import Any, Iterator, Optional


def iter_ndjson_lines(path: str, max_events: Optional[int] = None) -> Iterator[str]:
    with open(path, "r", encoding="utf-8") as handle:
        count = 0
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            yield line
            count += 1
            if max_events is not None and count >= max_events:
                break


def nested_get(obj: Any, *keys: str) -> Any:
    cur = obj
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def extract_language(event: dict[str, Any]) -> Optional[str]:
    payload = event.get("payload") or {}
    language = nested_get(payload, "pull_request", "base", "repo", "language")
    if language:
        return str(language)
    language = nested_get(payload, "pull_request", "head", "repo", "language")
    if language:
        return str(language)
    language = nested_get(payload, "forkee", "language")
    if language:
        return str(language)
    language = nested_get(payload, "repository", "language")
    if language:
        return str(language)
    return None


def extract_commit_count(event: dict[str, Any]) -> int:
    payload = event.get("payload") or {}
    size = payload.get("size")
    if isinstance(size, int):
        return size
    commits = payload.get("commits")
    if isinstance(commits, list):
        return len(commits)
    return 0


def actor_login(event: dict[str, Any]) -> str:
    actor = event.get("actor") or {}
    return str(actor.get("login") or actor.get("display_login") or "unknown")


def repo_name(event: dict[str, Any]) -> str:
    repo = event.get("repo") or {}
    return str(repo.get("name") or "unknown/unknown")


def map_emissions(event: dict[str, Any]) -> list[tuple[str, int]]:
    """Hadoop mapper keys: event-type-per-repo, users, pushes, languages, commits."""
    event_type = str(event.get("type") or "Unknown")
    repo = repo_name(event)
    user = actor_login(event)
    rows: list[tuple[str, int]] = [
        (f"evt|{repo}|{event_type}", 1),
        (f"user|{user}", 1),
        (f"type|{event_type}", 1),
    ]
    if event_type == "PushEvent":
        rows.append((f"push|{repo}", 1))
        rows.append((f"commits|{repo}", extract_commit_count(event)))
    language = extract_language(event)
    if language:
        rows.append((f"lang|{language}", 1))
    created = str(event.get("created_at") or "")
    if len(created) >= 13:
        rows.append((f"hour|{created[11:13]}", 1))
    return rows


def schema_type_name(value: Any) -> Any:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        inner = schema_type_name(value[0]) if value else "null"
        return {"type": "array", "element": inner}
    if isinstance(value, dict):
        return {
            "type": "struct",
            "fields": {key: schema_type_name(val) for key, val in value.items()},
        }
    return "string"


def merge_schema(left: Any, right: Any) -> Any:
    if left == "null":
        return right
    if right == "null":
        return left
    if left == right:
        return left
    if isinstance(left, dict) and isinstance(right, dict):
        if left.get("type") == "struct" and right.get("type") == "struct":
            keys = set(left["fields"]) | set(right["fields"])
            return {
                "type": "struct",
                "fields": {
                    key: merge_schema(
                        left["fields"].get(key, "null"),
                        right["fields"].get(key, "null"),
                    )
                    for key in sorted(keys)
                },
            }
        if left.get("type") == "array" and right.get("type") == "array":
            return {
                "type": "array",
                "element": merge_schema(left.get("element"), right.get("element")),
            }
    return "string"


def infer_schema_tree(records: list[dict[str, Any]]) -> Any:
    tree: Any = "null"
    for record in records:
        tree = merge_schema(tree, schema_type_name(record))
    return tree


def format_schema_tree(tree: Any, name: str = "root", indent: int = 0) -> str:
    pad = " |-- " * indent if indent else ""
    if indent == 0:
        lines = ["root"]
        if isinstance(tree, dict) and tree.get("type") == "struct":
            for key, val in tree["fields"].items():
                lines.append(format_schema_tree(val, key, 1))
        return "\n".join(lines)
    if isinstance(tree, dict) and tree.get("type") == "struct":
        lines = [f" |-- {name}: struct (nullable = true)"]
        for key, val in tree["fields"].items():
            lines.append(format_schema_tree(val, key, indent + 1))
        return "\n".join(lines)
    if isinstance(tree, dict) and tree.get("type") == "array":
        elem = tree.get("element")
        if isinstance(elem, dict) and elem.get("type") == "struct":
            lines = [f"{' |   ' * (indent - 1)} |-- {name}: array (nullable = true)"]
            lines.append(format_schema_tree(elem, "element", indent + 1))
            return "\n".join(lines)
        return f"{' |   ' * (indent - 1)} |-- {name}: array<{elem}> (nullable = true)"
    return f"{' |   ' * (indent - 1)} |-- {name}: {tree} (nullable = true)"


def count_schema_fields(tree: Any) -> int:
    if isinstance(tree, dict) and tree.get("type") == "struct":
        return sum(count_schema_fields(val) for val in tree["fields"].values()) + len(
            tree["fields"]
        )
    if isinstance(tree, dict) and tree.get("type") == "array":
        return 1 + count_schema_fields(tree.get("element"))
    return 1
