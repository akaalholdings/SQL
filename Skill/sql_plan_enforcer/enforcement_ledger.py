#!/usr/bin/env python3
"""Durable ledger of every plan-enforcement action, each with its exact rollback.

Unlike the sql_optimizer audit corpus (which is best-effort and non-blocking), this ledger
is **safety-critical and fail-closed**: for an autonomous loop, the record of what was forced
*is* the ability to undo it. So:

- Every ``force_plan`` / ``set_hints`` row stores the exact reverting statement
  (``sp_query_store_unforce_plan`` / ``sp_query_store_clear_hints``).
- ``write_ledger`` raises on a bad/invalid record. The caller MUST treat a failed ledger
  write as a hard stop: do not apply (or, if already applied, roll back immediately).
- ``pending_rollbacks`` reconstructs every control still in place from the ledger — the
  panic-button "revert everything we touched" list.

Usage mirrors ``record_audit.py``: write a JSON action document, then::

    python3 enforcement_ledger.py --input /tmp/action.json     # record one action
    python3 enforcement_ledger.py --pending                     # print active rollback SQL

Ledger dir resolution (override wins):
    1. ``$SQL_PLAN_ENFORCER_AUDIT_DIR``
    2. ``~/.copilot/skills/sql_plan_enforcer/audits``
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import secrets
import sys
from datetime import datetime, timezone

LEVERS = ("force_plan", "set_hints", "unforce_plan", "clear_hints")
# Levers that PLACE a control (leave something to roll back) vs levers that REMOVE one.
_APPLY_LEVERS = ("force_plan", "set_hints")
_ROLLBACK_LEVERS = ("unforce_plan", "clear_hints")

OUTCOMES = ("dry_run", "applied", "kept", "rolled_back", "force_failed", "skipped")
MODES = ("apply", "dry_run")

_STR_FIELDS = ("id", "timestamp", "environment", "query_hash", "category", "lever",
               "action_sql", "rollback_sql", "mode", "outcome", "reason", "detail_file")
_DICT_FIELDS = ("baseline_metrics",)

_GITIGNORE = """\
# Enforcement ledger — records exact production plan changes. Never commit.
*
!.gitignore
!README.md
"""

_README = """\
# sql_plan_enforcer ledger

Written by `enforcement_ledger.py` for every plan-enforcement action (forced plan, hint,
and each rollback). **Safety-critical**: this is how a change gets undone.

- `index.jsonl` — one record per action; the queryable layer.
- `runs/<id>.md` — full detail per action (exact apply + rollback SQL, baseline metrics).

Reconstruct everything still in place:

    python3 enforcement_ledger.py --pending

Validate the ledger:

    python3 enforcement_ledger.py --validate index.jsonl
"""


def ledger_dir() -> pathlib.Path:
    override = os.environ.get("SQL_PLAN_ENFORCER_AUDIT_DIR")
    if override:
        return pathlib.Path(override).expanduser()
    return pathlib.Path.home() / ".copilot" / "skills" / "sql_plan_enforcer" / "audits"


def _query_hash(action: dict) -> str:
    """Stable per-query key: the supplied hash, else from query text, else the query_id."""
    explicit = action.get("query_hash")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    text = action.get("query_text")
    if isinstance(text, str) and text.strip():
        normalized = " ".join(text.split()).lower()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"q{action.get('query_id', 'unknown')}"


def validate(record: object) -> list[str]:
    """Return human-readable problems with a ledger record (empty == valid)."""
    errors: list[str] = []
    if not isinstance(record, dict):
        return [f"record must be a JSON object, got {type(record).__name__}"]

    for field in _STR_FIELDS:
        if field not in record:
            errors.append(f"missing required field: {field}")
        elif not isinstance(record[field], str):
            errors.append(f"{field} must be a string")

    if "query_id" not in record:
        errors.append("missing required field: query_id")
    elif not isinstance(record["query_id"], int):
        errors.append("query_id must be an integer")

    # plan_id is required for plan forcing, null otherwise.
    if "plan_id" not in record:
        errors.append("missing required field: plan_id")
    elif record["plan_id"] is not None and not isinstance(record["plan_id"], int):
        errors.append("plan_id must be an integer or null")

    for field in _DICT_FIELDS:
        if field in record and not isinstance(record[field], dict):
            errors.append(f"{field} must be an object")

    if record.get("lever") not in LEVERS:
        errors.append(f"lever must be one of {', '.join(LEVERS)}; got {record.get('lever')!r}")
    if record.get("mode") not in MODES:
        errors.append(f"mode must be one of {', '.join(MODES)}; got {record.get('mode')!r}")
    if record.get("outcome") not in OUTCOMES:
        errors.append(f"outcome must be one of {', '.join(OUTCOMES)}; got {record.get('outcome')!r}")

    # The whole point of the ledger: an applied control must carry a real rollback.
    if record.get("lever") in _APPLY_LEVERS and record.get("outcome") in ("applied", "kept"):
        if not (isinstance(record.get("rollback_sql"), str) and record["rollback_sql"].strip()):
            errors.append("an applied control must include a non-empty rollback_sql")

    if record.get("lever") == "force_plan" and record.get("plan_id") is None:
        errors.append("force_plan requires a plan_id")

    return errors


def build_record(action: dict, *, now: datetime | None = None, nonce: str | None = None) -> dict:
    """Assemble a ledger record from an action document."""
    query_id = action.get("query_id")
    if not isinstance(query_id, int):
        raise ValueError("action must include an integer 'query_id'")
    if action.get("lever") not in LEVERS:
        raise ValueError(f"action 'lever' must be one of {', '.join(LEVERS)}")

    now = now or datetime.now(timezone.utc)
    nonce = nonce or secrets.token_hex(3)
    qhash = _query_hash(action)
    record_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{nonce}__q{query_id}"

    metrics = action.get("baseline_metrics")
    return {
        "id": record_id,
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": str(action.get("environment", "unknown")),
        "query_id": query_id,
        "query_hash": qhash,
        "category": str(action.get("category", "unknown")),
        "lever": action["lever"],
        "plan_id": action.get("plan_id") if isinstance(action.get("plan_id"), int) else None,
        "action_sql": str(action.get("action_sql", "")),
        "rollback_sql": str(action.get("rollback_sql", "")),
        "baseline_metrics": metrics if isinstance(metrics, dict) else {},
        "mode": str(action.get("mode", "dry_run")),
        "outcome": str(action.get("outcome", "dry_run")),
        "reason": str(action.get("reason", "")),
        "detail_file": f"runs/{record_id}.md",
    }


def _fenced(label: str, body: object, lang: str = "sql") -> str:
    text = body if isinstance(body, str) else json.dumps(body, indent=2)
    if not text.strip():
        return f"## {label}\n\n_None recorded._\n"
    return f"## {label}\n\n```{lang}\n{text.rstrip()}\n```\n"


def render_detail(record: dict) -> str:
    frontmatter = json.dumps(record, indent=2, ensure_ascii=False)
    return "\n".join(
        [
            f"---\n{frontmatter}\n---\n",
            f"# Enforcement action `{record['id']}`\n",
            f"- environment: `{record['environment']}`  ·  query_id: `{record['query_id']}`  "
            f"·  lever: `{record['lever']}`  ·  outcome: `{record['outcome']}`\n",
            _fenced("Applied", record["action_sql"]),
            _fenced("Rollback", record["rollback_sql"]),
            _fenced("Baseline metrics", record["baseline_metrics"], lang="json"),
            f"## Reason\n\n{record['reason'] or '_None recorded._'}\n",
        ]
    )


def ensure_corpus(root: pathlib.Path) -> None:
    (root / "runs").mkdir(parents=True, exist_ok=True)
    gitignore = root / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(_GITIGNORE, encoding="utf-8")
    readme = root / "README.md"
    if not readme.exists():
        readme.write_text(_README, encoding="utf-8")


def write_ledger(action: dict, root: pathlib.Path | None = None) -> dict:
    """Validate and persist one action. Raises on invalid input (fail-closed)."""
    root = root or ledger_dir()
    record = build_record(action)

    problems = validate(record)
    if problems:
        raise ValueError("ledger record is invalid: " + "; ".join(problems))

    ensure_corpus(root)
    (root / record["detail_file"]).write_text(render_detail(record), encoding="utf-8")
    with (root / "index.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def pending_rollbacks(records: list) -> list:
    """Reconstruct every control still in place, newest state per (env, query_id, family).

    force_plan/unforce_plan share the "force" family; set_hints/clear_hints share "hints".
    Returns a list of {environment, query_id, lever, rollback_sql} the operator can replay
    to revert everything the enforcer left active.
    """
    def family(lever: str) -> str:
        return "force" if lever in ("force_plan", "unforce_plan") else "hints"

    active: dict[tuple, dict] = {}
    for rec in records:
        if not isinstance(rec, dict) or rec.get("lever") not in LEVERS:
            continue
        key = (rec.get("environment"), rec.get("query_id"), family(rec["lever"]))
        lever = rec["lever"]
        outcome = rec.get("outcome")
        if lever in _APPLY_LEVERS and outcome in ("applied", "kept"):
            active[key] = {
                "environment": rec.get("environment"),
                "query_id": rec.get("query_id"),
                "lever": lever,
                "rollback_sql": rec.get("rollback_sql", ""),
            }
        elif lever in _ROLLBACK_LEVERS or outcome == "rolled_back":
            active.pop(key, None)
    return list(active.values())


def _load_index(root: pathlib.Path) -> list:
    path = root / "index.jsonl"
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Record / inspect plan-enforcement actions.")
    parser.add_argument("--input", "-i", help="Action JSON document, or '-' for stdin.")
    parser.add_argument("--pending", action="store_true", help="Print active rollback SQL and exit.")
    parser.add_argument("--validate", metavar="FILE", help="Validate a ledger index.jsonl and exit.")
    args = parser.parse_args(argv[1:])

    if args.validate:
        bad = 0
        for n, line in enumerate(pathlib.Path(args.validate).read_text("utf-8").splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            problems = validate(json.loads(line))
            for problem in problems:
                bad += 1
                print(f"line {n}: {problem}", file=sys.stderr)
        return 1 if bad else 0

    if args.pending:
        for item in pending_rollbacks(_load_index(ledger_dir())):
            print(f"-- {item['environment']} query_id={item['query_id']} ({item['lever']})")
            print(item["rollback_sql"])
        return 0

    source = args.input or "-"
    raw = sys.stdin.read() if source == "-" else pathlib.Path(source).read_text("utf-8")
    try:
        action = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"could not parse action document: {exc}", file=sys.stderr)
        return 1

    # Fail-closed: a failed ledger write must stop the apply, so we return non-zero.
    record = write_ledger(action)
    print(f"ledger {record['id']} ({record['lever']}/{record['outcome']}) -> {record['detail_file']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
