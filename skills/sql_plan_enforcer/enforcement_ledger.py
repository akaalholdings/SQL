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
    2. ``~/.copilot/skills/sql_plan_enforcer/audits`` — legacy location, used only when it
       already exists (so an established ledger keeps working after a host switch)
    3. ``~/.sql-skills/sql_plan_enforcer/audits`` — host-neutral default
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import re
import secrets
import sys
from datetime import datetime, timezone

from enforcer_storage import (
    append_durable_text,
    atomic_write_text,
    exclusive_lock,
    secure_dir,
    secure_file,
)

LEVERS = ("force_plan", "set_hints", "unforce_plan", "clear_hints")
# Levers that PLACE a control (leave something to roll back) vs levers that REMOVE one.
_APPLY_LEVERS = ("force_plan", "set_hints")
_ROLLBACK_LEVERS = ("unforce_plan", "clear_hints")

OUTCOMES = (
    "dry_run",
    "prepared",
    "emitted",
    "applied",
    "kept",
    "rolled_back",
    "force_failed",
    "skipped",
)
MODES = ("apply", "dry_run")

_STR_FIELDS = ("id", "timestamp", "environment", "query_hash", "category", "lever",
               "action_sql", "rollback_sql", "mode", "outcome", "reason", "detail_file")
_DICT_FIELDS = ("baseline_metrics",)
_NONEMPTY_STR_FIELDS = (
    "id",
    "timestamp",
    "environment",
    "query_hash",
    "category",
    "lever",
    "mode",
    "outcome",
    "detail_file",
)

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
    legacy = pathlib.Path.home() / ".copilot" / "skills" / "sql_plan_enforcer" / "audits"
    if legacy.exists():
        return legacy  # keep an established ledger working after a host switch
    return pathlib.Path.home() / ".sql-skills" / "sql_plan_enforcer" / "audits"


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


def _usable_environment(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
        and value.strip().lower() not in {"unknown", "none", "null"}
    )


def _normalized_environment(value: str) -> str:
    return value.strip().casefold()


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _canonical_sql(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().rstrip(";").split()).casefold()


def _contains_nonfinite_number(value: object) -> bool:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return False
    if isinstance(value, (int, float)):
        return not math.isfinite(float(value))
    if isinstance(value, dict):
        return any(
            not isinstance(key, str) or _contains_nonfinite_number(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_nonfinite_number(item) for item in value)
    return True


def _expected_control_sql(lever: str, query_id: int, plan_id: int | None) -> str | None:
    if lever == "force_plan" and plan_id is not None:
        return f"exec sys.sp_query_store_force_plan @query_id = {query_id}, @plan_id = {plan_id}"
    if lever == "unforce_plan" and plan_id is not None:
        return f"exec sys.sp_query_store_unforce_plan @query_id = {query_id}, @plan_id = {plan_id}"
    if lever == "clear_hints":
        return f"exec sys.sp_query_store_clear_hints @query_id = {query_id}"
    return None


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
    for field in _NONEMPTY_STR_FIELDS:
        if isinstance(record.get(field), str) and not record[field].strip():
            errors.append(f"{field} must not be empty")

    if "query_id" not in record:
        errors.append("missing required field: query_id")
    elif not _positive_int(record["query_id"]):
        errors.append("query_id must be a positive integer")

    if not _usable_environment(record.get("environment")):
        errors.append("environment must be a non-empty, known string")

    # plan_id is required for force/unforce, null for hint controls.
    if "plan_id" not in record:
        errors.append("missing required field: plan_id")
    elif record["plan_id"] is not None and not _positive_int(record["plan_id"]):
        errors.append("plan_id must be a positive integer or null")

    for field in _DICT_FIELDS:
        if field in record and not isinstance(record[field], dict):
            errors.append(f"{field} must be an object")
    baseline = record.get("baseline_metrics")
    if isinstance(baseline, dict) and _contains_nonfinite_number(baseline):
        errors.append("baseline_metrics must contain only finite JSON values")
    if record.get("outcome") in {"prepared", "emitted", "applied", "kept"} and (
        not isinstance(baseline, dict) or not baseline
    ):
        errors.append(f"{record.get('outcome')} outcome requires non-empty baseline_metrics")

    if record.get("lever") not in LEVERS:
        errors.append(f"lever must be one of {', '.join(LEVERS)}; got {record.get('lever')!r}")
    if record.get("mode") not in MODES:
        errors.append(f"mode must be one of {', '.join(MODES)}; got {record.get('mode')!r}")
    if record.get("outcome") not in OUTCOMES:
        errors.append(f"outcome must be one of {', '.join(OUTCOMES)}; got {record.get('outcome')!r}")
    if record.get("outcome") == "dry_run" and record.get("mode") != "dry_run":
        errors.append("dry_run outcome requires dry_run mode")
    if (
        record.get("outcome") in {
            "prepared", "applied", "kept", "rolled_back", "force_failed"
        }
        and record.get("mode") != "apply"
    ):
        errors.append(f"{record.get('outcome')} outcome requires apply mode")

    if record.get("outcome") in {
        "dry_run", "prepared", "emitted", "applied", "kept", "rolled_back", "force_failed"
    }:
        if not (isinstance(record.get("action_sql"), str) and record["action_sql"].strip()):
            errors.append(f"{record.get('outcome')} outcome requires a non-empty action_sql")

    # The whole point of the ledger: an applied control must carry a real rollback.
    if record.get("lever") in _APPLY_LEVERS and record.get("outcome") in ("applied", "kept"):
        if not (isinstance(record.get("rollback_sql"), str) and record["rollback_sql"].strip()):
            errors.append("an applied control must include a non-empty rollback_sql")

    # An emitted script leaves your hands with a human — it must carry both the apply
    # statement and its undo, since the ledger row is the only durable copy of either.
    if record.get("lever") in _APPLY_LEVERS and record.get("outcome") in {
        "dry_run", "prepared", "emitted"
    }:
        if not (isinstance(record.get("rollback_sql"), str) and record["rollback_sql"].strip()):
            errors.append(
                f"a {record.get('outcome')} control must include a non-empty rollback_sql"
            )

    if record.get("lever") in {"force_plan", "unforce_plan"} and record.get("plan_id") is None:
        errors.append(f"{record.get('lever')} requires a plan_id")
    if record.get("lever") in {"set_hints", "clear_hints"} and record.get("plan_id") is not None:
        errors.append(f"{record.get('lever')} requires plan_id to be null")

    lever = record.get("lever")
    query_id = record.get("query_id")
    plan_id = record.get("plan_id")
    if lever in LEVERS and _positive_int(query_id):
        expected_action = _expected_control_sql(lever, query_id, plan_id)
        action_sql = record.get("action_sql")
        if action_sql and expected_action and _canonical_sql(action_sql) != expected_action:
            errors.append(f"action_sql does not match the exact {lever} statement")
        if action_sql and lever == "set_hints":
            set_pattern = re.compile(
                rf"^exec\s+sys\.sp_query_store_set_hints\s+@query_id\s*=\s*{query_id}\s*,\s*"
                rf"@query_hints\s*=\s*N'OPTION\s*\((?:''|[^'])*\)'\s*;?\s*$",
                re.IGNORECASE,
            )
            if not set_pattern.fullmatch(action_sql.strip()):
                errors.append("action_sql does not match a single set_query_store_hints statement")

        rollback_sql = record.get("rollback_sql")
        if rollback_sql and lever == "force_plan" and _positive_int(plan_id):
            expected_rollback = _expected_control_sql("unforce_plan", query_id, plan_id)
            if _canonical_sql(rollback_sql) != expected_rollback:
                errors.append("rollback_sql does not match the exact unforce_plan statement")
        if rollback_sql and lever == "set_hints":
            expected_rollback = _expected_control_sql("clear_hints", query_id, None)
            if _canonical_sql(rollback_sql) != expected_rollback:
                errors.append("rollback_sql does not match the exact clear_hints statement")

    detail_file = record.get("detail_file")
    if isinstance(detail_file, str):
        detail_path = pathlib.PurePosixPath(detail_file)
        if (
            len(detail_path.parts) != 2
            or detail_path.parts[0] != "runs"
            or detail_path.suffix != ".md"
            or detail_path.name in {".", ".."}
        ):
            errors.append("detail_file must be a direct runs/<id>.md path")
        elif isinstance(record.get("id"), str) and detail_file != f"runs/{record['id']}.md":
            errors.append("detail_file must match the ledger record id")

    timestamp = record.get("timestamp")
    if isinstance(timestamp, str):
        try:
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is None or parsed.tzinfo is None:
            errors.append("timestamp must be an ISO-8601 timestamp with timezone")

    return errors


def build_record(action: dict, *, now: datetime | None = None, nonce: str | None = None) -> dict:
    """Assemble a ledger record from an action document."""
    query_id = action.get("query_id")
    if not _positive_int(query_id):
        raise ValueError("action must include a positive integer 'query_id'")
    if not _usable_environment(action.get("environment")):
        raise ValueError("action must include a non-empty, known 'environment'")
    if action.get("lever") not in LEVERS:
        raise ValueError(f"action 'lever' must be one of {', '.join(LEVERS)}")

    now = now or datetime.now(timezone.utc)
    nonce = nonce or secrets.token_hex(3)
    qhash = _query_hash(action)
    record_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{nonce}__q{query_id}"

    return {
        "id": record_id,
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": _normalized_environment(action["environment"]),
        "query_id": query_id,
        "query_hash": qhash,
        "category": action.get("category", "unknown"),
        "lever": action["lever"],
        "plan_id": action.get("plan_id"),
        "action_sql": action.get("action_sql", ""),
        "rollback_sql": action.get("rollback_sql", ""),
        "baseline_metrics": action.get("baseline_metrics", {}),
        "mode": action.get("mode", "dry_run"),
        "outcome": action.get("outcome", "dry_run"),
        "reason": action.get("reason", ""),
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
    secure_dir(root)
    secure_dir(root / "runs")
    gitignore = root / ".gitignore"
    if not gitignore.exists():
        atomic_write_text(gitignore, _GITIGNORE)
    else:
        secure_file(gitignore)
    readme = root / "README.md"
    if not readme.exists():
        atomic_write_text(readme, _README)
    else:
        secure_file(readme)


def write_ledger(action: dict, root: pathlib.Path | None = None) -> dict:
    """Validate and persist one action. Raises on invalid input (fail-closed)."""
    root = root or ledger_dir()
    with exclusive_lock(root):
        record = build_record(action)

        problems = validate(record)
        if problems:
            raise ValueError("ledger record is invalid: " + "; ".join(problems))

        ensure_corpus(root)
        atomic_write_text(root / record["detail_file"], render_detail(record))
        append_durable_text(root / "index.jsonl", json.dumps(record, ensure_ascii=False) + "\n")
    return record


def pending_rollbacks(records: list) -> list:
    """Reconstruct every control still in place, newest state per (env, query_id, family).

    force_plan/unforce_plan share the "force" family; set_hints/clear_hints share "hints".
    Returns a list of {environment, query_id, lever, rollback_sql} the operator can replay
    to revert everything the enforcer left active.
    """
    active: dict[tuple, dict] = {}
    for rec in records:
        problems = validate(rec)
        if problems:
            raise ValueError("ledger record is invalid: " + "; ".join(problems))
        key = (
            _normalized_environment(rec["environment"]),
            rec["query_id"],
            _lever_family(rec["lever"]),
        )
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


def _lever_family(lever: str) -> str:
    return "force" if lever in ("force_plan", "unforce_plan") else "hints"


def unresolved_prepared(records: list) -> list[dict]:
    """Return live actions prepared before a crash/unknown tool outcome.

    A later confirmed row for the same environment/query/control family resolves the
    uncertainty. Until then the loop must verify read-only and must not apply again.
    """
    unresolved: dict[str, dict] = {}
    resolving_outcomes = {
        "emitted", "applied", "kept", "rolled_back", "force_failed", "skipped"
    }
    for rec in records:
        problems = validate(rec)
        if problems:
            raise ValueError("ledger record is invalid: " + "; ".join(problems))
        if rec["outcome"] == "prepared":
            unresolved[rec["id"]] = {
                "environment": rec["environment"],
                "query_id": rec["query_id"],
                "lever": rec["lever"],
                "plan_id": rec["plan_id"],
                "action_sql": rec["action_sql"],
                "rollback_sql": rec["rollback_sql"],
                "prepared_record_id": rec["id"],
            }
        elif rec["outcome"] in resolving_outcomes:
            action = _canonical_sql(rec.get("action_sql"))
            if not action:
                continue
            matching = [
                record_id
                for record_id, prepared in unresolved.items()
                if _normalized_environment(prepared["environment"])
                == _normalized_environment(rec["environment"])
                and prepared["query_id"] == rec["query_id"]
                and _lever_family(prepared["lever"]) == _lever_family(rec["lever"])
                and action in {
                    _canonical_sql(prepared["action_sql"]),
                    _canonical_sql(prepared["rollback_sql"]),
                }
            ]
            if matching:
                # Resolve one exact operation only. Duplicate prepared rows stay
                # visible until each has its own confirmed outcome.
                unresolved.pop(matching[-1], None)
    return list(unresolved.values())


def _load_index_unlocked(root: pathlib.Path) -> list:
    secure_dir(root)
    path = root / "index.jsonl"
    if not path.exists():
        return []
    secure_file(path)
    records = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if line:
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"ledger index is corrupt at line {line_number}: {path}") from exc
            problems = validate(record)
            if problems:
                raise ValueError(
                    f"ledger index is invalid at line {line_number}: "
                    + "; ".join(problems)
                )
            records.append(record)
    return records


def _load_index(root: pathlib.Path) -> list:
    with exclusive_lock(root):
        return _load_index_unlocked(root)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Record / inspect plan-enforcement actions.")
    parser.add_argument("--input", "-i", help="Action JSON document, or '-' for stdin.")
    parser.add_argument("--pending", action="store_true", help="Print active rollback SQL and exit.")
    parser.add_argument("--validate", metavar="FILE", help="Validate a ledger index.jsonl and exit.")
    args = parser.parse_args(argv[1:])

    if args.validate:
        try:
            lines = pathlib.Path(args.validate).read_text("utf-8").splitlines()
            bad = 0
            for n, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(f"line {n}: invalid JSON: {exc}", file=sys.stderr)
                    bad += 1
                    continue
                problems = validate(record)
                for problem in problems:
                    bad += 1
                    print(f"line {n}: {problem}", file=sys.stderr)
            return 1 if bad else 0
        except OSError as exc:
            print(f"could not read ledger: {exc}", file=sys.stderr)
            return 2

    if args.pending:
        try:
            records = _load_index(ledger_dir())
        except (OSError, ValueError) as exc:
            print(f"ledger rejected: {exc}", file=sys.stderr)
            return 2
        uncertain = unresolved_prepared(records)
        for item in uncertain:
            print(
                f"-- UNRESOLVED PREPARED ACTION: {item['environment']} "
                f"query_id={item['query_id']} ({item['lever']})"
            )
            print("-- Verify read-only before any new apply; rollback if the control is active.")
            print(item["rollback_sql"] or "-- No rollback SQL recorded.")
        for item in pending_rollbacks(records):
            print(f"-- {item['environment']} query_id={item['query_id']} ({item['lever']})")
            print(item["rollback_sql"])
        return 3 if uncertain else 0

    source = args.input or "-"
    try:
        raw = (
            sys.stdin.read()
            if source == "-"
            else pathlib.Path(source).read_text("utf-8")
        )
        action = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"could not parse action document: {exc}", file=sys.stderr)
        return 1

    # Fail-closed: a failed ledger write must stop the apply, so we return non-zero.
    try:
        record = write_ledger(action)
    except (OSError, ValueError) as exc:
        print(f"ledger write rejected: {exc}", file=sys.stderr)
        return 2
    print(f"ledger {record['id']} ({record['lever']}/{record['outcome']}) -> {record['detail_file']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
