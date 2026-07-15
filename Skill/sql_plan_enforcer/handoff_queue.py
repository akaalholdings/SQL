#!/usr/bin/env python3
"""Durable handoff queue: evidence packs the enforcer (and health triage) hand to sql_optimizer.

A ``handoff_optimizer`` candidate is a real work item — a query that needs a rewrite or an
index, which this skill must never apply itself. Before this queue existed the handoff was a
report bullet that nobody consumed; now it is a durable pack carrying the evidence already
gathered (Query Store metrics, plan ids, query text) so the optimizer starts warm, plus a
status lifecycle so shipped rewrites flow back into the enforcer's re-verification
(``coverage_state.py`` state ``redeploy_verify``).

Like ``enforcement_ledger.py`` (and unlike the best-effort audit corpus), enqueueing is
**fail-closed**: a lost pack is a lost work item, so ``add`` raises on invalid input and the
caller must treat a failed write as a hard stop for that handoff.

Lifecycle::

    open ──▶ claimed ──▶ shipped     rewrite/index deployed; enforcer re-verifies
                │  ▲          │       (verify says regression → reopen)
                │  └──────────┘
                ├──▶ declined        already optimal / not worth a rewrite (terminal)
                └──▶ open            released without a verdict

Storage (``$SQL_PLAN_ENFORCER_HANDOFF_DIR``, default alongside the ledger):

- ``packs/<id>.json`` — current record per pack, atomic tmp+replace.
- ``index.jsonl``     — append-only event log (one line per transition; latest wins).

Writes are serialized with a small POSIX lock file. Atomic per-pack replace plus the
append-only index keeps readers safe while scheduled ticks and attended sessions overlap.

There is at most one unresolved pack for each ``(environment, query_id)`` identity;
both ``open`` and ``claimed`` are unresolved.  ``shipped`` and ``declined`` are terminal
for dedupe.

CLI::

    python3 handoff_queue.py add --input /tmp/pack.json
    python3 handoff_queue.py list --status open
    python3 handoff_queue.py claim <id>
    python3 handoff_queue.py complete <id> --resolution /tmp/resolution.json
    python3 handoff_queue.py reopen <id> --note "post-deploy regression"
    python3 handoff_queue.py validate
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

SOURCES = ("sql_plan_enforcer", "sql_health_triage")
STATUSES = ("open", "claimed", "shipped", "declined")

# new status -> statuses it may come from
_TRANSITIONS = {
    "claimed": ("open",),
    "shipped": ("claimed",),
    "declined": ("claimed",),
    "open": ("claimed", "shipped"),  # release, or reopen after a post-deploy regression
}

# Terminal-ish statuses that must carry a resolution explaining the verdict.
_NEEDS_RESOLUTION = ("shipped", "declined")

_STR_FIELDS = ("id", "created_at", "source", "environment", "query_hash",
               "category", "reason", "status")
_PACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")

_GITIGNORE = """\
# Handoff packs may contain production query text. Never commit.
*
!.gitignore
!README.md
"""

_README = """\
# sql_plan_enforcer handoff queue

Private runtime state written by `handoff_queue.py`. Packs may contain production query
text and metrics. Do not commit or copy this directory into a shared workspace.
"""


def queue_dir() -> pathlib.Path:
    override = os.environ.get("SQL_PLAN_ENFORCER_HANDOFF_DIR")
    if override:
        return pathlib.Path(override).expanduser()
    legacy = pathlib.Path.home() / ".copilot" / "skills" / "sql_plan_enforcer" / "handoffs"
    if legacy.exists():
        return legacy  # keep in-flight packs findable after a host switch
    return pathlib.Path.home() / ".sql-skills" / "sql_plan_enforcer" / "handoffs"


def _query_hash(query_id, query_text: str | None) -> str:
    """Stable per-query key: from text when supplied, else the query_id."""
    if isinstance(query_text, str) and query_text.strip():
        normalized = " ".join(query_text.split()).lower()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"q{query_id}"


def _usable_environment(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
        and value.strip().lower() not in {"unknown", "none", "null"}
    )


def _normalized_environment(value: str) -> str:
    return value.strip().casefold()


def _positive_query_id(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _true_flag(value: object) -> bool:
    return value is True or (
        isinstance(value, str) and value.strip().casefold() in {"1", "true", "yes", "y"}
    )


def _contains_truncation(value: object) -> bool:
    if isinstance(value, dict):
        if _true_flag(value.get("truncated")):
            return True
        return any(_contains_truncation(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_truncation(item) for item in value)
    return False


def _validate_pack_id(pack_id: object) -> str:
    if not isinstance(pack_id, str) or not pack_id.strip():
        raise ValueError("pack id must be a non-empty string")
    value = pack_id.strip()
    if (
        pathlib.Path(value).name != value
        or value in {".", ".."}
        or not _PACK_ID_PATTERN.fullmatch(value)
    ):
        raise ValueError("pack id must be a plain file name")
    return value


def build_pack(
    candidate: dict,
    *,
    environment: str,
    query_text: str | None = None,
    source: str = "sql_plan_enforcer",
    now: datetime | None = None,
    nonce: str | None = None,
) -> dict:
    """Assemble an evidence pack from a ranked scan candidate."""
    if not isinstance(candidate, dict):
        raise ValueError("candidate must be an object")
    query_id = candidate.get("query_id")
    if not _positive_query_id(query_id):
        raise ValueError("candidate must include a positive integer 'query_id'")
    if not _usable_environment(environment):
        raise ValueError("environment must be a non-empty, known string")
    if source not in SOURCES:
        raise ValueError(f"source must be one of {', '.join(SOURCES)}")
    if _contains_truncation(candidate):
        raise ValueError("truncated candidate evidence cannot create a handoff pack")

    now = now or datetime.now(timezone.utc)
    nonce = nonce or secrets.token_hex(3)
    pack_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{nonce}__q{query_id}"

    metrics = {
        key: candidate[key]
        for key in ("count_executions", "avg_duration", "avg_cpu_time",
                    "avg_logical_io_reads", "total_duration", "total_cpu_time",
                    "total_logical_reads")
        if key in candidate
    }
    evidence: dict = {"metrics": metrics}
    if candidate.get("regression_pct") is not None:
        evidence["regression_pct"] = candidate["regression_pct"]
    plan_ids = {}
    if candidate.get("current_plan_id") is not None:
        plan_ids["current"] = candidate["current_plan_id"]
    if candidate.get("proposed_plan_id") is not None:
        plan_ids["proposed"] = candidate["proposed_plan_id"]
    evidence["plan_ids"] = plan_ids
    if isinstance(query_text, str) and query_text.strip():
        evidence["query_sql_text"] = query_text
    if candidate.get("adapted_from"):
        evidence["notes"] = f"scanned via {candidate['adapted_from']}"

    return {
        "id": pack_id,
        "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
        "environment": _normalized_environment(environment),
        "query_id": query_id,
        "query_hash": _query_hash(query_id, query_text),
        "category": str(candidate.get("category", "unknown")),
        "reason": str(candidate.get("reason", "")),
        "evidence": evidence,
        "status": "open",
    }


def validate(record: object) -> list[str]:
    """Return human-readable problems with a pack record (empty == valid)."""
    errors: list[str] = []
    if not isinstance(record, dict):
        return [f"record must be a JSON object, got {type(record).__name__}"]

    for field in _STR_FIELDS:
        if field not in record:
            errors.append(f"missing required field: {field}")
        elif not isinstance(record[field], str):
            errors.append(f"{field} must be a string")
        elif not record[field].strip():
            errors.append(f"{field} must not be empty")

    if "query_id" not in record:
        errors.append("missing required field: query_id")
    elif not _positive_query_id(record["query_id"]):
        errors.append("query_id must be a positive integer")

    if not _usable_environment(record.get("environment")):
        errors.append("environment must be a non-empty, known string")

    if record.get("source") not in SOURCES:
        errors.append(f"source must be one of {', '.join(SOURCES)}; got {record.get('source')!r}")
    if record.get("status") not in STATUSES:
        errors.append(f"status must be one of {', '.join(STATUSES)}; got {record.get('status')!r}")

    evidence = record.get("evidence")
    if not isinstance(evidence, dict):
        errors.append("evidence must be an object")
    elif not isinstance(evidence.get("metrics"), dict):
        errors.append("evidence.metrics must be an object")
    elif _contains_truncation(evidence):
        errors.append("truncated evidence cannot be queued for an owner handoff")
    else:
        for name, value in evidence["metrics"].items():
            if (
                not isinstance(name, str)
                or not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(float(value))
            ):
                errors.append(f"evidence metric {name!r} must be a finite number")
        plan_ids = evidence.get("plan_ids")
        if not isinstance(plan_ids, dict):
            errors.append("evidence.plan_ids must be an object")
        elif any(not _positive_query_id(value) for value in plan_ids.values()):
            errors.append("evidence.plan_ids values must be positive integers")
        regression_pct = evidence.get("regression_pct")
        if regression_pct is not None and (
            not isinstance(regression_pct, (int, float))
            or isinstance(regression_pct, bool)
            or not math.isfinite(float(regression_pct))
        ):
            errors.append("evidence.regression_pct must be a finite number")
        query_text = evidence.get("query_sql_text")
        if query_text is not None and (
            not isinstance(query_text, str) or not query_text.strip()
        ):
            errors.append("evidence.query_sql_text must be a non-empty string")

    try:
        _validate_pack_id(record.get("id"))
    except ValueError as exc:
        errors.append(str(exc))

    if record.get("status") in _NEEDS_RESOLUTION:
        resolution = record.get("resolution")
        if not isinstance(resolution, dict) or resolution.get("outcome") != record.get("status"):
            errors.append(
                f"a {record.get('status')} pack must carry a matching resolution outcome"
            )

    return errors


def apply_transition(
    record: dict,
    new_status: str,
    *,
    resolution: dict | None = None,
    now: datetime | None = None,
) -> dict:
    """Return a copy of the record moved to new_status. Raises on illegal transitions."""
    problems = validate(record)
    if problems:
        raise ValueError("handoff pack is invalid: " + "; ".join(problems))
    if new_status not in STATUSES:
        raise ValueError(f"unknown status {new_status!r}")
    current = record.get("status")
    allowed_from = _TRANSITIONS.get(new_status, ())
    if current not in allowed_from:
        raise ValueError(f"cannot move a {current!r} pack to {new_status!r}")
    if new_status in _NEEDS_RESOLUTION and not (
        isinstance(resolution, dict) and resolution.get("outcome") == new_status
    ):
        raise ValueError(
            f"moving to {new_status!r} requires a matching resolution outcome"
        )

    now = now or datetime.now(timezone.utc)
    updated = dict(record)
    updated["status"] = new_status
    updated["updated_at"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    if resolution is not None:
        updated["resolution"] = {**resolution, "timestamp": updated["updated_at"]}
    elif new_status == "open":
        updated.pop("resolution", None)  # reopened packs start clean
    return updated


def _pack_path(root: pathlib.Path, pack_id: str) -> pathlib.Path:
    return root / "packs" / f"{_validate_pack_id(pack_id)}.json"


def _ensure_queue_root(root: pathlib.Path) -> None:
    secure_dir(root)
    for name, content in ((".gitignore", _GITIGNORE), ("README.md", _README)):
        path = root / name
        if path.exists():
            secure_file(path)
        else:
            atomic_write_text(path, content)


def _write_pack(root: pathlib.Path, record: dict) -> None:
    path = _pack_path(root, record["id"])
    _ensure_queue_root(root)
    secure_dir(path.parent)
    atomic_write_text(path, json.dumps(record, ensure_ascii=False, indent=2) + "\n")
    append_durable_text(root / "index.jsonl", json.dumps(record, ensure_ascii=False) + "\n")


def load_packs(root: pathlib.Path | None = None) -> list[dict]:
    root = root or queue_dir()
    secure_dir(root)
    index_path = root / "index.jsonl"
    if index_path.exists():
        secure_file(index_path)
    packs_dir = root / "packs"
    if not packs_dir.is_dir():
        return []
    secure_dir(packs_dir)
    packs = []
    unresolved: dict[tuple[str, int], str] = {}
    for path in sorted(packs_dir.glob("*.json")):
        try:
            secure_file(path)
            record = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise ValueError(f"could not read handoff pack {path}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"handoff pack is not valid JSON: {path}: {exc}") from exc
        problems = validate(record)
        if problems:
            raise ValueError(f"handoff pack is invalid: {path}: {'; '.join(problems)}")
        if record.get("status") in {"open", "claimed"}:
            identity = (
                _normalized_environment(record["environment"]),
                record["query_id"],
            )
            previous = unresolved.get(identity)
            if previous is not None:
                raise ValueError(
                    "duplicate unresolved handoff packs for "
                    f"{identity[0]}/{identity[1]}: {previous}, {record['id']}"
                )
            unresolved[identity] = record["id"]
        packs.append(record)
    return packs


def validate_index(root: pathlib.Path | None = None) -> list[str]:
    """Validate every append-only queue event without trusting current pack files."""
    root = root or queue_dir()
    secure_dir(root)
    path = root / "index.jsonl"
    if not path.exists():
        return []
    secure_file(path)
    problems = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            problems.append(f"index line {line_number}: invalid JSON")
            continue
        for problem in validate(record):
            problems.append(f"index line {line_number}: {problem}")
    return problems


def find_pack(pack_id: str, root: pathlib.Path | None = None) -> dict:
    root = root or queue_dir()
    secure_dir(root)
    path = _pack_path(root, pack_id)
    if not path.exists():
        raise FileNotFoundError(f"no pack {pack_id!r} in {root}")
    try:
        secure_file(path)
        record = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"could not read handoff pack {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"handoff pack is not valid JSON: {path}") from exc
    problems = validate(record)
    if problems:
        raise ValueError(f"handoff pack is invalid: {'; '.join(problems)}")
    return record


def add(record: dict, root: pathlib.Path | None = None) -> dict:
    """Validate and persist one new pack. Raises on invalid input (fail-closed)."""
    root = root or queue_dir()
    with exclusive_lock(root):
        problems = validate(record)
        if problems:
            raise ValueError("handoff pack is invalid: " + "; ".join(problems))

        # Open and claimed packs are unresolved.  Deduplicate by the database-scoped
        # Query Store identity, not query text, because query text can change while the
        # query_id remains the work item's identity.
        for existing in load_packs(root):
            if (
                existing.get("status") in {"open", "claimed"}
                and _normalized_environment(existing.get("environment"))
                == _normalized_environment(record.get("environment"))
                and existing.get("query_id") == record.get("query_id")
            ):
                raise ValueError(
                    f"an open pack for this query already exists: {existing.get('id')}"
                )

        _write_pack(root, record)
    return record


def transition(
    pack_id: str,
    new_status: str,
    *,
    resolution: dict | None = None,
    root: pathlib.Path | None = None,
) -> dict:
    """Load, transition, and persist one pack."""
    root = root or queue_dir()
    with exclusive_lock(root):
        updated = apply_transition(find_pack(pack_id, root), new_status, resolution=resolution)
        _write_pack(root, updated)
    return updated


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Enforcer -> optimizer handoff queue.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_add = sub.add_parser("add", help="Enqueue a new evidence pack (fail-closed).")
    p_add.add_argument("--input", "-i", default="-", help="Pack JSON; '-' for stdin.")

    p_list = sub.add_parser("list", help="List packs.")
    p_list.add_argument("--status", choices=STATUSES, default=None)

    p_claim = sub.add_parser("claim", help="Mark an open pack as being worked.")
    p_claim.add_argument("pack_id")

    p_complete = sub.add_parser("complete", help="Resolve a claimed pack (shipped/declined).")
    p_complete.add_argument("pack_id")
    p_complete.add_argument("--resolution", "-r", default="-",
                            help="Resolution JSON with at least {\"outcome\": ...}; '-' stdin.")

    p_reopen = sub.add_parser("reopen", help="Reopen a claimed/shipped pack.")
    p_reopen.add_argument("pack_id")
    p_reopen.add_argument("--note", default="")

    sub.add_parser("validate", help="Validate every pack on disk.")

    args = parser.parse_args(argv[1:])
    root = queue_dir()

    if args.cmd == "add":
        try:
            raw = (
                sys.stdin.read()
                if args.input == "-"
                else pathlib.Path(args.input).read_text("utf-8")
            )
            record = add(json.loads(raw), root=root)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            print(f"handoff add rejected: {exc}", file=sys.stderr)
            return 2
        print(f"queued {record['id']} ({record['category']}) -> packs/{record['id']}.json")
        return 0

    if args.cmd == "list":
        try:
            packs = load_packs(root)
        except (OSError, ValueError) as exc:
            print(f"handoff list rejected: {exc}", file=sys.stderr)
            return 2
        if args.status:
            packs = [p for p in packs if p.get("status") == args.status]
        print(json.dumps({"count": len(packs), "packs": packs}, ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "claim":
        try:
            updated = transition(args.pack_id, "claimed", root=root)
        except (OSError, ValueError) as exc:
            print(f"handoff claim rejected: {exc}", file=sys.stderr)
            return 2
        print(f"claimed {updated['id']}")
        return 0

    if args.cmd == "complete":
        try:
            raw = (
                sys.stdin.read()
                if args.resolution == "-"
                else pathlib.Path(args.resolution).read_text("utf-8")
            )
            resolution = json.loads(raw)
            if not isinstance(resolution, dict):
                raise ValueError("resolution must be a JSON object")
            outcome = resolution.get("outcome")
            if outcome not in _NEEDS_RESOLUTION:
                raise ValueError("resolution outcome must be 'shipped' or 'declined'")
            updated = transition(
                args.pack_id,
                outcome,
                resolution=resolution,
                root=root,
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            print(f"handoff completion rejected: {exc}", file=sys.stderr)
            return 2
        print(f"{updated['status']} {updated['id']}")
        return 0

    if args.cmd == "reopen":
        try:
            with exclusive_lock(root):
                updated = apply_transition(find_pack(args.pack_id, root), "open")
                if args.note:
                    updated["reason"] = (
                        f"{updated.get('reason', '')} | reopened: {args.note}"
                    ).strip(" |")
                _write_pack(root, updated)
        except (OSError, ValueError) as exc:
            print(f"handoff reopen rejected: {exc}", file=sys.stderr)
            return 2
        print(f"reopened {updated['id']}")
        return 0

    if args.cmd == "validate":
        try:
            packs = load_packs(root)
            index_problems = validate_index(root)
        except (OSError, ValueError) as exc:
            print(f"handoff validation rejected: {exc}", file=sys.stderr)
            return 2
        bad = 0
        for pack in packs:
            for problem in validate(pack):
                bad += 1
                print(f"{pack.get('id')}: {problem}", file=sys.stderr)
        for problem in index_problems:
            bad += 1
            print(problem, file=sys.stderr)
        return 1 if bad else 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
