#!/usr/bin/env python3
"""Durable lifecycle ledger for disposable optimizer test-index experiments.

The agent records intent before creating a test index and closes the record only after
the index is dropped. A later session can inspect ``pending`` after a timeout or crash;
this module never executes the cleanup SQL itself.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import secrets
import sys
from datetime import datetime, timezone

from optimizer_storage import (
    append_text_line,
    atomic_write_text,
    ensure_private_dir,
    exclusive_lock,
    secure_file,
)

TEST_PREFIX = "IX_Testing_"
STATUSES = ("planned", "created", "dropped", "failed")
_TRANSITIONS = {
    "created": ("planned",),
    "dropped": ("created",),
    # Once creation succeeded, the record must stay pending until the index is
    # confirmed dropped. Marking it failed would hide an orphaned live index.
    "failed": ("planned",),
}


def experiment_dir() -> pathlib.Path:
    override = os.environ.get("SQL_OPTIMIZER_EXPERIMENT_DIR")
    if override:
        return pathlib.Path(override).expanduser()
    legacy = pathlib.Path.home() / ".copilot" / "skills" / "sql_optimizer" / "experiments"
    if legacy.exists():
        return legacy
    return pathlib.Path.home() / ".sql-skills" / "sql_optimizer" / "experiments"


def _quote(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"


def _validate_identifier(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not text or not (text[0].isalpha() or text[0] == "_") or not all(
        char.isalnum() or char == "_" for char in text
    ):
        raise ValueError(f"{label} must be a plain identifier")
    return text


def build_record(
    *,
    database: str,
    schema: str,
    table: str,
    index: str,
    rollback_sql: str | None = None,
    now: datetime | None = None,
    nonce: str | None = None,
) -> dict:
    database = str(database or "").strip()
    if not database:
        raise ValueError("database must not be empty")
    schema = _validate_identifier(schema, "schema")
    table = _validate_identifier(table, "table")
    index = _validate_identifier(index, "index")
    if not index.upper().startswith(TEST_PREFIX.upper()):
        raise ValueError(f"index must start with {TEST_PREFIX}")
    expected_rollback = f"DROP INDEX {_quote(index)} ON {_quote(schema)}.{_quote(table)}"
    if rollback_sql is not None and rollback_sql.strip() != expected_rollback:
        raise ValueError("rollback_sql must exactly match the generated test-index drop")
    now = now or datetime.now(timezone.utc)
    record_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{nonce or secrets.token_hex(3)}"
    return {
        "id": record_id,
        "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "database": database,
        "schema": schema,
        "table": table,
        "index": index,
        "rollback_sql": expected_rollback,
        "status": "planned",
    }


def validate(record: object) -> list[str]:
    if not isinstance(record, dict):
        return ["record must be an object"]
    errors = []
    for field in ("id", "created_at", "updated_at", "database", "schema", "table", "index", "rollback_sql", "status"):
        if not isinstance(record.get(field), str) or not record[field].strip():
            errors.append(f"missing required field: {field}")
    if record.get("status") not in STATUSES:
        errors.append(f"status must be one of {', '.join(STATUSES)}")
    validated_identifiers = {}
    for field in ("schema", "table", "index"):
        if isinstance(record.get(field), str):
            try:
                validated_identifiers[field] = _validate_identifier(record[field], field)
            except ValueError as exc:
                errors.append(str(exc))
    index = validated_identifiers.get("index")
    if index and not index.upper().startswith(TEST_PREFIX.upper()):
        errors.append(f"index must start with {TEST_PREFIX}")
    if all(name in validated_identifiers for name in ("schema", "table", "index")):
        expected_rollback = (
            f"DROP INDEX {_quote(validated_identifiers['index'])} ON "
            f"{_quote(validated_identifiers['schema'])}.{_quote(validated_identifiers['table'])}"
        )
        if record.get("rollback_sql") != expected_rollback:
            errors.append("rollback_sql does not match the exact generated test-index drop")
    for field in ("created_at", "updated_at"):
        value = record.get(field)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                parsed = None
            if parsed is None or parsed.tzinfo is None:
                errors.append(f"{field} must be an ISO-8601 timestamp with timezone")
    if isinstance(record.get("id"), str):
        try:
            _path(pathlib.Path("."), record["id"])
        except ValueError as exc:
            errors.append(str(exc))
    return errors


def _path(root: pathlib.Path, experiment_id: str) -> pathlib.Path:
    if (
        not experiment_id
        or pathlib.Path(experiment_id).name != experiment_id
        or experiment_id in {".", ".."}
    ):
        raise ValueError("experiment id must be a plain file name")
    return root / "records" / f"{experiment_id}.json"


def _write_unlocked(root: pathlib.Path, record: dict) -> None:
    ensure_private_dir(root)
    path = _path(root, record["id"])
    atomic_write_text(path, json.dumps(record, ensure_ascii=False, indent=2))
    append_text_line(
        root / "index.jsonl",
        json.dumps(record, ensure_ascii=False) + "\n",
    )


def _load_unlocked(experiment_id: str, root: pathlib.Path) -> dict:
    path = _path(root, experiment_id)
    try:
        secure_file(path)
        record = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"no experiment {experiment_id!r}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"experiment record is corrupt: {path}") from exc
    problems = validate(record)
    if problems:
        raise ValueError("experiment record is invalid: " + "; ".join(problems))
    return record


def load(experiment_id: str, root: pathlib.Path | None = None) -> dict:
    root = root or experiment_dir()
    with exclusive_lock(root):
        return _load_unlocked(experiment_id, root)


def begin(record: dict, root: pathlib.Path | None = None) -> dict:
    root = root or experiment_dir()
    problems = validate(record)
    if problems:
        raise ValueError("experiment record is invalid: " + "; ".join(problems))
    with exclusive_lock(root):
        if _path(root, record["id"]).exists():
            raise ValueError(f"experiment {record['id']!r} already exists")
        _write_unlocked(root, record)
    return record


def transition(experiment_id: str, status: str, *, root: pathlib.Path | None = None) -> dict:
    if status not in _TRANSITIONS:
        raise ValueError(f"unsupported transition {status!r}")
    root = root or experiment_dir()
    with exclusive_lock(root):
        record = _load_unlocked(experiment_id, root)
        if record["status"] not in _TRANSITIONS[status]:
            raise ValueError(f"cannot move a {record['status']!r} experiment to {status!r}")
        updated = dict(record)
        updated["status"] = status
        updated["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_unlocked(root, updated)
    return updated


def pending(root: pathlib.Path | None = None) -> list[dict]:
    root = root or experiment_dir()
    if not root.exists():
        return []
    with exclusive_lock(root):
        records_dir = root / "records"
        if not records_dir.is_dir():
            return []
        records = []
        for path in sorted(records_dir.glob("*.json")):
            secure_file(path)
            record = json.loads(path.read_text(encoding="utf-8"))
            problems = validate(record)
            if problems:
                raise ValueError(f"{path}: " + "; ".join(problems))
            if record["status"] in {"planned", "created"}:
                records.append(record)
    return records


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Track disposable SQL optimizer test indexes.")
    sub = parser.add_subparsers(dest="command", required=True)
    start = sub.add_parser("begin", help="Record intent before creating the index.")
    start.add_argument("--database", required=True)
    start.add_argument("--schema", required=True)
    start.add_argument("--table", required=True)
    start.add_argument("--index", required=True)
    created = sub.add_parser("mark-created", help="Record successful creation.")
    created.add_argument("experiment_id")
    dropped = sub.add_parser("mark-dropped", help="Record successful cleanup.")
    dropped.add_argument("experiment_id")
    failed = sub.add_parser(
        "mark-failed",
        help="Record a creation that failed before the index existed.",
    )
    failed.add_argument("experiment_id")
    sub.add_parser("pending", help="Show experiments requiring verification or cleanup.")
    args = parser.parse_args(argv[1:])

    try:
        if args.command == "begin":
            record = begin(build_record(
                database=args.database, schema=args.schema, table=args.table, index=args.index,
            ))
            print(record["id"])
        elif args.command == "mark-created":
            print(transition(args.experiment_id, "created")["id"])
        elif args.command == "mark-dropped":
            print(transition(args.experiment_id, "dropped")["id"])
        elif args.command == "mark-failed":
            print(transition(args.experiment_id, "failed")["id"])
        else:
            records = pending()
            print(json.dumps({"count": len(records), "experiments": records}, indent=2))
        return 0
    except (OSError, ValueError) as exc:
        print(f"test-index ledger error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
