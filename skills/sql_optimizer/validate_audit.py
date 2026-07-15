#!/usr/bin/env python3
"""Validate sql_optimizer audit records against the corpus contract.

One audit record is the JSON object written as a line in ``audits/index.jsonl``
(and mirrored in the frontmatter of ``audits/runs/<id>.md``). Every record must
share an identical shape so the self-improvement review pass can aggregate
hundreds of them cheaply and trust every row.

Use as a library::

    from validate_audit import validate
    errors = validate(record)          # -> list[str], empty means valid

Use as a CLI (validates each JSONL line on stdin or in a file)::

    python3 validate_audit.py audits/index.jsonl
    cat audits/index.jsonl | python3 validate_audit.py
"""

from __future__ import annotations

import json
import math
import pathlib
import re
import sys
from datetime import datetime

# Allowed values for the ``outcome`` field. Negatives are kept on purpose:
# a no-change / regressed / equivalence-failed run is high-value learning signal.
OUTCOMES = (
    "improved",
    "no_change",
    "already_optimal",
    "regressed",
    "equivalence_failed",
    "abandoned",
)

# field name -> expected python type(s)
_STR_FIELDS = ("id", "timestamp", "query_hash", "environment", "outcome", "detail_file")
_LIST_FIELDS = ("tables", "anti_patterns", "rules_applied", "guidance_gaps")
_DICT_FIELDS = ("index_changes", "metrics", "improvement")
_BOOL_FIELDS = ("equivalence_proven",)

REQUIRED_FIELDS = _STR_FIELDS + _LIST_FIELDS + _DICT_FIELDS + _BOOL_FIELDS
_HASH_PATTERN = re.compile(r"^[0-9a-f]{12}$")


def _validate_json_value(value: object, path: str, errors: list[str]) -> None:
    """Reject values that cannot be represented safely in strict JSON."""
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            errors.append(f"{path} must contain only finite numbers")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]", errors)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                errors.append(f"{path} keys must be non-empty strings")
                continue
            _validate_json_value(item, f"{path}.{key}", errors)
        return
    errors.append(f"{path} contains unsupported value type {type(value).__name__}")


def validate(record: object) -> list[str]:
    """Return a list of human-readable problems with ``record`` (empty == valid)."""
    errors: list[str] = []

    if not isinstance(record, dict):
        return [f"record must be a JSON object, got {type(record).__name__}"]

    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"missing required field: {field}")

    for field in _STR_FIELDS:
        if field in record and not isinstance(record[field], str):
            errors.append(f"{field} must be a string")
        elif field in record and not record[field].strip():
            errors.append(f"{field} must not be empty")

    for field in _BOOL_FIELDS:
        # bool is a subclass of int; isinstance(..., bool) is what we want here.
        if field in record and not isinstance(record[field], bool):
            errors.append(f"{field} must be a boolean")

    for field in _LIST_FIELDS:
        if field in record:
            value = record[field]
            if not isinstance(value, list):
                errors.append(f"{field} must be a list")
            elif not all(isinstance(item, str) for item in value):
                errors.append(f"{field} must be a list of strings")

    for field in _DICT_FIELDS:
        if field in record and not isinstance(record[field], dict):
            errors.append(f"{field} must be an object")

    if "index_changes" in record and isinstance(record["index_changes"], dict):
        for key in ("adds", "drops", "alters"):
            if key not in record["index_changes"]:
                errors.append(f"index_changes.{key} is required")
            elif not isinstance(record["index_changes"][key], int):
                errors.append(f"index_changes.{key} must be an integer")
            elif (
                isinstance(record["index_changes"][key], bool)
                or record["index_changes"][key] < 0
            ):
                errors.append(f"index_changes.{key} must be a non-negative integer")

    metrics = record.get("metrics")
    if isinstance(metrics, dict):
        _validate_json_value(metrics, "metrics", errors)

    improvement = record.get("improvement")
    if isinstance(improvement, dict):
        for key, value in improvement.items():
            if (
                not isinstance(key, str)
                or not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(float(value))
            ):
                errors.append(f"improvement metric {key!r} must be a finite number")

    if "outcome" in record and record["outcome"] not in OUTCOMES:
        errors.append(
            f"outcome must be one of {', '.join(OUTCOMES)}; got {record['outcome']!r}"
        )

    if record.get("outcome") == "improved" and record.get("equivalence_proven") is not True:
        errors.append("improved outcome requires equivalence_proven=true")
    if record.get("outcome") == "equivalence_failed" and record.get("equivalence_proven") is not False:
        errors.append("equivalence_failed outcome requires equivalence_proven=false")

    query_hash = record.get("query_hash")
    if isinstance(query_hash, str) and not _HASH_PATTERN.fullmatch(query_hash):
        errors.append("query_hash must be 12 lowercase hexadecimal characters")
    timestamp = record.get("timestamp")
    if isinstance(timestamp, str):
        try:
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is None or parsed.tzinfo is None:
            errors.append("timestamp must be an ISO-8601 timestamp with timezone")
    detail_file = record.get("detail_file")
    if isinstance(detail_file, str):
        path = pathlib.PurePosixPath(detail_file)
        if len(path.parts) != 2 or path.parts[0] != "runs" or path.suffix != ".md":
            errors.append("detail_file must be a direct runs/<id>.md path")
        elif isinstance(record.get("id"), str) and detail_file != f"runs/{record['id']}.md":
            errors.append("detail_file must match the record id")

    return errors


def _iter_lines(source: str | None):
    if source is None or source == "-":
        yield from enumerate(sys.stdin, start=1)
    else:
        with open(source, encoding="utf-8") as handle:
            yield from enumerate(handle, start=1)


def main(argv: list[str]) -> int:
    source = argv[1] if len(argv) > 1 else None
    bad = 0
    checked = 0
    for line_no, raw in _iter_lines(source):
        raw = raw.strip()
        if not raw:
            continue
        checked += 1
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            bad += 1
            print(f"line {line_no}: invalid JSON: {exc}", file=sys.stderr)
            continue
        problems = validate(record)
        if problems:
            bad += 1
            for problem in problems:
                print(f"line {line_no}: {problem}", file=sys.stderr)

    if bad:
        print(f"{bad}/{checked} record(s) invalid", file=sys.stderr)
        return 1
    print(f"{checked} record(s) valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
