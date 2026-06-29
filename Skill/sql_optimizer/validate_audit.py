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
import sys

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
            if key in record["index_changes"] and not isinstance(
                record["index_changes"][key], int
            ):
                errors.append(f"index_changes.{key} must be an integer")

    if "outcome" in record and record["outcome"] not in OUTCOMES:
        errors.append(
            f"outcome must be one of {', '.join(OUTCOMES)}; got {record['outcome']!r}"
        )

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
