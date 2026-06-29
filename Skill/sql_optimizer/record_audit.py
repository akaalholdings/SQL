#!/usr/bin/env python3
"""Write one sql_optimizer audit entry to the durable audit corpus.

Called as the final, non-user-facing step of an optimization (see ``AuditGuide.md``).
The optimizing agent writes a run document to ``/tmp/audit.json`` (same pattern as
``RunGuide.md`` writing ``/tmp/*.sql``) and runs::

    python3 record_audit.py --input /tmp/audit.json

This script derives ``id`` / ``query_hash`` / ``timestamp``, assembles the compact
index record, validates it against the corpus contract (``validate_audit.py``),
appends one line to ``audits/index.jsonl`` and writes the full detail file
``audits/runs/<id>.md`` (which holds the RAW SQL).

Audit dir resolution (override wins):
    1. ``$SQL_OPTIMIZER_AUDIT_DIR``
    2. ``~/.copilot/skills/sql_optimizer/audits`` (the installed location both the
       running skill and the dev-repo review pass can resolve)
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

from validate_audit import OUTCOMES, validate

# Fields copied verbatim from the run document into the compact index record.
_LIST_FIELDS = ("tables", "anti_patterns", "rules_applied", "guidance_gaps")
_DICT_FIELDS = ("index_changes", "metrics", "improvement")

_GITIGNORE = """\
# Audit corpus — may contain RAW SQL. Never commit.
*
!.gitignore
!README.md
"""

_README = """\
# sql_optimizer audit corpus

Written by `record_audit.py` after each optimization run. **These files contain
raw SQL** (original query, rewrite, and generated scripts) exactly as supplied —
secure and back up this directory accordingly; do not commit it.

Layout:

- `index.jsonl` — one compact record per run; the queryable layer the
  self-improvement review pass (`ImproveGuide.md`) scans first.
- `runs/<id>.md` — full detail per run (raw query, rewrite, scripts, plan
  findings, metrics, narrative). Opened only for outlier/recurring cases.
- `reports/<date>-review.md` — output of `ImproveGuide.md` review passes.

Validate the index at any time with `python3 validate_audit.py index.jsonl`.
"""


def audit_dir() -> pathlib.Path:
    override = os.environ.get("SQL_OPTIMIZER_AUDIT_DIR")
    if override:
        return pathlib.Path(override).expanduser()
    return pathlib.Path.home() / ".copilot" / "skills" / "sql_optimizer" / "audits"


def audit_enabled() -> bool:
    """Audit logging is on by default; opt out with SQL_OPTIMIZER_AUDIT=0/false/off/no."""
    return os.environ.get("SQL_OPTIMIZER_AUDIT", "1").strip().lower() not in {
        "0",
        "false",
        "off",
        "no",
    }


def normalize_query(query: str) -> str:
    """Whitespace- and case-insensitive form used for the dedup hash."""
    return " ".join(query.split()).lower()


def query_hash(query: str) -> str:
    return hashlib.sha256(normalize_query(query).encode("utf-8")).hexdigest()[:12]


def build_record(run: dict, *, now: datetime | None = None, nonce: str | None = None) -> dict:
    """Assemble the compact index record from a run document."""
    query = run.get("query")
    if not isinstance(query, str) or not query.strip():
        raise ValueError("run document must include a non-empty 'query' string")

    outcome = run.get("outcome")
    if outcome not in OUTCOMES:
        raise ValueError(
            f"run document 'outcome' must be one of {', '.join(OUTCOMES)}; got {outcome!r}"
        )

    now = now or datetime.now(timezone.utc)
    qhash = query_hash(query)
    # The nonce keeps the id unique even when the same query is recorded twice in
    # the same second (query_hash, stored separately, still does dedup detection).
    nonce = nonce or secrets.token_hex(3)
    record_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{nonce}__{qhash}"

    record = {
        "id": record_id,
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "query_hash": qhash,
        "environment": str(run.get("environment", "unknown")),
        "equivalence_proven": bool(run.get("equivalence_proven", False)),
        "outcome": outcome,
        "detail_file": f"runs/{record_id}.md",
    }
    for field in _LIST_FIELDS:
        value = run.get(field, [])
        record[field] = [str(item) for item in value] if isinstance(value, list) else []
    for field in _DICT_FIELDS:
        value = run.get(field, {})
        record[field] = value if isinstance(value, dict) else {}
    return record


def _fenced(label: str, body: object, lang: str = "sql") -> str:
    text = body if isinstance(body, str) else ""
    if not text.strip():
        return f"## {label}\n\n_None recorded._\n"
    return f"## {label}\n\n```{lang}\n{text.rstrip()}\n```\n"


def _bullets(label: str, items: object) -> str:
    if not isinstance(items, list) or not items:
        return f"## {label}\n\n_None recorded._\n"
    lines = "\n".join(f"- {item}" for item in items)
    return f"## {label}\n\n{lines}\n"


def render_detail(record: dict, run: dict) -> str:
    """Full per-run markdown: frontmatter mirrors the index record, body holds raw SQL."""
    frontmatter = json.dumps(record, indent=2, ensure_ascii=False)
    scripts = run.get("scripts") if isinstance(run.get("scripts"), dict) else {}
    sections = [
        f"---\n{frontmatter}\n---\n",
        f"# Optimization audit `{record['id']}`\n",
        _fenced("Original query (raw)", run.get("query")),
        _fenced("Optimized query (raw)", run.get("rewrite")),
        _fenced("Index script", scripts.get("index")),
        _fenced("Rollback script", scripts.get("rollback")),
        _fenced("Deploy script", scripts.get("deploy")),
        _fenced("Plan findings", run.get("plan_findings"), lang="text"),
        _fenced("Three-scenario metrics", json.dumps(record["metrics"], indent=2), lang="json"),
        _fenced("Improvement vs baseline", json.dumps(record["improvement"], indent=2), lang="json"),
        _fenced("What changed and why", run.get("what_changed"), lang="text"),
        _bullets("Guidance gaps", record["guidance_gaps"]),
        _fenced("Notes", run.get("notes"), lang="text"),
    ]
    return "\n".join(sections)


def ensure_corpus(root: pathlib.Path) -> None:
    (root / "runs").mkdir(parents=True, exist_ok=True)
    (root / "reports").mkdir(parents=True, exist_ok=True)
    gitignore = root / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(_GITIGNORE, encoding="utf-8")
    readme = root / "README.md"
    if not readme.exists():
        readme.write_text(_README, encoding="utf-8")


def write_audit(run: dict, root: pathlib.Path | None = None) -> dict:
    """Validate and persist one audit entry. Returns the index record."""
    root = root or audit_dir()
    record = build_record(run)

    problems = validate(record)
    if problems:
        raise ValueError("assembled audit record is invalid: " + "; ".join(problems))

    ensure_corpus(root)
    (root / record["detail_file"]).write_text(render_detail(record, run), encoding="utf-8")
    with (root / "index.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Record one sql_optimizer audit entry.")
    parser.add_argument(
        "--input",
        "-i",
        default="-",
        help="Path to the run JSON document, or '-' for stdin (default).",
    )
    args = parser.parse_args(argv[1:])

    if not audit_enabled():
        print("audit logging disabled (SQL_OPTIMIZER_AUDIT) — nothing recorded")
        return 0

    raw = sys.stdin.read() if args.input == "-" else pathlib.Path(args.input).read_text("utf-8")
    try:
        run = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"could not parse run document: {exc}", file=sys.stderr)
        return 1

    try:
        record = write_audit(run)
    except (ValueError, OSError) as exc:
        # A failed audit write must never fail the optimization itself.
        print(f"audit not recorded: {exc}", file=sys.stderr)
        return 1

    print(f"recorded audit {record['id']} ({record['outcome']}) -> {record['detail_file']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
