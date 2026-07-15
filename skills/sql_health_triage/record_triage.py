#!/usr/bin/env python3
"""Best-effort durable log of triage sessions (mirrors sql_optimizer's audit corpus).

Unlike the enforcer's ledger, this log is **never** safety-critical: triage is read-only,
so a failed write loses history, not the ability to undo anything. The caller surfaces a
one-line note on failure and moves on — never block or fail a triage on logging.

Record one entry per completed triage (incident or sweep). Outcomes:

- ``resolved``              the finding was addressed during the session (human acted)
- ``handed_off_optimizer``  culprit query enqueued as a handoff pack for sql_optimizer
- ``handed_off_enforcer``   plan-instability finding pointed at sql_plan_enforcer
- ``escalated_human``       needs a decision or action only the operator can take
- ``inconclusive``          symptom not reproduced / nothing crossed a threshold

Usage::

    python3 record_triage.py --input /tmp/triage.json

Log dir resolution (override wins):
    1. ``$SQL_HEALTH_TRIAGE_AUDIT_DIR``
    2. ``~/.copilot/skills/sql_health_triage/audits`` — legacy location, used only when
       it already exists
    3. ``~/.sql-skills/sql_health_triage/audits`` — host-neutral default

Disable entirely with ``SQL_HEALTH_TRIAGE_AUDIT=0`` (also ``false``/``off``/``no``).
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
import pathlib
import secrets
import sys
from datetime import datetime, timezone

from triage_report import build_report as build_triage_report

OUTCOMES = (
    "resolved",
    "handed_off_optimizer",
    "handed_off_enforcer",
    "escalated_human",
    "inconclusive",
)
AUDIT_DIR_MODE = 0o700
AUDIT_FILE_MODE = 0o600
ACTIONABLE_SEVERITIES = frozenset(("critical", "high", "medium", "low"))
INCONCLUSIVE_NEXT_STEP = (
    "Evidence was truncated; narrow or re-query the diagnostic evidence before "
    "drawing a conclusion or taking corrective action."
)

_GITIGNORE = """\
# Triage log — records production symptoms and query ids. Never commit.
*
!.gitignore
!README.md
"""

_README = """\
# sql_health_triage log

Written by `record_triage.py`, one entry per completed triage session.

- `index.jsonl` — one record per session; the queryable layer.
- `runs/<id>.md` — full detail per session (findings, actions, handoffs).
"""


def audit_enabled() -> bool:
    raw = os.environ.get("SQL_HEALTH_TRIAGE_AUDIT", "1").strip().lower()
    return raw not in ("0", "false", "off", "no")


def audit_dir() -> pathlib.Path:
    override = os.environ.get("SQL_HEALTH_TRIAGE_AUDIT_DIR")
    if override:
        return pathlib.Path(override).expanduser()
    legacy = pathlib.Path.home() / ".copilot" / "skills" / "sql_health_triage" / "audits"
    if legacy.exists():
        return legacy
    return pathlib.Path.home() / ".sql-skills" / "sql_health_triage" / "audits"


def validate(record: object) -> list[str]:
    errors: list[str] = []
    if not isinstance(record, dict):
        return [f"record must be a JSON object, got {type(record).__name__}"]
    for field in ("id", "timestamp", "environment", "mode", "outcome", "detail_file"):
        if field not in record:
            errors.append(f"missing required field: {field}")
        elif not isinstance(record[field], str):
            errors.append(f"{field} must be a string")
    if record.get("outcome") not in OUTCOMES:
        errors.append(f"outcome must be one of {', '.join(OUTCOMES)}; got {record.get('outcome')!r}")
    if "findings" in record and not isinstance(record["findings"], list):
        errors.append("findings must be a list")
    if "handoff_pack_ids" in record and not isinstance(record["handoff_pack_ids"], list):
        errors.append("handoff_pack_ids must be a list")
    return errors


def _is_truncated_finding(finding: object) -> bool:
    if not isinstance(finding, dict):
        return False
    return _contains_truncation(finding.get("evidence"))


def _true_flag(value: object) -> bool:
    return value is True or (
        isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "y"}
    )


def _contains_truncation(value: object) -> bool:
    if isinstance(value, dict):
        if _true_flag(value.get("truncated")):
            return True
        return any(_contains_truncation(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_truncation(item) for item in value)
    return False


def _safe_findings(findings: object) -> list:
    if not isinstance(findings, list):
        return []
    safe = []
    for finding in findings:
        if _is_truncated_finding(finding):
            safe.append({
                **finding,
                "severity": "inconclusive",
                "status": "inconclusive",
                "owner": None,
                "recommended_action": None,
                "next_step": INCONCLUSIVE_NEXT_STEP,
            })
        else:
            safe.append(finding)
    return safe


def _has_actionable_owner(findings: list, owner: str) -> bool:
    return any(
        isinstance(finding, dict)
        and not _is_truncated_finding(finding)
        and finding.get("owner") == owner
        and (
            finding.get("status") == "actionable"
            or finding.get("severity") in ACTIONABLE_SEVERITIES
        )
        for finding in findings
    )


def build_record(session: dict, *, now: datetime | None = None, nonce: str | None = None) -> dict:
    if not isinstance(session, dict):
        raise ValueError("session must be a JSON object")
    outcome = session.get("outcome")
    if outcome not in OUTCOMES:
        raise ValueError(f"session 'outcome' must be one of {', '.join(OUTCOMES)}")

    now = now or datetime.now(timezone.utc)
    nonce = nonce or secrets.token_hex(3)
    record_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{nonce}"
    raw_findings = session.get("findings", [])
    if not isinstance(raw_findings, list) or any(
        not isinstance(finding, dict) for finding in raw_findings
    ):
        raise ValueError("session 'findings' must be a list of objects")
    # Recompute severity/status/owner from the report contract instead of trusting
    # caller-supplied routing fields in a durable log.
    findings = _safe_findings(build_triage_report(raw_findings)["findings"])
    handoff_pack_ids = (
        [pack_id for pack_id in session.get("handoff_pack_ids", [])
         if isinstance(pack_id, str) and pack_id.strip()]
        if isinstance(session.get("handoff_pack_ids"), list) else []
    )

    # Fail closed when a caller supplies a routed outcome or pack id without a
    # complete actionable finding owned by that destination. In particular,
    # truncated evidence must never survive into the durable log as a handoff.
    owner_for_outcome = {
        "handed_off_optimizer": "sql-optimizer",
        "handed_off_enforcer": "sql-plan-enforcer",
        "escalated_human": "human",
    }.get(outcome)
    if owner_for_outcome and not _has_actionable_owner(findings, owner_for_outcome):
        outcome = "inconclusive"
    if (
        outcome != "handed_off_optimizer"
        or not _has_actionable_owner(findings, "sql-optimizer")
    ):
        handoff_pack_ids = []

    return {
        "id": record_id,
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": str(session.get("environment", "unknown")),
        "mode": str(session.get("mode", "triage")),
        "symptom": str(session.get("symptom", "")),
        "outcome": outcome,
        "findings": findings,
        "handoff_pack_ids": handoff_pack_ids,
        "notes": str(session.get("notes", "")),
        "detail_file": f"runs/{record_id}.md",
    }


def render_detail(record: dict) -> str:
    frontmatter = json.dumps(record, indent=2, ensure_ascii=False)
    findings = record.get("findings") or []
    lines = [
        f"---\n{frontmatter}\n---\n",
        f"# Triage session `{record['id']}`\n",
        f"- environment: `{record['environment']}`  ·  mode: `{record['mode']}`  "
        f"·  outcome: `{record['outcome']}`\n",
        f"## Symptom\n\n{record.get('symptom') or '_None recorded (health sweep)._'}\n",
        "## Findings\n",
    ]
    if findings:
        for finding in findings:
            label = f"{finding.get('domain')}/{finding.get('metric')}"
            if _is_truncated_finding(finding) or finding.get("status") == "inconclusive":
                lines.append(f"- **inconclusive** {label} — {finding.get('summary')}")
                lines.append(f"  {INCONCLUSIVE_NEXT_STEP} No owner handoff.")
            elif finding.get("severity") in ACTIONABLE_SEVERITIES:
                lines.append(
                    f"- **{finding.get('severity')}** {label}"
                    f" — {finding.get('summary')} → {finding.get('recommended_action')}"
                    f" (owner: {finding.get('owner')})"
                )
            else:
                lines.append(f"- **info** {label} — {finding.get('summary')}")
                lines.append("  Informational only — no corrective action or owner handoff.")
    else:
        lines.append("_None — nothing crossed a triage threshold._")
    if record.get("handoff_pack_ids"):
        lines.append("\n## Handoff packs\n")
        lines.extend(f"- `{pack_id}`" for pack_id in record["handoff_pack_ids"])
    if record.get("notes"):
        lines.append(f"\n## Notes\n\n{record['notes']}")
    return "\n".join(lines) + "\n"


def _absolute(path: pathlib.Path | str) -> pathlib.Path:
    return pathlib.Path(os.path.abspath(pathlib.Path(path).expanduser()))


def _reject_symlink_components(path: pathlib.Path) -> None:
    current = pathlib.Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            raise OSError(f"refusing symlinked triage storage path: {current}")


def _secure_file(path: pathlib.Path | str) -> pathlib.Path:
    path = _absolute(path)
    _reject_symlink_components(path)
    if path.exists() and not path.is_file():
        raise OSError(f"triage storage path is not a file: {path}")
    if path.exists():
        path.chmod(AUDIT_FILE_MODE)
    return path


def _restrictive_directory(path: pathlib.Path | str) -> pathlib.Path:
    path = _absolute(path)
    _reject_symlink_components(path)
    missing = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    path.mkdir(parents=True, exist_ok=True, mode=AUDIT_DIR_MODE)
    _reject_symlink_components(path)
    if not path.is_dir():
        raise OSError(f"triage storage path is not a directory: {path}")
    path.chmod(AUDIT_DIR_MODE)
    for created in missing:
        if created.is_symlink() or not created.is_dir():
            raise OSError(f"triage storage path is not a directory: {created}")
        created.chmod(AUDIT_DIR_MODE)
    return path


def _durable_write(path: pathlib.Path, text: str, *, exclusive: bool = False) -> None:
    path = _secure_file(path)
    _restrictive_directory(path.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if exclusive:
        flags |= os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, AUDIT_FILE_MODE)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        # fdopen owns the descriptor after it succeeds; this is only needed
        # when descriptor wrapping itself fails.
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    path.chmod(AUDIT_FILE_MODE)


def _durable_append(path: pathlib.Path, text: str) -> None:
    path = _secure_file(path)
    _restrictive_directory(path.parent)
    fd = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
        AUDIT_FILE_MODE,
    )
    try:
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    path.chmod(AUDIT_FILE_MODE)


def _fsync_directory(path: pathlib.Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # File fsyncs are still performed on platforms that do not fsync dirs.
        pass
    finally:
        os.close(fd)


@contextmanager
def _audit_lock(root: pathlib.Path):
    root = _restrictive_directory(root)
    lock_path = root / ".lock"
    _secure_file(lock_path)
    fd = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        AUDIT_FILE_MODE,
    )
    handle = os.fdopen(fd, "a+", encoding="utf-8")
    try:
        lock_path.chmod(AUDIT_FILE_MODE)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def ensure_corpus(root: pathlib.Path) -> None:
    _restrictive_directory(root)
    _restrictive_directory(root / "runs")
    gitignore = root / ".gitignore"
    if not gitignore.exists():
        _durable_write(gitignore, _GITIGNORE, exclusive=True)
    else:
        _secure_file(gitignore)
    readme = root / "README.md"
    if not readme.exists():
        _durable_write(readme, _README, exclusive=True)
    else:
        _secure_file(readme)


def write_triage(session: dict, root: pathlib.Path | None = None) -> dict | None:
    """Persist one triage session. Returns the record, or None when logging is disabled."""
    if not audit_enabled():
        return None
    root = root or audit_dir()
    record = build_record(session)

    problems = validate(record)
    if problems:
        raise ValueError("triage record is invalid: " + "; ".join(problems))

    with _audit_lock(root):
        ensure_corpus(root)
        detail_path = root / record["detail_file"]
        _durable_write(detail_path, render_detail(record), exclusive=True)
        _durable_append(
            root / "index.jsonl",
            json.dumps(record, ensure_ascii=False) + "\n",
        )
        _fsync_directory(detail_path.parent)
        _fsync_directory(root)
    return record


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Record one triage session (best-effort).")
    parser.add_argument("--input", "-i", default="-", help="Session JSON; '-' for stdin.")
    args = parser.parse_args(argv[1:])

    try:
        raw = (
            sys.stdin.read()
            if args.input == "-"
            else pathlib.Path(args.input).read_text("utf-8")
        )
        session = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"could not parse session document: {exc}", file=sys.stderr)
        return 1

    try:
        record = write_triage(session)
    except (ValueError, OSError) as exc:
        # Best-effort: the caller surfaces this one line and never blocks the triage.
        print(f"triage log write failed: {exc}", file=sys.stderr)
        return 1

    if record is None:
        print("triage logging disabled (SQL_HEALTH_TRIAGE_AUDIT=0); nothing recorded")
        return 0
    print(f"triage {record['id']} ({record['outcome']}) -> {record['detail_file']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
