#!/usr/bin/env python3
"""Install the sql_plan_enforcer skill bundle.

Companion to sql_optimizer (which consumes this skill's handoff queue) and
sql_health_triage (which enqueues into it): install all three, since the enforcer
reuses the optimizer's StyleGuide / SchemaGuide / Query-Store-hint conventions by
reference and owns the queue the other two share.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import sys


SKILL_NAME = "sql_plan_enforcer"
KNOWN_BUNDLES = ("sql_optimizer", "sql_plan_enforcer", "sql_health_triage")


SKILL_FILES = (
    "SKILL.md",
    "ScanGuide.md",
    "ReviewGuide.md",
    "EnforceGuide.md",
    "SafetyGuide.md",
    "RunGuide.md",
    "LoopGuide.md",
    "AuditGuide.md",
    "scan_rank.py",
    "scan_adapter.py",
    "review_report.py",
    "verify_decision.py",
    "authorization.py",
    "enforcement_ledger.py",
    "coverage_state.py",
    "handoff_queue.py",
    "storage_lock.py",
    "enforcer_storage.py",
)


def _is_credential_name(name: str) -> bool:
    lowered = pathlib.Path(name).name.lower()
    return (
        lowered == ".env"
        or lowered.startswith(".env.")
        or lowered.startswith("credentials")
        or lowered.startswith("secret")
        or lowered.endswith((".pem", ".key", ".p12", ".pfx"))
    )


def resolve_dest(explicit: str | None = None) -> pathlib.Path:
    """Destination skills root: --dest > $SQL_SKILLS_DEST > existing host dir > ~/.claude/skills."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    env = os.environ.get("SQL_SKILLS_DEST")
    if env:
        return pathlib.Path(env).expanduser()
    for host_dir in (".claude", ".copilot"):
        candidate = pathlib.Path.home() / host_dir / "skills"
        if (candidate / SKILL_NAME).is_dir():
            return candidate
    for host_dir in (".claude", ".copilot"):
        candidate = pathlib.Path.home() / host_dir / "skills"
        if any((candidate / bundle).is_dir() for bundle in KNOWN_BUNDLES):
            return candidate
    for host_dir in (".claude", ".copilot"):
        candidate = pathlib.Path.home() / host_dir / "skills"
        if candidate.is_dir():
            return candidate
    for host_dir in (".claude", ".copilot"):
        candidate = pathlib.Path.home() / host_dir / "skills"
        if candidate.parent.is_dir():
            return candidate
    return pathlib.Path.home() / ".claude" / "skills"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=f"Install the {SKILL_NAME} skill bundle.")
    parser.add_argument("--dest", default=None,
                        help="Skills root to install into (default: auto-detect host dir).")
    args = parser.parse_args(argv)

    source_dir = pathlib.Path(__file__).parent.resolve()
    dest_dir = resolve_dest(args.dest) / SKILL_NAME

    missing = [
        name for name in SKILL_FILES
        if _is_credential_name(name)
        or not (source_dir / name).is_file()
        or (source_dir / name).is_symlink()
    ]
    if missing:
        print(f"Missing required skill files: {', '.join(missing)}", file=sys.stderr)
        return 1

    if dest_dir.is_symlink() or any(
        (dest_dir / name).is_symlink() for name in SKILL_FILES
    ):
        print(f"Refusing symbolic link in install destination: {dest_dir}", file=sys.stderr)
        return 1

    dest_dir.mkdir(parents=True, exist_ok=True)
    for name in SKILL_FILES:
        shutil.copy2(source_dir / name, dest_dir / name)

    # Prune stale top-level files from older installs; preserve directories such as the
    # ledger corpus (audits/) and __pycache__.
    keep = set(SKILL_FILES)
    pruned = []
    for entry in dest_dir.iterdir():
        if entry.is_file() and entry.name not in keep and not _is_credential_name(entry.name):
            entry.unlink()
            pruned.append(entry.name)

    print(f"Installed {SKILL_NAME} skill bundle to: {dest_dir}")
    if pruned:
        print(f"Pruned stale files: {', '.join(sorted(pruned))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
