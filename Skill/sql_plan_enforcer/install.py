#!/usr/bin/env python3
"""Install the sql_plan_enforcer Copilot skill bundle.

Companion to sql_optimizer: install that skill alongside this one, since the enforcer
reuses its StyleGuide / SchemaGuide / Query-Store-hint conventions by reference.
"""

from __future__ import annotations

import pathlib
import shutil
import sys


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
    "review_report.py",
    "verify_decision.py",
    "authorization.py",
    "enforcement_ledger.py",
    "coverage_state.py",
)


def main() -> int:
    source_dir = pathlib.Path(__file__).parent.resolve()
    dest_dir = pathlib.Path.home() / ".copilot" / "skills" / "sql_plan_enforcer"

    missing = [name for name in SKILL_FILES if not (source_dir / name).exists()]
    if missing:
        print(f"Missing required skill files: {', '.join(missing)}", file=sys.stderr)
        return 1

    dest_dir.mkdir(parents=True, exist_ok=True)
    for name in SKILL_FILES:
        shutil.copy2(source_dir / name, dest_dir / name)

    # Prune stale top-level files from older installs; preserve directories such as the
    # ledger corpus (audits/) and __pycache__.
    keep = set(SKILL_FILES)
    pruned = []
    for entry in dest_dir.iterdir():
        if entry.is_file() and entry.name not in keep:
            entry.unlink()
            pruned.append(entry.name)

    print(f"Installed sql_plan_enforcer skill bundle to: {dest_dir}")
    if pruned:
        print(f"Pruned stale files: {', '.join(sorted(pruned))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
