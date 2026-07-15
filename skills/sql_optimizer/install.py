#!/usr/bin/env python3
"""Install the sql_optimizer skill bundle."""

from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import sys


SKILL_NAME = "sql_optimizer"
KNOWN_BUNDLES = ("sql_optimizer", "sql_plan_enforcer", "sql_health_triage")


SKILL_FILES = (
    "SKILL.md",
    "queryguide.md",
    "IndexingGuide.md",
    "SchemaGuide.md",
    "StyleGuide.md",
    "RunGuide.md",
    "Examples.md",
    "AuditGuide.md",
    "ImproveGuide.md",
    "IntakeGuide.md",
    "SandboxGuide.md",
    "main.txt",
    "record_audit.py",
    "validate_audit.py",
    "summarize_audits.py",
    "test_index_ledger.py",
    "storage_lock.py",
    "optimizer_storage.py",
)

SKILL_DIRS = (
    "sources",
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


def _ignore_runtime_entries(_directory: str, names: list[str]) -> set[str]:
    return {
        name for name in names
        if name == ".DS_Store" or name == "__pycache__" or _is_credential_name(name)
    }


def _source_problems(source_dir: pathlib.Path) -> list[str]:
    problems = []
    for name in SKILL_FILES:
        path = source_dir / name
        if _is_credential_name(name) or path.is_symlink() or not path.is_file():
            problems.append(name)
    for name in SKILL_DIRS:
        directory = source_dir / name
        if _is_credential_name(name) or directory.is_symlink() or not directory.is_dir():
            problems.append(name)
            continue
        for current, directories, files in os.walk(directory, followlinks=False):
            current_path = pathlib.Path(current)
            for entry in tuple(directories) + tuple(files):
                path = current_path / entry
                if _is_credential_name(entry) or path.is_symlink():
                    problems.append(path.relative_to(source_dir).as_posix())
    return problems


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

    missing = _source_problems(source_dir)
    if missing:
        print(f"Missing required skill files: {', '.join(missing)}", file=sys.stderr)
        return 1

    if dest_dir.is_symlink() or any(
        (dest_dir / name).is_symlink() for name in SKILL_FILES + SKILL_DIRS
    ):
        print(f"Refusing symbolic link in install destination: {dest_dir}", file=sys.stderr)
        return 1

    dest_dir.mkdir(parents=True, exist_ok=True)
    for name in SKILL_FILES:
        shutil.copy2(source_dir / name, dest_dir / name)
    for name in SKILL_DIRS:
        target = dest_dir / name
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(
            source_dir / name,
            target,
            ignore=_ignore_runtime_entries,
        )

    # Prune stale skill files left by older installs (e.g. removed guides) so the
    # active skill matches source. Only top-level files are pruned — directories
    # such as the audit corpus (audits/) and __pycache__ are preserved.
    keep = set(SKILL_FILES) | set(SKILL_DIRS)
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
