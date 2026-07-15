#!/usr/bin/env python3
"""Install the sql_plan_enforcer Copilot skill."""

from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import sys
import tempfile
import uuid


SKILL_NAME = "sql_plan_enforcer"
SKILL_FILES = ("SKILL.md",)
SKILL_DIRS: tuple[str, ...] = ()
KNOWN_BUNDLES = ("sql_optimizer", "sql_plan_enforcer", "sql_health_triage")


def resolve_dest(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    configured = os.environ.get("SQL_SKILLS_DEST")
    if configured:
        return pathlib.Path(configured).expanduser()
    for host in (".copilot", ".claude"):
        candidate = pathlib.Path.home() / host / "skills"
        if (candidate / SKILL_NAME).is_dir():
            return candidate
    for host in (".copilot", ".claude"):
        candidate = pathlib.Path.home() / host / "skills"
        if candidate.is_dir() or candidate.parent.is_dir():
            return candidate
    return pathlib.Path.home() / ".copilot" / "skills"


def _remove(path: pathlib.Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def install(skills_root: pathlib.Path) -> pathlib.Path:
    source = pathlib.Path(__file__).resolve().parent / "SKILL.md"
    if source.is_symlink() or not source.is_file():
        raise RuntimeError(f"Missing or unsafe source file: {source}")
    skills_root.mkdir(parents=True, exist_ok=True)
    if skills_root.is_symlink() or not skills_root.is_dir():
        raise RuntimeError(f"Refusing unsafe skills root: {skills_root}")
    destination = skills_root / SKILL_NAME
    if destination.is_symlink():
        raise RuntimeError(f"Refusing symbolic-link destination: {destination}")

    stage_root = pathlib.Path(tempfile.mkdtemp(prefix=f".{SKILL_NAME}.stage-", dir=skills_root))
    staged = stage_root / SKILL_NAME
    staged.mkdir(mode=0o700)
    shutil.copy2(source, staged / "SKILL.md")
    backup = skills_root / f".{SKILL_NAME}.backup-{uuid.uuid4().hex}"
    moved_existing = False
    try:
        if destination.exists():
            os.replace(destination, backup)
            moved_existing = True
        os.replace(staged, destination)
    except Exception:
        if destination.exists() or destination.is_symlink():
            _remove(destination)
        if moved_existing and backup.exists():
            os.replace(backup, destination)
        raise
    else:
        if backup.exists():
            _remove(backup)
    finally:
        if stage_root.exists():
            shutil.rmtree(stage_root)
    return destination


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=f"Install {SKILL_NAME}.")
    parser.add_argument("--dest", help="Destination skills root.")
    args = parser.parse_args(argv)
    try:
        destination = install(resolve_dest(args.dest))
    except Exception as exc:
        print(f"Install failed: {exc}", file=sys.stderr)
        return 1
    print(f"Installed {SKILL_NAME} to {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
