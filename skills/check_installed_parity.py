#!/usr/bin/env python3
"""Verify exact installed SQL skill parity and retired-surface absence."""

from __future__ import annotations

import argparse
import filecmp
import os
import pathlib
import sys
from collections.abc import Sequence

ROOT = pathlib.Path(__file__).resolve().parent
ACTIVE_BUNDLES = ("sql_optimizer", "sql_plan_enforcer", "sql_health_triage")
LEARNING_PACK = pathlib.Path("knowledge") / "azure-sql-mcp-learning-pack.json"
RETIRED_BUNDLE = "_".join(("query", "geneva", "db"))  # noqa: FLY002
HOST_SKILL_DIRS = (
    pathlib.Path(".copilot/skills"),
    pathlib.Path(".claude/skills"),
    pathlib.Path(".agents/skills"),
    pathlib.Path(".codex/skills"),
)


def resolve_dest(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    configured = os.environ.get("SQL_SKILLS_DEST")
    if configured:
        return pathlib.Path(configured).expanduser()
    for host in (".copilot", ".claude"):
        candidate = pathlib.Path.home() / host / "skills"
        if all((candidate / bundle).is_dir() for bundle in ACTIVE_BUNDLES):
            return candidate
    return pathlib.Path.home() / ".copilot" / "skills"


def default_retired_wrapper() -> pathlib.Path:
    return pathlib.Path.home() / ".local" / "bin" / RETIRED_BUNDLE


def discoverable_skill_roots(skills_root: pathlib.Path) -> tuple[pathlib.Path, ...]:
    candidates = [skills_root.expanduser()]
    candidates.extend(pathlib.Path.home() / relative for relative in HOST_SKILL_DIRS)
    return tuple(dict.fromkeys(candidates))


def find_retired_skill_paths(skills_root: pathlib.Path) -> tuple[pathlib.Path, ...]:
    if not skills_root.exists():
        return ()
    found: list[pathlib.Path] = []
    for current, directories, _files in os.walk(skills_root, followlinks=False):
        current_path = pathlib.Path(current)
        kept: list[str] = []
        for name in directories:
            child = current_path / name
            if name == RETIRED_BUNDLE:
                found.append(child)
            elif child.is_symlink():
                continue
            else:
                kept.append(name)
        directories[:] = kept
    return tuple(sorted(set(found)))


def compare_install(
    skills_root: pathlib.Path,
    *,
    retired_wrapper: pathlib.Path | None = None,
    discovery_roots: Sequence[pathlib.Path] | None = None,
) -> list[str]:
    problems: list[str] = []
    for bundle in ACTIVE_BUNDLES:
        source = ROOT / bundle / "SKILL.md"
        destination = skills_root / bundle
        if destination.is_symlink():
            problems.append(f"{bundle}: installed directory is a symbolic link")
            continue
        if not destination.is_dir():
            problems.append(f"{bundle}: installed directory missing")
            continue
        entries = sorted(entry.name for entry in destination.iterdir())
        if entries != ["SKILL.md"]:
            problems.append(
                f"{bundle}: runtime must contain only SKILL.md; found {', '.join(entries) or 'nothing'}"
            )
            continue
        installed_skill = destination / "SKILL.md"
        if installed_skill.is_symlink():
            problems.append(f"{bundle}: SKILL.md is a symbolic link")
        elif not filecmp.cmp(source, installed_skill, shallow=False):
            problems.append(f"{bundle}: SKILL.md differs from source")

    installed_pack = skills_root / LEARNING_PACK
    if installed_pack.exists() or installed_pack.is_symlink():
        problems.append(
            "learning pack must remain a reviewed Git artifact; it must not be installed"
        )

    roots = discovery_roots or discoverable_skill_roots(skills_root)
    for discovery_root in roots:
        for retired in find_retired_skill_paths(
            pathlib.Path(discovery_root).expanduser()
        ):
            problems.append(f"retired skill remains discoverable: {retired}")

    wrapper = (retired_wrapper or default_retired_wrapper()).expanduser()
    if wrapper.exists() or wrapper.is_symlink():
        problems.append(f"retired PATH wrapper remains discoverable: {wrapper}")

    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check exact installed SQL skill parity.")
    parser.add_argument("--dest", help="Destination skills root.")
    parser.add_argument("--retired-wrapper", help="Obsolete PATH wrapper path to verify absent.")
    args = parser.parse_args(argv)

    skills_root = resolve_dest(args.dest)
    wrapper = (
        pathlib.Path(args.retired_wrapper).expanduser()
        if args.retired_wrapper
        else default_retired_wrapper()
    )
    problems = compare_install(skills_root, retired_wrapper=wrapper)
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1

    print(f"Installed skill parity OK ({skills_root}): {', '.join(ACTIVE_BUNDLES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
