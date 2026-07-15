#!/usr/bin/env python3
"""Verify source skill bundles match the installed copies.

The install destination resolves the same way the installers do:
``--dest`` > ``$SQL_SKILLS_DEST`` > existing host dir (``~/.claude/skills`` then
``~/.copilot/skills``) > ``~/.claude/skills``.
"""

from __future__ import annotations

import argparse
import filecmp
import importlib.util
import os
import pathlib
import sys
from types import ModuleType


ROOT = pathlib.Path(__file__).resolve().parent
# Parity is intentionally limited to the maintained collection. Archived
# bundles under legacy/ are neither installed nor checked here.
ACTIVE_BUNDLES = ("sql_optimizer", "sql_plan_enforcer", "sql_health_triage")
_IGNORED_NAMES = {".DS_Store", "__pycache__"}


def _is_credential_name(name: str) -> bool:
    lowered = pathlib.Path(name).name.lower()
    return (
        lowered == ".env"
        or lowered.startswith(".env.")
        or lowered.startswith("credentials")
        or lowered.startswith("secret")
        or lowered.endswith((".pem", ".key", ".p12", ".pfx"))
    )


def _ignored_relative(path: pathlib.Path) -> bool:
    return any(
        part in _IGNORED_NAMES or _is_credential_name(part)
        for part in path.parts
    )


def resolve_dest(explicit: str | None = None) -> pathlib.Path:
    """Skills root, mirroring each bundle's install.py resolution."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    env = os.environ.get("SQL_SKILLS_DEST")
    if env:
        return pathlib.Path(env).expanduser()
    for host_dir in (".claude", ".copilot"):
        candidate = pathlib.Path.home() / host_dir / "skills"
        if sum((candidate / bundle).is_dir() for bundle in ACTIVE_BUNDLES) == len(ACTIVE_BUNDLES):
            return candidate
    populated: list[tuple[int, pathlib.Path]] = []
    for host_dir in (".claude", ".copilot"):
        candidate = pathlib.Path.home() / host_dir / "skills"
        count = sum((candidate / bundle).is_dir() for bundle in ACTIVE_BUNDLES)
        if count:
            populated.append((count, candidate))
    if populated:
        return max(populated, key=lambda item: item[0])[1]
    for host_dir in (".claude", ".copilot"):
        candidate = pathlib.Path.home() / host_dir / "skills"
        if candidate.is_dir():
            return candidate
    for host_dir in (".claude", ".copilot"):
        candidate = pathlib.Path.home() / host_dir / "skills"
        if candidate.parent.is_dir():
            return candidate
    return pathlib.Path.home() / ".claude" / "skills"


def _load_install_module(bundle: str) -> ModuleType:
    path = ROOT / bundle / "install.py"
    spec = importlib.util.spec_from_file_location(f"{bundle}_install", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load installer for {bundle}: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _compare_bundle(bundle: str, skills_root: pathlib.Path) -> list[str]:
    source_dir = ROOT / bundle
    dest_dir = skills_root / bundle
    install = _load_install_module(bundle)
    skill_files = tuple(install.SKILL_FILES)
    skill_dirs = tuple(getattr(install, "SKILL_DIRS", ()))
    problems: list[str] = []

    if dest_dir.is_symlink():
        return [f"{bundle}: installed bundle directory is a symbolic link: {dest_dir}"]
    if not dest_dir.exists():
        return [f"{bundle}: installed directory missing: {dest_dir}"]
    if not dest_dir.is_dir():
        return [f"{bundle}: installed bundle path is not a directory: {dest_dir}"]

    expected: dict[pathlib.Path, pathlib.Path] = {}
    expected_dirs: set[pathlib.Path] = set()
    top_level_files: set[pathlib.Path] = set()
    for name in skill_files:
        relative = pathlib.Path(name)
        if _ignored_relative(relative):
            continue
        source = source_dir / relative
        dest = dest_dir / relative
        if source.is_symlink():
            problems.append(f"{bundle}: symbolic link source refused: {name}")
            continue
        if not source.is_file():
            problems.append(f"{bundle}: source file missing: {name}")
            continue
        expected[relative] = source
        if len(relative.parts) == 1:
            top_level_files.add(relative)

    declared_dirs: list[pathlib.Path] = []
    for name in skill_dirs:
        relative = pathlib.Path(name)
        source = source_dir / relative
        dest = dest_dir / relative
        declared_dirs.append(relative)
        if source.is_symlink():
            problems.append(f"{bundle}: symbolic link source refused: {name}")
            continue
        if not source.is_dir():
            problems.append(f"{bundle}: source directory missing: {name}")
            continue
        if not dest.is_dir():
            problems.append(f"{bundle}: installed directory missing: {name}")
            continue
        for path in source.rglob("*"):
            nested = path.relative_to(source_dir)
            if _ignored_relative(nested):
                continue
            if path.is_symlink():
                problems.append(
                    f"{bundle}: symbolic link source refused: {nested.as_posix()}"
                )
            elif path.is_dir():
                expected_dirs.add(nested)
            elif path.is_file():
                expected[nested] = path

    for relative, source in sorted(expected.items()):
        dest = dest_dir / relative
        label = relative.as_posix()
        if dest.is_symlink():
            problems.append(f"{bundle}: installed path is a symbolic link: {label}")
        elif not dest.is_file():
            problems.append(f"{bundle}: installed file missing: {label}")
        elif not filecmp.cmp(source, dest, shallow=False):
            problems.append(f"{bundle}: installed file differs: {label}")

    allowed = {path.name for path in top_level_files}
    for entry in dest_dir.iterdir():
        if _is_credential_name(entry.name):
            continue
        if entry.is_file() and entry.name not in allowed:
            problems.append(f"{bundle}: stale installed top-level file: {entry.name}")

    declared_dir_prefixes = tuple(
        directory.parts for directory in declared_dirs
    )
    for path in dest_dir.rglob("*"):
        relative = path.relative_to(dest_dir)
        if _ignored_relative(relative):
            continue
        in_declared_tree = any(
            relative.parts[:len(prefix)] == prefix
            for prefix in declared_dir_prefixes
        )
        if not in_declared_tree:
            continue
        if path.is_symlink():
            problems.append(
                f"{bundle}: installed path is a symbolic link: {relative.as_posix()}"
            )
        elif path.is_file() and relative not in expected:
            problems.append(
                f"{bundle}: stale installed file: {relative.as_posix()}"
            )
        elif path.is_dir() and relative not in expected_dirs and relative not in declared_dirs:
            problems.append(
                f"{bundle}: stale installed directory: {relative.as_posix()}"
            )

    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check source vs installed skill parity.")
    parser.add_argument("--dest", default=None,
                        help="Skills root the bundles were installed into (default: auto-detect).")
    args = parser.parse_args(argv)

    skills_root = resolve_dest(args.dest)
    problems: list[str] = []
    for bundle in ACTIVE_BUNDLES:
        problems.extend(_compare_bundle(bundle, skills_root))

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1

    print(f"installed skill parity ok ({skills_root}): " + ", ".join(ACTIVE_BUNDLES))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
