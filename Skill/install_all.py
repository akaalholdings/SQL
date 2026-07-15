#!/usr/bin/env python3
"""Stage and transactionally install all SQL skill bundles."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import importlib.util
import os
import pathlib
import shutil
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parent
BUNDLES = ("sql_optimizer", "sql_plan_enforcer", "sql_health_triage")
_IGNORED_NAMES = {".DS_Store", "__pycache__"}


@dataclass(frozen=True)
class BundleSpec:
    name: str
    installer: object
    source_dir: pathlib.Path
    skill_files: tuple[str, ...]
    skill_dirs: tuple[str, ...]


@dataclass(frozen=True)
class JournalEntry:
    destination: pathlib.Path
    backup: pathlib.Path | None


def _is_credential_name(name: str) -> bool:
    lowered = pathlib.Path(name).name.lower()
    return (
        lowered == ".env"
        or lowered.startswith(".env.")
        or lowered.startswith("credentials")
        or lowered.startswith("secret")
        or lowered.endswith((".pem", ".key", ".p12", ".pfx"))
    )


def _is_ignored_name(name: str) -> bool:
    return name in _IGNORED_NAMES or _is_credential_name(name)


def _ignore_runtime_entries(_directory: str, names: list[str]) -> set[str]:
    return {name for name in names if _is_ignored_name(name)}


def _load_installer(bundle: str):
    path = ROOT / bundle / "install.py"
    spec = importlib.util.spec_from_file_location(f"{bundle}_install_all", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load installer: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _source_problems(spec: BundleSpec) -> list[str]:
    problems: list[str] = []
    for name in spec.skill_files:
        source = spec.source_dir / name
        if _is_credential_name(name):
            problems.append(f"{spec.name}: credential file declaration refused: {name}")
        elif source.is_symlink():
            problems.append(f"{spec.name}: symbolic link source refused: {name}")
        elif not source.is_file():
            problems.append(f"{spec.name}: source file missing: {name}")

    for name in spec.skill_dirs:
        directory = spec.source_dir / name
        if _is_credential_name(name):
            problems.append(f"{spec.name}: credential directory declaration refused: {name}")
            continue
        if directory.is_symlink():
            problems.append(f"{spec.name}: symbolic link source refused: {name}")
            continue
        if not directory.is_dir():
            problems.append(f"{spec.name}: source directory missing: {name}")
            continue
        for current, directories, files in os.walk(directory, followlinks=False):
            current_path = pathlib.Path(current)
            retained_directories: list[str] = []
            for entry in directories:
                path = current_path / entry
                relative = path.relative_to(spec.source_dir).as_posix()
                if entry in _IGNORED_NAMES:
                    continue
                if _is_credential_name(entry):
                    problems.append(
                        f"{spec.name}: credential path in declared tree refused: {relative}"
                    )
                    continue
                if path.is_symlink():
                    problems.append(
                        f"{spec.name}: symbolic link source refused: {relative}"
                    )
                    continue
                retained_directories.append(entry)
            directories[:] = retained_directories

            for filename in files:
                if filename in _IGNORED_NAMES:
                    continue
                path = current_path / filename
                relative = path.relative_to(spec.source_dir).as_posix()
                if _is_credential_name(filename):
                    problems.append(
                        f"{spec.name}: credential path in declared tree refused: {relative}"
                    )
                elif path.is_symlink():
                    problems.append(
                        f"{spec.name}: symbolic link source refused: {relative}"
                    )
                elif not path.is_file():
                    problems.append(
                        f"{spec.name}: declared tree entry is not a file: {relative}"
                    )
    return problems


def _preflight() -> tuple[list[BundleSpec], list[str]]:
    specs: list[BundleSpec] = []
    problems: list[str] = []
    for bundle in BUNDLES:
        try:
            installer = _load_installer(bundle)
        except (OSError, ImportError, RuntimeError) as exc:
            problems.append(f"{bundle}: could not load installer: {exc}")
            continue

        source_dir = ROOT / bundle
        skill_files = tuple(getattr(installer, "SKILL_FILES", ()))
        skill_dirs = tuple(getattr(installer, "SKILL_DIRS", ()))
        bundle_spec = BundleSpec(bundle, installer, source_dir, skill_files, skill_dirs)
        problems.extend(_source_problems(bundle_spec))
        specs.append(bundle_spec)
    return specs, problems


def _stage_bundle(spec: BundleSpec, stage_root: pathlib.Path) -> pathlib.Path:
    problems = _source_problems(spec)
    if problems:
        raise ValueError("; ".join(problems))
    staged_bundle = stage_root / spec.name
    staged_bundle.mkdir(parents=True)
    for name in spec.skill_files:
        source = spec.source_dir / name
        target = staged_bundle / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    for name in spec.skill_dirs:
        shutil.copytree(
            spec.source_dir / name,
            staged_bundle / name,
            ignore=_ignore_runtime_entries,
        )
    return staged_bundle


def _lexists(path: pathlib.Path) -> bool:
    return path.exists() or path.is_symlink()


def _remove_path(path: pathlib.Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def _backup_path(
    backup_root: pathlib.Path,
    bundle: str,
    relative: pathlib.Path,
) -> pathlib.Path:
    return backup_root / bundle / relative


def _backup_existing(
    destination: pathlib.Path,
    backup: pathlib.Path,
    journal: list[JournalEntry],
) -> JournalEntry:
    if not _lexists(destination):
        entry = JournalEntry(destination, None)
        journal.append(entry)
        return entry
    backup.parent.mkdir(parents=True, exist_ok=True)
    os.replace(destination, backup)
    entry = JournalEntry(destination, backup)
    journal.append(entry)
    return entry


def _ensure_directory(
    destination: pathlib.Path,
    backup: pathlib.Path,
    journal: list[JournalEntry],
) -> None:
    if _lexists(destination):
        if destination.is_dir() and not destination.is_symlink():
            return
        _backup_existing(destination, backup, journal)
        destination.mkdir()
        return
    destination.mkdir()
    journal.append(JournalEntry(destination, None))


def _ensure_parent_directories(
    destination_root: pathlib.Path,
    relative_parent: pathlib.Path,
    backup_root: pathlib.Path,
    bundle: str,
    journal: list[JournalEntry],
) -> None:
    current = destination_root
    relative = pathlib.Path()
    for part in relative_parent.parts:
        relative /= part
        current /= part
        _ensure_directory(
            current,
            _backup_path(backup_root, bundle, relative),
            journal,
        )


def _replace_staged(
    staged: pathlib.Path,
    destination: pathlib.Path,
    backup: pathlib.Path,
    journal: list[JournalEntry],
) -> None:
    # Never move a directory out of the way for a file replacement: it could
    # contain unmanaged private state that must remain untouched.
    if destination.is_dir() and not destination.is_symlink():
        raise OSError(f"cannot replace directory with file: {destination}")
    _backup_existing(destination, backup, journal)
    os.replace(staged, destination)


def _remove_stale(
    destination: pathlib.Path,
    backup: pathlib.Path,
    journal: list[JournalEntry],
) -> None:
    if _lexists(destination):
        _backup_existing(destination, backup, journal)


def _stage_tree_files(
    stage_dir: pathlib.Path,
) -> tuple[list[pathlib.Path], list[pathlib.Path]]:
    directories: list[pathlib.Path] = []
    files: list[pathlib.Path] = []
    for current, dirnames, filenames in os.walk(stage_dir, followlinks=False):
        dirnames[:] = [name for name in dirnames if not _is_ignored_name(name)]
        current_path = pathlib.Path(current)
        directories.extend(current_path / name for name in dirnames)
        files.extend(
            current_path / name
            for name in filenames
            if not _is_ignored_name(name)
        )
    return directories, files


def _sync_declared_tree(
    spec: BundleSpec,
    stage_dir: pathlib.Path,
    destination_dir: pathlib.Path,
    backup_root: pathlib.Path,
    journal: list[JournalEntry],
) -> None:
    declared_relative_root = pathlib.Path(stage_dir.name)
    _ensure_directory(
        destination_dir,
        backup_root / spec.name / declared_relative_root,
        journal,
    )
    source_directories, source_files = _stage_tree_files(stage_dir)
    source_relative_directories = {
        path.relative_to(stage_dir) for path in source_directories
    }
    source_relative_files = {
        path.relative_to(stage_dir) for path in source_files
    }

    for path in sorted(source_directories, key=lambda item: len(item.parts)):
        relative = path.relative_to(stage_dir)
        _ensure_directory(
            destination_dir / relative,
            backup_root / spec.name / declared_relative_root / relative,
            journal,
        )
    for path in sorted(source_files):
        relative = path.relative_to(stage_dir)
        _ensure_parent_directories(
            destination_dir,
            relative.parent,
            backup_root,
            spec.name,
            journal,
        )
        _replace_staged(
            path,
            destination_dir / relative,
            backup_root / spec.name / declared_relative_root / relative,
            journal,
        )

    for current, dirnames, filenames in os.walk(destination_dir, followlinks=False):
        dirnames[:] = [name for name in dirnames if not _is_ignored_name(name)]
        current_path = pathlib.Path(current)
        for filename in filenames:
            if _is_ignored_name(filename):
                continue
            destination_file = current_path / filename
            relative = destination_file.relative_to(destination_dir)
            if relative not in source_relative_files:
                _remove_stale(
                    destination_file,
                    backup_root / spec.name / declared_relative_root / relative,
                    journal,
                )

    # Remove empty stale directories and stale symlinks bottom-up. Runtime state
    # lives outside declared trees; credential-named entries are always preserved.
    for current, dirnames, _filenames in os.walk(
        destination_dir, topdown=False, followlinks=False
    ):
        current_path = pathlib.Path(current)
        for dirname in dirnames:
            path = current_path / dirname
            relative = path.relative_to(destination_dir)
            if _is_credential_name(dirname) or relative in source_relative_directories:
                continue
            if path.is_symlink() or (path.is_dir() and not any(path.iterdir())):
                _remove_stale(
                    path,
                    backup_root / spec.name / declared_relative_root / relative,
                    journal,
                )


def _commit_bundle(
    spec: BundleSpec,
    staged_bundle: pathlib.Path,
    destination_root: pathlib.Path,
    backup_root: pathlib.Path,
    journal: list[JournalEntry],
) -> None:
    destination_bundle = destination_root / spec.name
    if _lexists(destination_bundle):
        if not destination_bundle.is_dir() or destination_bundle.is_symlink():
            raise OSError(f"installed bundle path is not a directory: {destination_bundle}")
    else:
        destination_bundle.mkdir(parents=True)
        journal.append(JournalEntry(destination_bundle, None))

    declared_files = {pathlib.Path(name) for name in spec.skill_files}
    for name in spec.skill_files:
        relative = pathlib.Path(name)
        _replace_staged(
            staged_bundle / relative,
            destination_bundle / relative,
            _backup_path(backup_root, spec.name, relative),
            journal,
        )

    for name in spec.skill_dirs:
        relative = pathlib.Path(name)
        _sync_declared_tree(
            spec,
            staged_bundle / relative,
            destination_bundle / relative,
            backup_root,
            journal,
        )

    for entry in destination_bundle.iterdir():
        if _is_credential_name(entry.name):
            continue
        relative = entry.relative_to(destination_bundle)
        if entry.is_symlink() and relative not in declared_files and relative not in {
            pathlib.Path(name) for name in spec.skill_dirs
        }:
            raise OSError(f"refusing unmanaged symbolic link in installed bundle: {entry}")
        if entry.is_dir():
            continue
        if relative not in declared_files:
            _remove_stale(
                entry,
                _backup_path(backup_root, spec.name, relative),
                journal,
            )


def _rollback(journal: list[JournalEntry]) -> None:
    rollback_errors: list[Exception] = []
    for entry in reversed(journal):
        try:
            if _lexists(entry.destination):
                _remove_path(entry.destination)
            if entry.backup is not None:
                entry.destination.parent.mkdir(parents=True, exist_ok=True)
                os.replace(entry.backup, entry.destination)
        except (OSError, ValueError) as exc:
            rollback_errors.append(exc)
    if rollback_errors:
        raise RuntimeError(f"rollback failed for {len(rollback_errors)} path(s)")


def _install(specs: list[BundleSpec], destination: pathlib.Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage_root = pathlib.Path(tempfile.mkdtemp(
        prefix=".sql-skills-stage-",
        dir=destination.parent,
    ))
    backup_root = pathlib.Path(tempfile.mkdtemp(
        prefix=".sql-skills-backup-",
        dir=destination.parent,
    ))
    journal: list[JournalEntry] = []
    try:
        staged = {spec.name: _stage_bundle(spec, stage_root) for spec in specs}
        if _lexists(destination):
            if not destination.is_dir() or destination.is_symlink():
                raise OSError(f"skills destination is not a directory: {destination}")
        else:
            destination.mkdir()
            journal.append(JournalEntry(destination, None))
        for spec in specs:
            _commit_bundle(
                spec,
                staged[spec.name],
                destination,
                backup_root,
                journal,
            )
    except (OSError, RuntimeError, ValueError) as exc:
        try:
            _rollback(journal)
        except RuntimeError as rollback_error:
            print(f"install failed: {exc}; {rollback_error}", file=sys.stderr)
            return 1
        print(f"install failed: {exc}", file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(stage_root, ignore_errors=True)
        shutil.rmtree(backup_root, ignore_errors=True)

    for spec in specs:
        print(f"Installed {spec.name} skill bundle to: {destination / spec.name}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install all SQL skill bundles.")
    parser.add_argument("--dest", required=True, help="Skills root to install into.")
    args = parser.parse_args(argv)
    destination = pathlib.Path(args.dest).expanduser()

    specs, problems = _preflight()
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1
    return _install(specs, destination)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
