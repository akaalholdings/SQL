"""Release-tool regression tests for recursive parity and atomic installation."""

from __future__ import annotations

import importlib.util
import pathlib
import stat
import sys

import pytest


SKILL_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


install_all = _load_module("release_install_all", SKILL_ROOT / "install_all.py")
parity = _load_module("release_parity", SKILL_ROOT / "check_installed_parity.py")


def _install(destination: pathlib.Path) -> None:
    assert install_all.main(["--dest", str(destination)]) == 0


def _snapshot(root: pathlib.Path) -> dict[str, tuple[str, int, bytes | None]]:
    snapshot: dict[str, tuple[str, int, bytes | None]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        mode = stat.S_IMODE(path.stat().st_mode)
        if path.is_dir():
            snapshot[relative] = ("dir", mode, None)
        elif path.is_file():
            snapshot[relative] = ("file", mode, path.read_bytes())
    return snapshot


def test_parity_detects_missing_changed_and_stale_nested_files(tmp_path, capsys):
    destination = tmp_path / "skills"
    _install(destination)

    missing = destination / "sql_optimizer" / "sources" / "brentozar-indexing" / "manifest.json"
    missing.unlink()
    changed = destination / "sql_optimizer" / "sources" / "kendra-indexing" / "manifest.json"
    changed.write_text("changed", encoding="utf-8")
    stale = destination / "sql_optimizer" / "sources" / "nested-stale" / "drift.txt"
    stale.parent.mkdir()
    stale.write_text("stale", encoding="utf-8")

    assert parity.main(["--dest", str(destination)]) == 1
    errors = capsys.readouterr().err
    assert "installed file missing: sources/brentozar-indexing/manifest.json" in errors
    assert "installed file differs: sources/kendra-indexing/manifest.json" in errors
    assert "stale installed file: sources/nested-stale/drift.txt" in errors


def test_install_prunes_empty_stale_directory_inside_declared_tree(tmp_path):
    destination = tmp_path / "skills"
    _install(destination)
    stale = destination / "sql_optimizer" / "sources" / "empty-stale"
    stale.mkdir()

    _install(destination)

    assert not stale.exists()
    assert parity.main(["--dest", str(destination)]) == 0


def test_install_all_preserves_unmanaged_state_directories(tmp_path):
    destination = tmp_path / "skills"
    _install(destination)
    audit_state = destination / "sql_optimizer" / "audits" / "index.jsonl"
    experiment_state = destination / "sql_optimizer" / "experiments" / "records" / "pending.json"
    audit_state.parent.mkdir(parents=True)
    experiment_state.parent.mkdir(parents=True)
    audit_state.write_text("audit-state", encoding="utf-8")
    experiment_state.write_text("experiment-state", encoding="utf-8")
    local_env = destination / "sql_optimizer" / ".env"
    local_env.write_text("LOCAL_ONLY=placeholder\n", encoding="utf-8")

    _install(destination)

    assert audit_state.read_text(encoding="utf-8") == "audit-state"
    assert experiment_state.read_text(encoding="utf-8") == "experiment-state"
    assert local_env.read_text(encoding="utf-8") == "LOCAL_ONLY=placeholder\n"


def test_late_install_failure_rolls_back_every_bundle(tmp_path, monkeypatch):
    destination = tmp_path / "skills"
    _install(destination)
    before = _snapshot(destination)

    original_commit = install_all._commit_bundle
    calls = 0

    def fail_on_third_commit(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("injected late commit failure")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(install_all, "_commit_bundle", fail_on_third_commit)

    assert install_all.main(["--dest", str(destination)]) == 1
    assert calls == 3
    assert _snapshot(destination) == before


def test_failed_first_install_removes_new_destination(tmp_path, monkeypatch):
    destination = tmp_path / "skills"
    original_commit = install_all._commit_bundle
    calls = 0

    def fail_on_second_commit(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected fresh-install failure")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(install_all, "_commit_bundle", fail_on_second_commit)

    assert install_all.main(["--dest", str(destination)]) == 1
    assert not destination.exists()


def test_staging_refuses_symbolic_link_sources(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("must not be copied", encoding="utf-8")
    (source / "SKILL.md").symlink_to(outside)
    spec = install_all.BundleSpec(
        "example",
        object(),
        source,
        ("SKILL.md",),
        (),
    )

    with pytest.raises(ValueError, match="symbolic link source refused"):
        install_all._stage_bundle(spec, tmp_path / "stage")


def test_staging_refuses_credential_named_sources(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / ".env.production").write_text("not-for-install", encoding="utf-8")
    spec = install_all.BundleSpec(
        "example",
        object(),
        source,
        (".env.production",),
        (),
    )

    with pytest.raises(ValueError, match="credential file declaration refused"):
        install_all._stage_bundle(spec, tmp_path / "stage")


def test_parity_rejects_symlink_inside_declared_tree(tmp_path, capsys):
    destination = tmp_path / "skills"
    _install(destination)
    target = destination / "sql_optimizer" / "sources" / "outside.json"
    target.write_text("{}", encoding="utf-8")
    link = destination / "sql_optimizer" / "sources" / "linked.json"
    link.symlink_to(target)

    assert parity.main(["--dest", str(destination)]) == 1
    assert "installed path is a symbolic link" in capsys.readouterr().err


def test_parity_rejects_symlinked_bundle_directory(tmp_path, capsys):
    destination = tmp_path / "skills"
    _install(destination)
    real_bundle = destination / "sql_optimizer-real"
    (destination / "sql_optimizer").rename(real_bundle)
    (destination / "sql_optimizer").symlink_to(real_bundle, target_is_directory=True)

    assert parity.main(["--dest", str(destination)]) == 1
    assert "installed bundle directory is a symbolic link" in capsys.readouterr().err
