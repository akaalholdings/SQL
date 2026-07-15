from __future__ import annotations

import importlib.util
import pathlib
import stat

import pytest


SKILLS_ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO_ROOT = SKILLS_ROOT.parent
ACTIVE_BUNDLES = ("sql_optimizer", "sql_plan_enforcer", "sql_health_triage")
RETIRED_BUNDLE = "_".join(("query", "geneva", "db"))
RETIRED_COMPONENT = "".join(("con", "nector"))


def _load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


install_all = _load("skills_install_all", SKILLS_ROOT / "install_all.py")
parity = _load("skills_parity", SKILLS_ROOT / "check_installed_parity.py")


def _install(tmp_path: pathlib.Path, destination: pathlib.Path):
    wrapper = tmp_path / "bin" / RETIRED_BUNDLE
    return install_all.install_all(
        destination,
        backup_root=tmp_path / "archive",
        retired_wrapper=wrapper,
        discovery_roots=(destination,),
    )


def test_release_tools_target_exactly_the_maintained_collection() -> None:
    assert install_all.ACTIVE_BUNDLES == ACTIVE_BUNDLES
    assert parity.ACTIVE_BUNDLES == ACTIVE_BUNDLES
    for bundle in ACTIVE_BUNDLES:
        module = _load(f"{bundle}_installer", SKILLS_ROOT / bundle / "install.py")
        assert module.SKILL_FILES == ("SKILL.md",)
        assert module.SKILL_DIRS == ()


def test_clean_install_contains_only_authoritative_skill_files(
    tmp_path: pathlib.Path,
) -> None:
    destination = tmp_path / "skills"
    installed_root, archive = _install(tmp_path, destination)

    assert installed_root == destination
    assert archive is None
    assert {
        entry.name for entry in destination.iterdir() if not entry.name.startswith(".")
    } == set(ACTIVE_BUNDLES)
    for bundle in ACTIVE_BUNDLES:
        assert {entry.name for entry in (destination / bundle).iterdir()} == {"SKILL.md"}
        assert (destination / bundle / "SKILL.md").read_bytes() == (
            SKILLS_ROOT / bundle / "SKILL.md"
        ).read_bytes()


def test_install_replaces_stale_active_payloads(tmp_path: pathlib.Path) -> None:
    destination = tmp_path / "skills"
    for bundle in ACTIVE_BUNDLES:
        old = destination / bundle
        old.mkdir(parents=True)
        (old / "old-helper.py").write_text("old", encoding="utf-8")
        (old / "state").mkdir()

    _root, archive = _install(tmp_path, destination)

    for bundle in ACTIVE_BUNDLES:
        assert {entry.name for entry in (destination / bundle).iterdir()} == {"SKILL.md"}
    assert archive is not None
    assert stat.S_IMODE((tmp_path / "archive").stat().st_mode) == 0o700
    archived_markers = {
        marker.read_text(encoding="utf-8")
        for marker in archive.glob("prior-*/old-helper.py")
    }
    assert archived_markers == {"old"}


def test_install_archives_retired_skills_and_path_wrapper(
    tmp_path: pathlib.Path,
) -> None:
    destination = tmp_path / "skills"
    top_level = destination / RETIRED_BUNDLE
    nested = destination / "legacy" / RETIRED_BUNDLE
    wrapper = tmp_path / "bin" / RETIRED_BUNDLE
    for path in (top_level, nested):
        path.mkdir(parents=True)
        (path / "old.txt").write_text("retired", encoding="utf-8")
    wrapper.parent.mkdir(parents=True)
    wrapper.write_text("#!/bin/sh\n", encoding="utf-8")

    _root, archive = install_all.install_all(
        destination,
        backup_root=tmp_path / "archive",
        retired_wrapper=wrapper,
        discovery_roots=(destination,),
    )

    assert archive is not None
    assert not top_level.exists()
    assert not nested.exists()
    assert not wrapper.exists()
    assert len(tuple(archive.iterdir())) == 3
    assert stat.S_IMODE(archive.stat().st_mode) == 0o700


def test_late_install_failure_restores_all_prior_bundles(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "skills"
    for bundle in ACTIVE_BUNDLES:
        old = destination / bundle
        old.mkdir(parents=True)
        (old / "marker.txt").write_text(bundle, encoding="utf-8")

    real_replace = install_all.os.replace
    failed = False

    def flaky_replace(source, target):
        nonlocal failed
        source_path = pathlib.Path(source)
        if (
            not failed
            and ".sql-skills-stage-" in source_path.parent.name
            and source_path.name == "sql_plan_enforcer"
        ):
            failed = True
            raise OSError("synthetic late commit failure")
        return real_replace(source, target)

    monkeypatch.setattr(install_all.os, "replace", flaky_replace)

    with pytest.raises(OSError, match="synthetic"):
        _install(tmp_path, destination)

    for bundle in ACTIVE_BUNDLES:
        assert (destination / bundle / "marker.txt").read_text(encoding="utf-8") == bundle


def test_parity_rejects_changed_extra_and_retired_surfaces(
    tmp_path: pathlib.Path,
) -> None:
    destination = tmp_path / "skills"
    wrapper = tmp_path / "bin" / RETIRED_BUNDLE
    _install(tmp_path, destination)

    assert parity.compare_install(
        destination,
        retired_wrapper=wrapper,
        discovery_roots=(destination,),
    ) == []

    (destination / "sql_optimizer" / "SKILL.md").write_text("changed", encoding="utf-8")
    problems = parity.compare_install(
        destination,
        retired_wrapper=wrapper,
        discovery_roots=(destination,),
    )
    assert any("differs" in problem for problem in problems)

    (destination / "sql_optimizer" / "SKILL.md").write_bytes(
        (SKILLS_ROOT / "sql_optimizer" / "SKILL.md").read_bytes()
    )
    (destination / "sql_optimizer" / "extra.md").write_text("stale", encoding="utf-8")
    problems = parity.compare_install(
        destination,
        retired_wrapper=wrapper,
        discovery_roots=(destination,),
    )
    assert any("only SKILL.md" in problem for problem in problems)

    (destination / "sql_optimizer" / "extra.md").unlink()
    (destination / RETIRED_BUNDLE).mkdir()
    wrapper.parent.mkdir(parents=True)
    wrapper.write_text("retired", encoding="utf-8")
    problems = parity.compare_install(
        destination,
        retired_wrapper=wrapper,
        discovery_roots=(destination,),
    )
    assert any("retired skill" in problem for problem in problems)
    assert any("PATH wrapper" in problem for problem in problems)


def test_install_and_parity_cover_secondary_discovery_roots(
    tmp_path: pathlib.Path,
) -> None:
    destination = tmp_path / "copilot" / "skills"
    secondary = tmp_path / "agents" / "skills"
    retired = secondary / "nested" / RETIRED_BUNDLE
    wrapper = tmp_path / "bin" / RETIRED_BUNDLE
    retired.mkdir(parents=True)
    (retired / "old.txt").write_text("retired", encoding="utf-8")

    _root, archive = install_all.install_all(
        destination,
        backup_root=tmp_path / "archive",
        retired_wrapper=wrapper,
        discovery_roots=(destination, secondary),
    )

    assert archive is not None
    assert not retired.exists()
    assert parity.compare_install(
        destination,
        retired_wrapper=wrapper,
        discovery_roots=(destination, secondary),
    ) == []

    stale = secondary / RETIRED_BUNDLE
    stale.mkdir(parents=True)
    problems = parity.compare_install(
        destination,
        retired_wrapper=wrapper,
        discovery_roots=(destination, secondary),
    )
    assert any(str(stale) in problem for problem in problems)


def test_retired_repository_trees_have_no_payload() -> None:
    ignored_names = {"__pycache__", ".DS_Store"}

    def payloads(path: pathlib.Path) -> list[pathlib.Path]:
        if not path.exists():
            return []
        return [
            item
            for item in path.rglob("*")
            if item.name not in ignored_names
            and "__pycache__" not in item.parts
            and item.suffix != ".pyc"
        ]

    assert payloads(REPO_ROOT / "legacy" / RETIRED_BUNDLE) == []
    assert payloads(REPO_ROOT / RETIRED_COMPONENT) == []
