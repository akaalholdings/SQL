from __future__ import annotations

import pathlib

import install


def test_runtime_payload_is_only_skill_markdown() -> None:
    assert install.SKILL_FILES == ("SKILL.md",)
    assert install.SKILL_DIRS == ()


def test_install_is_exact_and_replaces_prior_bundle(tmp_path: pathlib.Path) -> None:
    root = tmp_path / "skills"
    destination = root / install.SKILL_NAME
    destination.mkdir(parents=True)
    (destination / "old-helper.py").write_text("old", encoding="utf-8")

    installed = install.install(root)

    assert installed == destination
    assert {entry.name for entry in installed.iterdir()} == {"SKILL.md"}
