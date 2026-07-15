from __future__ import annotations

import pathlib

import install


def test_runtime_payload_is_only_skill_markdown() -> None:
    assert install.SKILL_FILES == ("SKILL.md",)
    assert install.SKILL_DIRS == ()


def test_install_contains_no_skill_side_state_machine(tmp_path: pathlib.Path) -> None:
    destination = install.install(tmp_path / "skills")

    assert {entry.name for entry in destination.iterdir()} == {"SKILL.md"}
    assert not any(destination.rglob("*.py"))
