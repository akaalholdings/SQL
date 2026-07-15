from __future__ import annotations

import pathlib

import install


def test_runtime_payload_is_only_skill_markdown() -> None:
    assert install.SKILL_FILES == ("SKILL.md",)
    assert install.SKILL_DIRS == ()


def test_install_is_exact_and_removes_stale_runtime(tmp_path: pathlib.Path) -> None:
    root = tmp_path / "skills"
    destination = install.install(root)
    assert {entry.name for entry in destination.iterdir()} == {"SKILL.md"}

    (destination / "stale.txt").write_text("old", encoding="utf-8")
    (destination / "state").mkdir()
    (destination / "state" / "old.json").write_text("{}", encoding="utf-8")

    destination = install.install(root)
    assert {entry.name for entry in destination.iterdir()} == {"SKILL.md"}
    assert (destination / "SKILL.md").read_bytes() == (
        pathlib.Path(install.__file__).resolve().parent / "SKILL.md"
    ).read_bytes()
