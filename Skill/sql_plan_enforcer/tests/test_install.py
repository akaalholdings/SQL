"""Every declared file must exist in source, install must copy them all, and the
ledger corpus directory must survive a re-install (prune top-level files only)."""

import pathlib

import install


def test_all_install_files_exist_in_source():
    source_dir = pathlib.Path(install.__file__).resolve().parent
    missing = [name for name in install.SKILL_FILES if not (source_dir / name).exists()]
    assert missing == [], f"declared but missing from source: {missing}"


def test_safety_critical_files_are_installed():
    for name in ("SafetyGuide.md", "authorization.py", "enforcement_ledger.py"):
        assert name in install.SKILL_FILES


def test_main_copies_everything_to_dest(tmp_path, monkeypatch):
    monkeypatch.setattr(install.pathlib.Path, "home", lambda: tmp_path)
    assert install.main() == 0

    dest = tmp_path / ".copilot" / "skills" / "sql_plan_enforcer"
    for name in install.SKILL_FILES:
        assert (dest / name).exists(), f"{name} was not installed"


def test_main_prunes_stale_files_but_preserves_ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(install.pathlib.Path, "home", lambda: tmp_path)
    dest = tmp_path / ".copilot" / "skills" / "sql_plan_enforcer"
    dest.mkdir(parents=True)

    (dest / "OldGuide.md").write_text("stale", encoding="utf-8")
    (dest / "audits").mkdir()
    (dest / "audits" / "index.jsonl").write_text('{"keep": true}\n', encoding="utf-8")

    assert install.main() == 0

    assert not (dest / "OldGuide.md").exists(), "stale top-level file should be pruned"
    assert (dest / "audits").is_dir(), "ledger corpus must be preserved"
    assert (dest / "audits" / "index.jsonl").read_text(encoding="utf-8") == '{"keep": true}\n'
    assert (dest / "SKILL.md").exists()
