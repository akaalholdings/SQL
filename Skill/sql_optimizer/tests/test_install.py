"""Every installed file must exist in source, and install must copy them all."""

import pathlib

import install


def test_all_install_files_exist_in_source():
    source_dir = pathlib.Path(install.__file__).resolve().parent
    missing = [name for name in install.SKILL_FILES if not (source_dir / name).exists()]
    assert missing == [], f"declared but missing from source: {missing}"


def test_new_audit_files_are_installed():
    for name in (
        "AuditGuide.md",
        "ImproveGuide.md",
        "record_audit.py",
        "validate_audit.py",
        "summarize_audits.py",
    ):
        assert name in install.SKILL_FILES


def test_main_copies_everything_to_dest(tmp_path, monkeypatch):
    # Redirect ~ so install writes under the temp dir instead of the real home.
    monkeypatch.setattr(install.pathlib.Path, "home", lambda: tmp_path)

    assert install.main() == 0

    dest = tmp_path / ".copilot" / "skills" / "sql_optimizer"
    for name in install.SKILL_FILES:
        assert (dest / name).exists(), f"{name} was not installed"


def test_main_prunes_stale_files_but_preserves_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(install.pathlib.Path, "home", lambda: tmp_path)
    dest = tmp_path / ".copilot" / "skills" / "sql_optimizer"
    dest.mkdir(parents=True)

    # A stale guide from an older install, plus the audit data dir that must survive.
    (dest / "PlanGuide.md").write_text("stale", encoding="utf-8")
    (dest / "audits").mkdir()
    (dest / "audits" / "index.jsonl").write_text('{"keep": true}\n', encoding="utf-8")

    assert install.main() == 0

    assert not (dest / "PlanGuide.md").exists(), "stale top-level file should be pruned"
    assert (dest / "audits").is_dir(), "audit data dir must be preserved"
    assert (dest / "audits" / "index.jsonl").read_text(encoding="utf-8") == '{"keep": true}\n'
    assert (dest / "SKILL.md").exists()
