"""Every installed file must exist in source, and install must copy them all."""

import importlib.util
import pathlib

# Load this skill's install.py by path: both skills ship an install.py, and a plain
# `import install` resolves to whichever skill's module got imported first when the
# suites run in one pytest process.
_INSTALL_PATH = pathlib.Path(__file__).resolve().parents[1] / "install.py"
_spec = importlib.util.spec_from_file_location("sql_optimizer_install", _INSTALL_PATH)
install = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(install)


def test_all_install_files_exist_in_source():
    source_dir = pathlib.Path(install.__file__).resolve().parent
    missing = [name for name in install.SKILL_FILES if not (source_dir / name).exists()]
    missing.extend(
        name for name in install.SKILL_DIRS if not (source_dir / name).is_dir()
    )
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


def test_indexing_guide_is_installed():
    assert "IndexingGuide.md" in install.SKILL_FILES
    assert "sources" in install.SKILL_DIRS


def test_intake_guide_is_installed():
    assert "IntakeGuide.md" in install.SKILL_FILES


def test_resolve_dest_order(tmp_path, monkeypatch):
    monkeypatch.setattr(install.pathlib.Path, "home", lambda: tmp_path)
    monkeypatch.setenv("SQL_SKILLS_DEST", str(tmp_path / "env_dest"))
    # explicit --dest wins over env
    assert install.resolve_dest(str(tmp_path / "explicit")) == tmp_path / "explicit"
    # env wins over auto-detect
    (tmp_path / ".claude").mkdir()
    (tmp_path / ".copilot").mkdir()
    assert install.resolve_dest(None) == tmp_path / "env_dest"
    # auto-detect prefers ~/.claude over ~/.copilot; default is ~/.claude
    monkeypatch.delenv("SQL_SKILLS_DEST")
    assert install.resolve_dest(None) == tmp_path / ".claude" / "skills"


def test_main_copies_everything_to_dest(tmp_path, monkeypatch):
    monkeypatch.delenv("SQL_SKILLS_DEST", raising=False)
    dest_root = tmp_path / "skills"
    assert install.main(["--dest", str(dest_root)]) == 0

    dest = dest_root / "sql_optimizer"
    for name in install.SKILL_FILES:
        assert (dest / name).exists(), f"{name} was not installed"
    for name in install.SKILL_DIRS:
        assert (dest / name).is_dir(), f"{name} was not installed"
    assert (dest / "sources" / "brentozar-indexing" / "manifest.json").exists()
    assert (dest / "sources" / "kendra-indexing" / "manifest.json").exists()


def test_main_prunes_stale_files_but_preserves_dirs(tmp_path, monkeypatch):
    monkeypatch.delenv("SQL_SKILLS_DEST", raising=False)
    dest_root = tmp_path / "skills"
    dest = dest_root / "sql_optimizer"
    dest.mkdir(parents=True)

    # A stale guide from an older install, plus the audit data dir that must survive.
    (dest / "PlanGuide.md").write_text("stale", encoding="utf-8")
    (dest / ".env").write_text("LOCAL_ONLY=placeholder\n", encoding="utf-8")
    (dest / "audits").mkdir()
    (dest / "audits" / "index.jsonl").write_text('{"keep": true}\n', encoding="utf-8")

    assert install.main(["--dest", str(dest_root)]) == 0

    assert not (dest / "PlanGuide.md").exists(), "stale top-level file should be pruned"
    assert (dest / ".env").exists(), "credential files must never be pruned"
    assert (dest / "audits").is_dir(), "audit data dir must be preserved"
    assert (dest / "audits" / "index.jsonl").read_text(encoding="utf-8") == '{"keep": true}\n'
    assert (dest / "SKILL.md").exists()


def test_main_refuses_symlinked_destination_file(tmp_path, monkeypatch):
    skills_root = tmp_path / "skills"
    destination = skills_root / install.SKILL_NAME
    destination.mkdir(parents=True)
    outside = tmp_path / "outside.md"
    outside.write_text("unchanged", encoding="utf-8")
    (destination / "SKILL.md").symlink_to(outside)
    monkeypatch.setattr(install, "resolve_dest", lambda _explicit=None: skills_root)

    assert install.main([]) == 1
    assert outside.read_text(encoding="utf-8") == "unchanged"
