"""Every declared file must exist in source, install must copy them all, and the
ledger corpus directory must survive a re-install (prune top-level files only)."""

import importlib.util
import pathlib

# Load this skill's install.py by path: both skills ship an install.py, and a plain
# `import install` resolves to whichever skill's module got imported first when the
# suites run in one pytest process.
_INSTALL_PATH = pathlib.Path(__file__).resolve().parents[1] / "install.py"
_spec = importlib.util.spec_from_file_location("sql_plan_enforcer_install", _INSTALL_PATH)
install = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(install)


def test_all_install_files_exist_in_source():
    source_dir = pathlib.Path(install.__file__).resolve().parent
    missing = [name for name in install.SKILL_FILES if not (source_dir / name).exists()]
    assert missing == [], f"declared but missing from source: {missing}"


def test_safety_critical_files_are_installed():
    for name in ("SafetyGuide.md", "authorization.py", "enforcement_ledger.py"):
        assert name in install.SKILL_FILES


def test_handoff_and_adapter_modules_are_installed():
    for name in ("handoff_queue.py", "scan_adapter.py"):
        assert name in install.SKILL_FILES


def test_resolve_dest_order(tmp_path, monkeypatch):
    monkeypatch.setattr(install.pathlib.Path, "home", lambda: tmp_path)
    monkeypatch.setenv("SQL_SKILLS_DEST", str(tmp_path / "env_dest"))
    assert install.resolve_dest(str(tmp_path / "explicit")) == tmp_path / "explicit"
    (tmp_path / ".claude").mkdir()
    (tmp_path / ".copilot").mkdir()
    assert install.resolve_dest(None) == tmp_path / "env_dest"
    monkeypatch.delenv("SQL_SKILLS_DEST")
    assert install.resolve_dest(None) == tmp_path / ".claude" / "skills"


def test_main_copies_everything_to_dest(tmp_path, monkeypatch):
    monkeypatch.delenv("SQL_SKILLS_DEST", raising=False)
    dest_root = tmp_path / "skills"
    assert install.main(["--dest", str(dest_root)]) == 0

    dest = dest_root / "sql_plan_enforcer"
    for name in install.SKILL_FILES:
        assert (dest / name).exists(), f"{name} was not installed"


def test_main_prunes_stale_files_but_preserves_ledger(tmp_path, monkeypatch):
    monkeypatch.delenv("SQL_SKILLS_DEST", raising=False)
    dest_root = tmp_path / "skills"
    dest = dest_root / "sql_plan_enforcer"
    dest.mkdir(parents=True)

    (dest / "OldGuide.md").write_text("stale", encoding="utf-8")
    (dest / ".env").write_text("LOCAL_ONLY=placeholder\n", encoding="utf-8")
    (dest / "audits").mkdir()
    (dest / "audits" / "index.jsonl").write_text('{"keep": true}\n', encoding="utf-8")

    assert install.main(["--dest", str(dest_root)]) == 0

    assert not (dest / "OldGuide.md").exists(), "stale top-level file should be pruned"
    assert (dest / ".env").exists(), "credential files must never be pruned"
    assert (dest / "audits").is_dir(), "ledger corpus must be preserved"
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
