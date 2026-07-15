"""Guide invariants for the triage skill: strictly Azure SQL Database, strictly
read-only. These string checks pin both so a future edit cannot quietly regress them."""

import pathlib

SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]

GUIDES = sorted(SKILL_DIR.glob("*.md"))


def _read(name: str) -> str:
    return (SKILL_DIR / name).read_text(encoding="utf-8")


def _all_guides_text() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in GUIDES}


def test_guides_exist():
    assert GUIDES, f"no guides found in {SKILL_DIR}"


# The toolset is strictly Azure SQL Database. Foreign engines/services may appear only
# on the Platform Lock's own exclusion line ("Do not reference ...").
_FOREIGN_ENGINES = ("Synapse", "PostgreSQL", "MySQL", "Managed Instance", "Fabric",
                    "Databricks", "Oracle", "MariaDB", "SQLite", "SQL Server", "on-prem")


def test_platform_lock_is_strict():
    skill = _read("SKILL.md")
    assert "Platform Lock" in skill
    assert "Azure SQL Database" in skill
    for name, text in _all_guides_text().items():
        kept = [line for line in text.splitlines()
                if "Do not reference" not in line and "on-prem intuition" not in line]
        remainder = "\n".join(kept)
        for engine in _FOREIGN_ENGINES:
            assert engine not in remainder, f"{name} references foreign engine {engine!r}"


def test_no_company_references():
    # The skills are generic Azure SQL tooling — no employer/company context
    # (capital-S "Shell"; lowercase command-shell mentions are fine).
    for name, text in _all_guides_text().items():
        assert "Shell" not in text.replace("PowerShell", ""), f"{name} references the company"


def test_skill_is_strictly_read_only():
    skill = _read("SKILL.md")
    assert "read-only" in skill.lower()
    # write/admin tools may appear only in the NEVER-call list, and kill_session
    # only as recommend-only — no guide may show them as calls to make.
    for name, text in _all_guides_text().items():
        for tool in ("execute_tsql_unrestricted(", "force_query_plan(",
                     "apply_plan_action(", "rebuild_index(", "update_statistics(",
                     "kill_session("):
            assert tool not in text, f"{name} shows a write/admin tool call: {tool}"


def test_findings_route_to_owners():
    for name in ("SKILL.md", "TriageGuide.md", "ReportGuide.md"):
        text = _read(name)
        assert "sql-optimizer" in text, f"{name} missing the optimizer route"
        assert "sql-plan-enforcer" in text, f"{name} missing the enforcer route"
