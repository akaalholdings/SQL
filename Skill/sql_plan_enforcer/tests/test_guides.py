"""Guide invariants: the markdown playbooks must keep describing channels that actually
execute against azure-sql-mcp. These string checks pin the dead-leg fixes (hints have no
EXEC channel), the double-gate description, and the retirement of the legacy CLI so a
future edit cannot quietly regress them."""

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


def test_no_legacy_cli_references():
    for name, text in _all_guides_text().items():
        assert "query_geneva_db" not in text, f"{name} references the retired CLI"
        assert "tsv_to_json" not in text, f"{name} references the removed TSV bridge"


def test_no_stale_ungated_claims():
    for name, text in _all_guides_text().items():
        assert "bypasses the validator entirely" not in text, name
        assert "the server does not gate" not in text, name


def test_no_company_references():
    # The skills are generic Azure SQL tooling — no employer/company context
    # (capital-S "Shell"; lowercase command-shell mentions are fine).
    for name, text in _all_guides_text().items():
        assert "Shell" not in text.replace("PowerShell", ""), f"{name} references the company"


def test_hints_have_no_raw_sql_execution_path():
    # The EXEC statements for hints must never be shown as an execute_tsql_unrestricted
    # argument — that call is hard-denylisted server-side and fails every time. Hints
    # execute through the dedicated set/clear tools.
    for name, text in _all_guides_text().items():
        assert "execute_tsql_unrestricted(sql=\"EXEC" not in text, name
        assert "execute_tsql_unrestricted(sql=<single EXEC" not in text, name


def test_enforce_guide_documents_hint_channels():
    text = _read("EnforceGuide.md")
    assert "set_query_store_hints" in text          # dedicated tool is the primary path
    assert "clear_query_store_hints" in text        # rollback tool
    assert "Emit-script fallback" in text           # older servers still covered
    assert "sys.query_store_query_hints" in text    # the read-only confirmation check
    assert '"emitted"' in text


def test_run_guide_documents_channels():
    text = _read("RunGuide.md")
    assert "force_query_plan" in text
    assert "set_query_store_hints" in text
    assert "AZURE_SQL_WRITE_POLICY" in text


def test_tick_is_preview_only():
    # Every shown plan_enforcer_tick call must be dry_run=true, and no prose may
    # suggest flipping it — applies belong to force_query_plan under this skill's ledger.
    for name, text in _all_guides_text().items():
        for chunk in text.split("plan_enforcer_tick(")[1:]:
            args = chunk.split(")")[0]
            assert "dry_run=false" not in args, (
                f"{name} shows plan_enforcer_tick with dry_run=false"
            )
        if "plan_enforcer_tick" in text:
            assert "preview" in text.lower(), (
                f"{name} mentions the tick without the preview-only rule"
            )


def test_safety_guide_describes_both_gates():
    text = _read("SafetyGuide.md")
    assert "Two independent gates" in text
    assert "AZURE_SQL_WRITE_POLICY" in text


def test_audit_guide_lists_emitted_outcome():
    text = _read("AuditGuide.md")
    assert "`emitted`" in text
    assert "two-row hint lifecycle" in text.lower()


def test_skill_md_names_both_companions():
    text = _read("SKILL.md")
    assert "sql-optimizer" in text
    assert "sql-health-triage" in text
    assert "handoff_queue.py" in text


def test_loop_guide_documents_scheduled_ticks():
    text = _read("LoopGuide.md")
    assert "Scheduled ticks" in text
    assert "kill switch is the remote stop" in text.lower()
    assert "one tick per scheduled run" in text.lower()
    # unattended runs must not strand emit-script work
    assert "attended-only" in text


def test_scan_guide_has_primary_path_and_fallback_scans():
    text = _read("ScanGuide.md")
    assert "Primary path" in text
    for tool in ("detect_regressed_queries", "get_top_queries",
                 "detect_parameter_sniffing", "get_forced_plans"):
        assert tool in text, f"primary path missing {tool}"
    assert "scan_adapter.py" in text
    # the four fallback SQL blocks stay intact
    for marker in ("scan_regression.sql", "scan_top.sql",
                   "scan_param_sensitive.sql", "scan_stale_forced.sql"):
        assert marker in text, f"fallback scan {marker} was dropped"
    # empty detect_regressed_queries is not "healthy"
    assert "empty" in text.lower() and "automatic-tuning" in text
