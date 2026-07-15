"""Guide invariants: the optimizer's playbooks must keep describing channels that actually
execute against azure-sql-mcp. The server cannot run index DDL (AdminPolicy denylists
CREATE/DROP/ALTER INDEX and EXEC), so scenario 3 is the emit-script protocol — these
checks pin that fix and the double-gate description so future edits cannot regress them."""

import pathlib

SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]

GUIDES = sorted(SKILL_DIR.glob("*.md")) + [SKILL_DIR / "main.txt"]


def _read(name: str) -> str:
    return (SKILL_DIR / name).read_text(encoding="utf-8")


def _all_guides_text() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in GUIDES if p.exists()}


def test_guides_exist():
    assert len(_all_guides_text()) > 1, f"no guides found in {SKILL_DIR}"


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


def test_no_stale_ungated_claims():
    for name, text in _all_guides_text().items():
        assert "bypasses the validator entirely" not in text, name
        assert "the server does not gate" not in text, name


def test_no_company_references():
    # The skills are generic Azure SQL tooling — no employer/company context
    # (capital-S "Shell"; lowercase command-shell mentions are fine).
    for name, text in _all_guides_text().items():
        assert "Shell" not in text.replace("PowerShell", ""), f"{name} references the company"


def test_index_ddl_never_routed_through_unrestricted_tool():
    # execute_tsql_unrestricted hard-denylists CREATE/DROP INDEX — showing it as the DDL
    # channel is a call that fails every time.
    for name, text in _all_guides_text().items():
        assert "execute_tsql_unrestricted(sql=<single CREATE" not in text, name
        assert "execute_tsql_unrestricted(sql=<single DROP" not in text, name
        assert "execute_tsql_unrestricted(sql=<contents of" not in text, name


def test_run_guide_documents_test_index_channels():
    text = _read("RunGuide.md")
    assert "create_test_index" in text          # dedicated tool is the primary path
    assert "drop_test_index" in text            # rollback tool
    assert "IX_Testing_" in text                # enforced prefix
    assert "Emit-script fallback" in text       # older servers still covered
    assert "sys.indexes" in text or "get_object_details" in text  # verify before measuring
    assert "SandboxGuide.md" in text            # clone-first guidance


def test_sandbox_guide_scopes_the_risk():
    text = _read("SandboxGuide.md")
    assert "AS COPY OF" in text                 # the clone mechanism
    assert "AZURE_SQL_ALLOWED_DATABASES" in text
    assert "only" in text                       # blast wall: apply-on server lists only the clone
    assert "DML" in text                        # DML tuning home
    assert "operator" in text.lower()           # clone creation is never this skill's action


def test_run_guide_documents_operational_traps():
    text = _read("RunGuide.md")
    assert "AZURE_SQL_QUERY_TIMEOUT_SECONDS" in text
    assert "median" in text  # repetition rule: n=1 is not a benchmark


def test_skill_md_describes_double_gate():
    text = _read("SKILL.md")
    assert "AZURE_SQL_WRITE_POLICY" in text
    assert "create_test_index" in text and "drop_test_index" in text
    assert "IX_Testing_" in text  # only prefix-namespaced disposable indexes are tool-executed


def test_queryguide_wires_parameter_buckets():
    text = _read("queryguide.md")
    assert "get_query_parameter_buckets" in text  # production buckets are extracted, not guessed


def test_run_guide_enforces_benchmark_repetition():
    text = _read("RunGuide.md")
    assert "runs=3" in text  # server-side median/spread aggregation is the primary path


def test_skill_md_wires_fleet_intake():
    text = _read("SKILL.md")
    assert "IntakeGuide.md" in text
    assert "sql-plan-enforcer" in text
    assert "sql-health-triage" in text


_RULE_SLUGS_11_16 = {
    "kitchen_sink": "rule11_kitchen_sink",
    "rbar_loop": "rule12_rbar_loop",
    "or_across_columns": "rule13_or_across_columns",
    "correlated_per_row": "rule14_correlated_per_row",
    "mstvf_row_source": "rule15_mstvf_row_source",
    "nested_views": "rule16_nested_views",
}


def _rule_section(text: str, n: int) -> str:
    start = text.index(f"Rule {n}:")
    end_marker = f"Rule {n + 1}:" if f"Rule {n + 1}:" in text else "1.3. Intermediate"
    return text[start:text.index(end_marker, start)]


def test_queryguide_has_query_contract_step():
    text = _read("queryguide.md")
    assert "1.0. Query Contract" in text
    # The contract precedes plan deconstruction and is pre-registered for Phase 4.
    assert text.index("1.0. Query Contract") < text.index("1.1. Execution Plan Deconstruction")
    contract = text[text.index("1.0. Query Contract"):text.index("1.1. Execution Plan")]
    for token in ("Projection contract", "Cardinality contract", "Ordering contract",
                  "NULL semantics", "side effects"):
        assert token in contract, f"contract checklist missing {token!r}"
    # The boundary is load-bearing: contract preserves, plan evidence decides changes —
    # a planner that derives fixes from syntax would reintroduce guesswork.
    assert "PRESERVED" in contract and "CHANGED" in contract
    assert "not a finding until the plan shows it costing" in contract
    # Phase 4 proves against the pre-registered contract.
    assert "section 1.0 query contract" in text


def test_queryguide_covers_rewrite_rules_11_to_16():
    text = _read("queryguide.md")
    for n in range(11, 17):
        assert f"Rule {n}:" in text, f"Rule {n} missing from queryguide.md"
    # Rules 11-16 live inside section 1.2, before intermediate-structure analysis.
    assert text.index("Rule 16:") < text.index("1.3. Intermediate")
    rule11 = _rule_section(text, 11)
    assert "OPTION (RECOMPILE)" in rule11       # low-frequency fix
    assert "sp_executesql" in rule11             # hot-path fix stays parameterized
    rule13 = _rule_section(text, 13)
    assert "UNION ALL" in rule13
    assert "IS NULL" in rule13                   # NULL arm of the exclusion predicate
    assert "same column" in rule13               # IN-shaped OR is out of scope
    rule14 = _rule_section(text, 14)
    assert "ROW_NUMBER" in rule14
    assert "tie" in rule14.lower()               # ties caveat is load-bearing
    rule15 = _rule_section(text, 15)
    assert "inline" in rule15                    # iTVF conversion is the primary fix
    assert "nterleaved" in rule15                # engine mitigation has gaps
    assert "100" in rule15                       # the fixed row guess at level 140+
    rule16 = _rule_section(text, 16)
    assert "1.1" in rule16                       # diagnosis via Base Object Resolution
    assert "base tables" in rule16


def test_queryguide_rule12_separates_rbar_from_batching():
    # The 1.4 batching loop is deliberate chunked set work; Rule 12 must never
    # instruct collapsing it, and non-mechanical loops are redesigns, not rewrites.
    rule12 = _rule_section(_read("queryguide.md"), 12)
    assert "1.4" in rule12
    assert "DELETE TOP (4000)" in rule12
    assert "order-dependent" in rule12
    assert "redesign" in rule12


def test_examples_cover_rules_11_to_16():
    text = _read("Examples.md")
    assert "Rewrite-pattern examples" in text
    for n in range(11, 17):
        assert f"Rule {n}" in text, f"Rule {n} example missing from Examples.md"
    assert "OPTION (RECOMPILE)" in text
    assert "UNION ALL" in text
    assert "ROW_NUMBER() OVER" in text


def test_field_example_promotion_protocol():
    improve = _read("ImproveGuide.md")
    assert "Promoting field examples" in improve
    assert "SQL_OPTIMIZER_AUDIT_FULL_SQL" in improve       # prerequisite named
    assert "Anonymization — mandatory" in improve          # shipped content stays clean
    assert "at most 5" in improve                          # context cap
    assert "equivalence_failed" in improve                 # negatives outrank wins

    examples = _read("Examples.md")
    assert "## Field examples" in examples
    assert "ImproveGuide.md" in examples                   # promotion path, not hand-pasting


def test_audit_guide_slugs_cover_all_rules():
    text = _read("AuditGuide.md")
    assert "Rules 1–16" in text                  # en dash, matching existing style
    for anti_slug, rule_slug in _RULE_SLUGS_11_16.items():
        assert anti_slug in text, f"anti_pattern slug {anti_slug!r} missing"
        assert rule_slug in text, f"rules_applied slug {rule_slug!r} missing"
