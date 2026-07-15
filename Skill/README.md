# Azure SQL DBA Performance Toolset

Three agent skills that together cover the DBA performance loop for **Azure SQL Database**, executing exclusively through the [`azure-sql-mcp`](https://github.com/AkaalHoldings/azure-sql-mcp) server:

| Skill | Role | Mutates the database? |
|---|---|---|
| [`sql_health_triage`](sql_health_triage/SKILL.md) | **Detect** — incident triage and health sweeps: waits, blocking, deadlocks, tempdb, memory grants, resource limits, plan cache | Never (strictly read-only) |
| [`sql_optimizer`](sql_optimizer/SKILL.md) | **Fix one query** — actual-plan analysis, semantically identical rewrite, index recommendations, three-scenario benchmark | Only disposable `IX_Testing_`-prefixed test indexes via gated tools (ideally on a sandbox clone — `SandboxGuide.md`); production DDL is always a script |
| [`sql_plan_enforcer`](sql_plan_enforcer/SKILL.md) | **Stabilize the fleet** — Query Store scans, plan forcing/unforcing and hints with verify-or-revert | Only reversible Query Store plan controls, double-gated and ledgered |

## How they interlock

```
                     symptom / checkup
                            │
                   ┌────────▼────────┐
                   │ sql_health_triage│  read-only diagnosis; every finding gets an owner
                   └───┬─────────┬───┘
        query-shaped   │         │   plan instability
        culprit (pack) │         │   (pointer to review mode)
                   ┌───▼───┐ ┌───▼──────────────┐
        ┌─────────►│handoff│ │ sql_plan_enforcer │  scan → rank → force/hint → verify-or-revert
        │          │ queue │◄┤                  │  rewrite/index needs become packs ──┐
        │          └───┬───┘ └──────▲───────────┘                                     │
        │              │            │ shipped packs re-verified (redeploy_verify)     │
        │          ┌───▼────────┐   │                                                 │
        │          │sql_optimizer│──┘  claims packs, optimizes, records resolution    │
        │          └────────────┘                                                     │
        └─────────────────────────────────────────────────────────────────────────────┘
```

- The **handoff queue** ([`sql_plan_enforcer/handoff_queue.py`](sql_plan_enforcer/handoff_queue.py)) is the durable contract: triage and the enforcer enqueue evidence packs; the optimizer claims, optimizes, and resolves them; shipped rewrites flow back into the enforcer's re-verification.
- Each skill audits itself: optimizer audit corpus (best-effort), enforcer ledger (fail-closed — the record *is* the undo), triage log (best-effort).

## Install

```bash
python3 install_all.py --dest ~/.copilot/skills
python3 check_installed_parity.py        # verify source == installed
```

Each individual installer also supports automatic destination resolution: `--dest <path>` → `$SQL_SKILLS_DEST` → the host containing an existing SQL bundle → an existing `.claude/skills` or `.copilot/skills` directory → `~/.claude/skills`.

## Server prerequisites (`azure-sql-mcp`)

| Setting | Needed for |
|---|---|
| `AZURE_SQL_ALLOWED_DATABASES` | every skill — the only databases any tool can touch |
| `AZURE_SQL_ACCESS_MODE=restricted` (default) | triage and all read-only work; optimizer baseline/rewrite scenarios |
| `AZURE_SQL_ACCESS_MODE=unrestricted` + `AZURE_SQL_WRITE_POLICY=apply` | explicitly approved optimizer maintenance and enforcer plan controls — each call also needs explicit `dry_run=false` and is written to the server's JSONL audit |
| `AZURE_SQL_QUERY_TIMEOUT_SECONDS` (default 30) | raise for optimizer baselines slower than 30s |
| `AZURE_SQL_ROW_LIMIT` (default 200) | scan/diagnostic truncation threshold (`truncated: true` flags it) |
| `AZURE_SQL_TEST_INDEX_DATABASES` | optimizer test indexes | explicit sandbox/test database allowlist required for live create/drop |

The skills add their own gates on top of the server's (the enforcer's kill switch / apply mode / allowlist) — both layers must be open for anything to execute. See `sql_plan_enforcer/SafetyGuide.md` ("Two independent gates").

## Environment variables (skill-side)

| Variable | Skill | Purpose |
|---|---|---|
| `SQL_SKILLS_DEST` | all | install destination override |
| `SQL_OPTIMIZER_AUDIT` / `SQL_OPTIMIZER_AUDIT_DIR` / `SQL_OPTIMIZER_AUDIT_FULL_SQL` | optimizer | audit corpus: opt-out, location, raw-SQL opt-in |
| `SQL_OPTIMIZER_EXPERIMENT_DIR` | optimizer | durable disposable test-index experiment records |
| `SQL_PLAN_ENFORCER_DISABLE` | enforcer | kill switch — halts all applies instantly |
| `SQL_PLAN_ENFORCER_APPLY` | enforcer | apply mode (default off = dry-run) |
| `SQL_PLAN_ENFORCER_ALLOWLIST` | enforcer | allowlist file path |
| `SQL_PLAN_ENFORCER_AUDIT_DIR` / `SQL_PLAN_ENFORCER_STATE` / `SQL_PLAN_ENFORCER_HANDOFF_DIR` | enforcer | ledger / coverage state / handoff queue locations |
| `SQL_PLAN_ENFORCER_MAX_ENFORCE_PER_TICK` / `SQL_PLAN_ENFORCER_VERIFY_WAIT_MINUTES` / `SQL_PLAN_ENFORCER_REEVALUATE_TTL_DAYS` | enforcer | loop pacing |
| `SQL_PLAN_ENFORCER_MIN_EXECUTIONS` / `SQL_PLAN_ENFORCER_MIN_REGRESSION_PCT` / `SQL_PLAN_ENFORCER_MIN_CV` / `SQL_PLAN_ENFORCER_MIN_AVG_DURATION` | enforcer | eligibility floors |
| `SQL_PLAN_ENFORCER_MIN_IMPROVEMENT_PCT` / `SQL_PLAN_ENFORCER_REGRESS_TOLERANCE_PCT` | enforcer | keep/rollback floors |
| `SQL_HEALTH_TRIAGE_AUDIT` / `SQL_HEALTH_TRIAGE_AUDIT_DIR` | triage | triage log: opt-out, location |

State/audit directories resolve host-agnostically: env override → legacy `~/.copilot/skills/<skill>/...` when it already exists → `~/.sql-skills/<skill>/...`.

## Server upgrade notes

Both formerly-pending server tools have shipped; the emit-script protocols remain in the guides only as fallbacks for older server deployments:

- **`set_query_store_hints` / `clear_query_store_hints`** — the enforcer's hints lever is one gated server call bracketed by `prepared` and confirmed ledger rows (`sql_plan_enforcer/EnforceGuide.md` §5; the ledger's `emitted` outcome exists for the fallback).
- **`create_test_index` / `drop_test_index`** — the optimizer's scenario-3 test index is a gated create/capture/drop sequence (`sql_optimizer/RunGuide.md` step 4), prefix-enforced server-side with the rollback DROP attached. Pair with a database-clone sandbox (`sql_optimizer/SandboxGuide.md`) for zero-production-risk index and DML testing.

## Development

```bash
uv run --with pytest pytest sql_optimizer/tests sql_plan_enforcer/tests sql_health_triage/tests
```

Pure-function pytest suites (no live database needed); `pytest.ini` sets importlib mode so the three same-named test modules coexist in one run. Guide invariants are tested too (`test_guides.py` in each skill) — dead-leg regressions and stale gating claims fail the suite.

If an older install left a `query_geneva_db` bundle at `~/.copilot/skills/query_geneva_db/`, remove it manually — that skill is retired; azure-sql-mcp is the single execution channel.
