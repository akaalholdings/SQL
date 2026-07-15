# Azure SQL MCP skill collection

This directory is the maintained Azure SQL Database skill collection. It contains
exactly three active skills:

| Skill | Select it for | Database writes |
| --- | --- | --- |
| [`sql_health_triage`](sql_health_triage/SKILL.md) | Incident diagnosis, health sweeps, waits, blocking, deadlocks, tempdb, memory grants, resource pressure, and routing findings to an owner | Never; strictly read-only |
| [`sql_optimizer`](sql_optimizer/SKILL.md) | One supplied query: actual-plan analysis, semantically identical rewrite, index recommendations, and baseline/optimized/optimized+indexes evidence | Test-index operations only through gated MCP tools; production DDL is returned as a script |
| [`sql_plan_enforcer`](sql_plan_enforcer/SKILL.md) | Fleet-wide Query Store regression review and reversible plan controls with verification and rollback | Query Store plan forcing or hints only, behind independent gates |

The three skills share one execution dependency: the [`azure-sql-mcp`](../azure-sql-mcp/README.md)
server. They do not open direct database connections. The archived
[`legacy/query_geneva_db`](../legacy/query_geneva_db/) bundle is preserved for history only;
it is not an active skill, installer input, parity input, CI target, or recommendation for
new work.

## Select the skill

Use the skill whose scope matches the request, not merely the word "SQL". The frontmatter
names are hyphenated (`sql-health-triage`, `sql-optimizer`, and `sql-plan-enforcer`), while
the source and installed folder names use underscores.

| User context or trigger | Start here | Do not start here |
| --- | --- | --- |
| "Everything is slow", "queries are hanging", deadlock alerts, blocking, tempdb pressure, or "run a health check" | `sql_health_triage` | Do not rewrite SQL or apply a fix during triage |
| One query, an execution plan, SARGability, a parameter-sensitive query, or an index recommendation | `sql_optimizer` | Do not use it for a fleet-wide Query Store sweep |
| Query Store regressions across a database, unstable plans, stale forced plans, or a recurring monitoring tick | `sql_plan_enforcer` | Do not use it to rewrite SQL or change schema/data |
| A capacity decision, session kill, application change, or firewall/configuration change | The human owner, informed by triage evidence | No active skill is authorized to perform the decision |

When a request crosses boundaries, begin with the narrowest read-only stage. For example,
start with triage when the symptom is unknown, then hand off a query-shaped finding to the
optimizer or a plan-instability finding to the enforcer. Ask for the target database after
`list_databases`; never invent an alias or silently choose a default.

## Workflow handoffs

The maintained collection forms a detect -> stabilize/optimize -> verify loop:

```text
symptom or scheduled check
          |
          v
sql_health_triage  -- read-only evidence and owner classification
     |                         |
     | query-shaped            | plan instability
     v                         v
shared handoff queue      sql_plan_enforcer review
     |                         |
     v                         v
sql_optimizer ---------> redeploy verification in enforcer
```

- `sql_health_triage` records a report and may enqueue a complete query evidence pack for
  `sql_optimizer`. Truncated or incomplete evidence must not create a corrective handoff.
- `sql_plan_enforcer` owns fleet-wide Query Store scans and reversible plan controls. If a
  candidate needs a rewrite or index, enqueue it for `sql_optimizer` instead of changing
  SQL or schema in the enforcer.
- `sql_optimizer` claims a pack, follows its normal single-query workflow, records the
  resolution, and lets the enforcer re-verify a shipped rewrite or index.
- Review mode is the first contact for enforcer work. It reads and reports only. Dry-run is
  the default for enforcement; apply is a separately gated decision.
- Each skill returns incomplete evidence explicitly. `Unknown` is a valid result when the
  server or principal cannot provide a required fact.

## Prerequisites

### Runtime

- A reachable, correctly configured `azure-sql-mcp` server must expose the tools used by
  the selected skill. Live database work is not supported without it.
- The server must expose the intended target through `AZURE_SQL_ALLOWED_DATABASES`.
  Call `list_databases`, confirm the target with the user, and call the relevant capability
  or configuration tool before interpreting results.
- The principal needs only the permissions required by the selected read path for triage
  and optimizer analysis. Enforcer apply additionally requires Query Store to be usable and
  the principal to be authorized for the dedicated plan-control tools.
- Set `AZURE_SQL_QUERY_TIMEOUT_SECONDS` high enough for a known slow baseline. Keep
  `AZURE_SQL_ROW_LIMIT` as a display/fetch safety limit; never turn it into an invented SQL
  `TOP`, `OFFSET`, or row goal.

### Optional write-capable paths

- Optimizer test-index work needs an approved sandbox/test database in
  `AZURE_SQL_TEST_INDEX_DATABASES`, the server write policy, an explicit non-dry-run tool
  call, and user approval. Prefer a database clone.
- Enforcer apply needs the skill allowlist and the server write policy in addition to the
  per-call explicit non-dry-run flag. Query Store controls are the only permitted enforcer
  writes.
- Production optimizer deployment remains a human-reviewed script. The optimizer does not
  apply production index DDL as part of a normal single-query run.

### Local development

- Python 3.10 or newer is required by the bundled scripts.
- `pytest` is needed for the pure-function suites. The repository also supports running it
  through `uv` without changing the skill bundles.
- Tests are intentionally offline and do not require credentials, a database, or a running
  MCP server.

## Install and reinstall

The source root is `skills/`. From the repository root:

```bash
python3 skills/install_all.py --dest "$HOME/.copilot/skills"
python3 skills/check_installed_parity.py --dest "$HOME/.copilot/skills"
```

Or, from this directory:

```bash
python3 install_all.py --dest "$HOME/.copilot/skills"
python3 check_installed_parity.py --dest "$HOME/.copilot/skills"
```

`install_all.py` is the collection installer and its allowlist is deliberately the complete
active set: `sql_health_triage`, `sql_optimizer`, and `sql_plan_enforcer`. It does not scan,
copy, or install anything under `legacy/`, including `query_geneva_db`.

Reinstall with the same two commands after changing a skill. The installer stages the three
bundles before replacing managed files, prunes stale managed files from prior versions, and
preserves unmanaged runtime directories such as `audits/`, `experiments/`, `state/`, and
`__pycache__/`. It does not remove an old retired bundle that a host installed in the past;
that bundle is outside this collection's ownership and must not be reactivated.

Individual `install.py` files exist for packaging a single maintained bundle. Use them only
when a single-bundle install is intentional; run the collection installer for a normal
refresh so all three bundles stay in sync.

Do not run an installer from `legacy/query_geneva_db`. Its README is an archival deprecation
notice, not an installation guide.

## Parity checks

Run parity against the exact destination used for installation:

```bash
python3 skills/check_installed_parity.py --dest "$HOME/.copilot/skills"
```

The parity script checks only the three maintained bundles and their declared runtime files.
It detects missing, changed, stale, or symlinked files in managed trees while ignoring
`__pycache__`, `.DS_Store`, credential-named files, and intentionally unmanaged state
directories. A successful result names all three bundles. A legacy `query_geneva_db`
directory is neither required nor examined by this check.

If parity fails, reinstall to the same destination and run parity again. Do not copy files
by hand from an older source checkout; update the source under `skills/` and reinstall.

## Per-skill examples

These are trigger examples and workflow shapes, not database commands to run blindly.

### Health triage

Prompt shape:

```text
Use sql-health-triage. Everything is slow in the approved Azure SQL database.
Run incident triage, show the evidence behind each severity, and route only complete
actionable findings to the correct owner.
```

Expected flow:

1. Call `list_databases`, confirm the target, then call `check_capabilities` and
   `get_resource_limits`.
2. Follow the matching branch in `sql_health_triage/TriageGuide.md` or run its health-sweep
   path when no symptom is supplied.
3. Return `ReportGuide.md` format: evidence, severity, owner, incomplete observations, and
   human-only recommendations such as a possible `KILL` command. Never execute a kill or a
   write tool.
4. Log the completed report best-effort with `record_triage.py` and surface the resolved log
   path or the fact that logging is disabled.

### Single-query optimization

Prompt shape:

```text
Use sql-optimizer for this one Azure SQL query. Preserve result semantics, inspect the
actual plan if available, recommend only evidence-backed indexes, and benchmark the
baseline and rewrite before returning deployment scripts.
```

Expected flow:

1. Follow `SchemaGuide.md`, `queryguide.md`, `IndexingGuide.md`, `StyleGuide.md`, and
   `RunGuide.md` in that order.
2. Use `tune_query` and `benchmark_query_rewrite` where available, with explicit parameter
   values from the required Query Store buckets. Use `execute_sql` and `explain_query` for
   targeted read-only proof.
3. Prove result equivalence and return the three-scenario results matrix. A test index is
   disposable, `IX_Testing_`-prefixed, sandbox-approved, and created/dropped only through
   the dedicated gated tools. On older MCP servers, emit a script for an operator instead.
4. Record the audit last. Raw SQL is redacted by default; raw SQL persistence requires the
   explicit `SQL_OPTIMIZER_AUDIT_FULL_SQL=1` opt-in.

### Plan enforcement

Prompt shape:

```text
Use sql-plan-enforcer in review mode for a Query Store health snapshot. List regressions,
parameter-sensitive candidates, top consumers, and stale forced plans. Do not apply anything.
```

Expected flow:

1. Start with `ReviewGuide.md` for monitor-only work. Use `plan_health_review` plus the
   dedicated read-only Query Store scans and return a severity-grouped report.
2. For an enforcement request, run `ScanGuide.md` -> `scan_rank.py` -> `EnforceGuide.md`.
   Preview/dry-run is the default and emits exact apply/rollback scripts without executing.
3. Apply only after the skill allowlist, kill switch, `SQL_PLAN_ENFORCER_APPLY`, server
   `AZURE_SQL_WRITE_POLICY=apply`, explicit `dry_run=false`, and ledger preflight all pass.
4. Verify each control with `verify_decision.py`; keep only a measured improvement, revert a
   regression or no-op, and record every action in the fail-closed ledger.

## State and audit paths

Installation destination and runtime state are separate. The source move to `skills/` does
not migrate or delete an existing audit corpus.

| Skill | Override | Existing-host fallback | Default |
| --- | --- | --- | --- |
| Health triage audit | `SQL_HEALTH_TRIAGE_AUDIT_DIR` | Existing `~/.copilot/skills/sql_health_triage/audits/` | `~/.sql-skills/sql_health_triage/audits/` |
| Optimizer audit | `SQL_OPTIMIZER_AUDIT_DIR` | Existing `~/.copilot/skills/sql_optimizer/audits/` | `~/.sql-skills/sql_optimizer/audits/` |
| Optimizer experiments | `SQL_OPTIMIZER_EXPERIMENT_DIR` | Existing `~/.copilot/skills/sql_optimizer/experiments/` | `~/.sql-skills/sql_optimizer/experiments/` |
| Enforcer ledger | `SQL_PLAN_ENFORCER_AUDIT_DIR` | Existing `~/.copilot/skills/sql_plan_enforcer/audits/` | `~/.sql-skills/sql_plan_enforcer/audits/` |
| Enforcer coverage | `SQL_PLAN_ENFORCER_STATE` | Existing host state as documented by `LoopGuide.md` | `~/.sql-skills/sql_plan_enforcer/state/coverage.json` |
| Enforcer handoffs | `SQL_PLAN_ENFORCER_HANDOFF_DIR` | Existing `~/.copilot/skills/sql_plan_enforcer/handoffs/` | `~/.sql-skills/sql_plan_enforcer/handoffs/` |
| Enforcer allowlist | `SQL_PLAN_ENFORCER_ALLOWLIST` | Existing `~/.copilot/skills/sql_plan_enforcer/allowlist.json` | `~/.sql-skills/sql_plan_enforcer/allowlist.json` |

The audit toggles are `SQL_HEALTH_TRIAGE_AUDIT` and `SQL_OPTIMIZER_AUDIT`; `0`, `false`,
`off`, and `no` disable the corresponding best-effort log. The enforcer ledger is
safety-critical and fail-closed; do not disable or bypass it. Audit, state, handoff, and
allowlist files can contain query text, identifiers, or operational decisions. Keep their
directories owner-only, back them up securely, and never commit them.

## Write gates and safety boundaries

There are two separate safety layers: the skill's instructions/helpers and the MCP server's
tool policy. Passing one never authorizes the other.

### Health triage

Triage is permanently read-only. It can recommend a human session kill, capacity change, or
configuration change, but it never executes those actions and never creates a handoff from
truncated evidence.

### Optimizer

- `execute_sql` and `explain_query` remain read-only, regardless of server access mode.
- Test-index create/drop is the only normal tool-executed DDL path. It requires explicit user
  approval, `AZURE_SQL_WRITE_POLICY=apply`, `dry_run=false`, and membership of the target in
  `AZURE_SQL_TEST_INDEX_DATABASES`; names must use the `IX_Testing_` prefix. Prefer a clone.
- Production index/statistics/deployment changes are scripts for a human review. Do not route
  raw DDL through `execute_tsql_unrestricted`.

### Plan enforcer

- Review mode writes nothing: no apply, scripts staged for execution, ledger row, or coverage
  state change.
- Dry-run is the default. Live apply requires all of the following: kill switch disengaged,
  `SQL_PLAN_ENFORCER_APPLY` truthy, target environment and query id allowed by the skill
  allowlist, server `AZURE_SQL_WRITE_POLICY=apply`, explicit `dry_run=false`, and a successful
  `prepared` ledger write.
- Only reversible Query Store forcing and Query Store hint controls are allowed. Schema, data,
  statistics, and index changes go to the optimizer or the human.
- Every applied action gets post-change verification and an exact rollback path. Hold when
  provenance, completeness, or execution volume is insufficient.

## Testing and validation

The suites are pure-function and guide-invariant tests. They do not connect to a database.
From the repository root:

```bash
(cd skills && python3 -m pytest -q)
python3 -m compileall -q skills
```

From this directory, the equivalent focused run is:

```bash
python3 -m pytest
```

If the system interpreter does not provide `pytest`, use the repository's available virtual
environment or:

```bash
uv run --with pytest pytest
```

The active test configuration collects only `tests/`, `sql_health_triage/tests/`,
`sql_optimizer/tests/`, and `sql_plan_enforcer/tests/`. The archived `legacy/query_geneva_db`
tests are not an active CI target. After a local reinstall, run the parity command above as
the installed-copy check.

## Troubleshooting

### The MCP server is unavailable

Stop before live database work. Confirm the server registration/transport and that its
configured tool set includes the required high-level tool. Do not replace the server with a
direct database connection. Use the low-level read-only tools only as documented fallbacks.

### The target database is missing

Call `list_databases`, compare the result with the server's allowed-database configuration,
and ask the user to choose a returned database. Do not guess an alias or print credentials.

### Results are truncated or configuration is unknown

Treat `truncated: true`, missing capability data, unknown Query Store state, and unknown
provenance as incomplete evidence. Narrow the read, request the missing capability, or hold
the action. Do not convert an evidence gap into a confident recommendation.

### A baseline times out

Keep the query unchanged, raise `AZURE_SQL_QUERY_TIMEOUT_SECONDS` for the controlled run, and
repeat the same parameter set. Do not add a row limit or alter ordering to make the baseline
finish.

### An enforcer apply is denied

Remain in dry-run and report the first failed gate. Check the kill switch, apply flag, target
allowlist, server write policy, explicit per-call `dry_run=false`, Query Store configuration,
and the required ledger preflight. Never bypass a denial with unrestricted SQL.

### Parity reports drift

Use the same `--dest` for reinstall and parity. Check the named missing/changed/stale path,
then rerun `install_all.py`. Preserve unmanaged audit/state directories; do not copy from an
older source checkout.

### A retired `query_geneva_db` bundle is still present on the host

It is outside this collection's ownership. Do not run it or add it to an active install,
parity, test, or workflow path. Treat `legacy/query_geneva_db/README.md` as the authoritative
deprecation notice and route new work through `azure-sql-mcp` and the three maintained skills.
