# Intake Guide (fleet queue)

On-demand mode — run it when the user asks to "work the fleet queue", "pick up handoffs",
or similar. `sql-plan-enforcer` (and `sql-health-triage`) enqueue **evidence packs** for
queries that need what only this skill produces: a rewrite or an index. Each pack carries
the evidence already gathered — Query Store metrics, plan ids, category, and usually the
query text — so an optimization starts warm instead of from a blank prompt.

This mode changes **where the query comes from**, not how it is optimized: every claimed
pack still runs the full per-query guide order from `SKILL.md` (schema check → analysis →
indexing → style → benchmark → audit). Do not skip steps because the pack "already has
evidence" — pack metrics are the *baseline reference*; you still capture a fresh actual
plan and prove equivalence.

## Queue location

The queue lives with the enforcer skill: `handoff_queue.py` in the installed
`sql_plan_enforcer` directory (a sibling of this skill's install directory), storage under
`$SQL_PLAN_ENFORCER_HANDOFF_DIR` (see that module's docstring for the default).

## Working the queue

1. **List and pick.**

   ```bash
   python3 <sql_plan_enforcer dir>/handoff_queue.py list --status open
   ```

   Prefer the oldest pack unless the user picks one. Read its `category`, `reason`,
   `evidence.metrics`, and `evidence.plan_ids` — that is the enforcer's case for why this
   query needs a rewrite.

2. **Claim it** (so a second session doesn't duplicate the work):

   ```bash
   python3 <sql_plan_enforcer dir>/handoff_queue.py claim <pack_id>
   ```

3. **Optimize** — run the standard guide order on `evidence.query_sql_text` (if the pack
   lacks the text, fetch it read-only from `sys.query_store_query_text` by `query_id`).
   Seed the work with the pack: its metrics are the production baseline the enforcer
   measured, its plan ids point at the Query Store history worth pulling, and its
   category says what to look for (`top_consumer` → cost; `param_sensitive` → sniffing).

4. **Complete the pack** — after the response (and the `AuditGuide.md` write), map the
   run outcome to a resolution:

   | run outcome | resolution |
   |---|---|
   | `improved` and the user deployed the rewrite/index | `{"outcome": "shipped", "rewrite_shipped": true, "optimizer_audit_id": "<audit id>"}` |
   | `already_optimal` / `no_change` | `{"outcome": "declined", "rewrite_shipped": false, "optimizer_audit_id": "<audit id>", "notes": "<why>"}` |
   | improved but **not yet deployed** | leave the pack `claimed`; add the audit id to notes; complete it when the user ships |

   ```bash
   python3 <sql_plan_enforcer dir>/handoff_queue.py complete <pack_id> --resolution /tmp/resolution.json
   ```

   `shipped` is the signal the enforcer's loop watches for: it moves the query to
   `redeploy_verify` and judges the rewrite against the pack baseline on later ticks. If
   that verification finds a regression, the pack comes back reopened — treat a reopened
   pack as a fresh intake with the regression note as new evidence.

## Boundaries

- One pack at a time; a pack claimed here must end `shipped`, `declined`, or explicitly
  left `claimed` with a note — never silently abandoned.
- The queue is the enforcer's data: never edit pack files directly; go through the
  `handoff_queue.py` CLI so the event log stays coherent.
- Deployment remains the user's action (`RunGuide.md` boundaries apply unchanged);
  `rewrite_shipped: true` records their decision, it does not perform it.
