# Loop Guide

How to run the enforcer **continuously**. Two operating shapes, same tick, same state:

- **Attended loop** — inside an agent CLI session you open while you work; you pace the ticks.
- **Scheduled ticks** — the agent harness runs sessions on a schedule (a recurring in-session loop, or cron-style scheduled agent sessions); each scheduled run executes exactly one tick and exits.

Either way there is no daemon of this skill's own: the agent drives ticks, and durable coverage state (`coverage_state.py`) carries progress **between ticks and between sessions**, so every session — human-opened or scheduled — resumes where the last one left off and keeps advancing toward "every query evaluated."

## Why a cadence, not a busy loop

Verification is wall-clock-paced: after forcing a plan or setting a hint you must **wait for real production traffic** to accumulate enough executions before Query Store can prove it helped. So a tick does not block on its own changes — it applies a small batch, then later ticks (often a different session, hours later) verify them once `verify_after` has elapsed and enough executions exist. Space ticks roughly every 10–30 minutes of working time; let in-flight changes ride across as many ticks/sessions as they need.

## Session start (resume)

```bash
python3 coverage_state.py status        # evaluated_count, by_state, pending_verify
python3 enforcement_ledger.py --pending # active controls; non-zero means unresolved prepared work
```

Run the `SafetyGuide.md` preflight (kill switch / apply mode / allowlist), state the run mode (dry-run vs gated apply), and report resume progress in one line, e.g. `Resumed: 412 queries evaluated, 3 controls active, dry-run mode.`

## One tick

Fast path: call `plan_enforcer_tick(window_minutes=1440, max_actions=<cap>, dry_run=true, database_name=<workload db>)`
to preview the next ranked force/unforce action through the server's audit path. The tick is
**preview-only** — never call it with `dry_run=false`. It applies under the server's own ranking
without this skill's ledger or coverage state, so a live tick and this loop can double-apply;
applies go exclusively through `force_query_plan` in step 4.

1. **Scan + rank** (`ScanGuide.md`): call the four server scan tools (`detect_regressed_queries`, `get_top_queries`, `detect_parameter_sniffing`, `get_forced_plans`) on the chosen workload database, normalize with `scan_adapter.py`, then `scan_rank.py --eligible-only`. Fall back to the `execute_sql` scan SQL when the tools come back empty or a threshold is borderline (`ScanGuide.md` fallback section).

2. **Plan the tick** — feed the ranked candidates to the state store:

   ```bash
   python3 coverage_state.py select --candidates /tmp/ranked.json > /tmp/batch.json
   ```

`batch.json` gives you: `due_verify` (in-flight controls whose window elapsed), `due_confirm` (emitted hint scripts awaiting human confirmation), `due_redeploy` (shipped rewrites awaiting re-verification), `to_enforce` (new candidates within the blast radius), `handoffs` (rewrite/index → `sql_optimizer`), `deferred` (over the cap; next tick), and `rejected` (ambiguous, truncated, or review-only candidates that require review).

3. **Confirm and verify due in-flight work first** (`EnforceGuide.md` §5–6): for each `due_confirm` query (an emitted hints script), ask whether the human ran it and check read-only via `sys.query_store_query_hints` — confirmed → transition to `pending_verify`; not run → leave it, or `skipped` after repeated attempts. Then for each `due_verify` query, fetch current metrics via `execute_sql`, run `verify_decision.py`. On `rollback`, execute the recorded rollback (`force_query_plan` for forced plans; emit the recorded `clear_hints` script for hints) and log it; on `keep`, leave it; on `hold`, leave it for a later tick.

4. **Enforce the new batch** (`EnforceGuide.md` §1–5): per candidate — choose lever, record baseline, append the `prepared` ledger row (fail-closed), apply through the gate, then append the confirmed outcome (or emit scripts if dry-run).

   For each `handoffs` candidate, build the evidence pack and enqueue it (fail-closed, like the ledger):

   ```bash
   # fetch the query text first (read-only), so the optimizer starts warm:
   #   execute_sql(sql="SELECT qt.query_sql_text FROM sys.query_store_query AS q
   #                    JOIN sys.query_store_query_text AS qt ON q.query_text_id = qt.query_text_id
   #                    WHERE q.query_id = <id>", database_name=<workload db>)
   python3 handoff_queue.py add --input /tmp/pack.json   # built via handoff_queue.build_pack
   ```

   Then transition the query to `handed_off` with the pack id in `notes`. Check the return path each tick: `handoff_queue.py list --status shipped` — any shipped pack whose query is still `handed_off` transitions to `redeploy_verify` so the rewrite gets judged like any other change.

   For each `due_redeploy` query, fetch current metrics via `execute_sql` and run `verify_decision.py` against the **pack's** baseline metrics: `keep` → `kept` (rewrite verified); `rollback` → transition to `evaluated` AND reopen the pack (`handoff_queue.py reopen <id> --note "post-deploy regression"`) — a rewrite is not this skill's lever, so there is nothing to revert here; `hold` → leave it for a later tick.

5. **Record outcomes** — fold every transition back into state:

   ```bash
   python3 coverage_state.py record --outcomes /tmp/transitions.json
   ```

   Each transition is `{environment, query_id, state, ...}` where `state` is one of `evaluated`, `emitted`, `pending_verify`, `kept`, `reverted`, `already_optimal`, `handed_off`, `redeploy_verify`, `skipped`. Applied controls become `pending_verify` (carry `environment`, `lever`, `plan_id`, non-empty `baseline_metrics`, and the exact generated `rollback_sql`); emitted hint scripts become `emitted` (same fields — the confirmation on a later tick promotes them to `pending_verify`); handoffs become `handed_off` (pack id in `notes`); shipped packs promote their query to `redeploy_verify` (carry the pack's baseline metrics); deferred become `skipped`; queries with nothing wrong become `already_optimal` (so the long tail gets marked covered, not re-scanned every tick). An active `pending_verify` control can transition only to `kept` or `reverted` (or remain pending); it cannot be forgotten as `evaluated`/`skipped`. Illegal transitions, non-finite baselines, and mismatched rollback SQL fail closed.

6. **Report** the tick: verified (kept/reverted), newly enforced, handed off, and updated coverage (`evaluated_count`, `pending_verify`).

`coverage_state.py` and the handoff queue are idempotent across ticks: an already
handed-off `(environment, query_id)` is not emitted again, and state/queue writes are
serialized and durable. Handoff packs are unresolved while `open` or `claimed`; shipped
and declined packs are terminal for dedupe. If a state file or pack is corrupt, the tick
fails closed and must be repaired or restored before any apply decision continues. An
unresolved `prepared` ledger row is also a hard stop: verify the target read-only, append
the confirmed outcome, and rerun `--pending` before selecting new work.

## Between ticks

Wait ~10–30 minutes (you are working; let traffic accrue). Then run the next tick. If you self-pace with the harness loop, schedule the next tick on that order of delay — not seconds. The point of waiting is to give in-flight controls executions to be judged on.

## Scheduled ticks (unattended operation)

Running ticks on a schedule turns the enforcer into a 24/7 loop with the same verify-or-revert discipline. Nothing in the tick changes — only who starts the session:

- **Cadence:** every 15–30 minutes fits the default `verify_wait_minutes=60` (each control gets judged within one or two ticks of its window elapsing). Slower cadences work; they just stretch verification latency. Never faster than ~10 minutes — there is nothing to learn before traffic accrues.
- **One tick per scheduled run.** The session starts, resumes from `coverage_state.py status` + `enforcement_ledger.py --pending`, runs the preflight and one tick, reports, and exits. No sleep-and-repeat inside a scheduled session — the scheduler is the pacing.
- **The gates do not relax.** A scheduled apply run needs exactly what an attended one needs: kill switch off, `SQL_PLAN_ENFORCER_APPLY` set, allowlist pass, server `AZURE_SQL_WRITE_POLICY=apply`. Start scheduled operation in **dry-run** for a few days and read the ledger before enabling apply.
- **The kill switch is the remote stop.** Set `SQL_PLAN_ENFORCER_DISABLE=1` (or clear `SQL_PLAN_ENFORCER_APPLY`) in the scheduled environment to halt applies without touching the schedule; ticks keep scanning and verifying read-only.
- **Emit-script levers are attended-only.** If the server lacks the hints tools (fallback mode, `EnforceGuide.md` §5), an unattended tick must not emit hint scripts into a report nobody reads: skip `set_hints` candidates (`skipped`, reason "unattended; emit-script lever needs an operator") and leave them for the next attended session. `due_confirm` items likewise wait for a human.
- **The ledger is the shift report.** Whoever reviews the loop reads `enforcement_ledger.py --pending` and the newest ledger rows; a scheduled run's response text is ephemeral, the ledger is not.

## Progressive coverage ("every single query")

- **Event-driven priority** rides on top: regressions and failing forced plans (`scan_rank.py` tiers 0–1) jump the queue every tick.
- **The long tail** is reached because resolved/evaluated queries enter a cooldown (`reevaluate_after`, default 7 days), so each tick's batch naturally moves on to queries not yet covered, while still re-checking old ones after the TTL (plans and data drift).
- Coverage is "done for now" when `status` shows the active query population in a resolved state; it is never permanently done — the TTL brings everything back around.

## Session end

Nothing to tear down. State is durable on disk — `$SQL_PLAN_ENFORCER_STATE`, else the legacy `~/.copilot/skills/sql_plan_enforcer/state/coverage.json` when it already exists, else `~/.sql-skills/sql_plan_enforcer/state/coverage.json` — with owner-only directory/file modes and atomic fsynced writes. The next session resumes from `coverage_state.py status` and `enforcement_ledger.py --pending`.
