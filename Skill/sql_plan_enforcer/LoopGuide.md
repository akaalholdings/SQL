# Loop Guide

How to run the enforcer **continuously** — as a loop inside a Copilot CLI session you open while you work. There is no daemon or cron: the agent drives ticks, and durable coverage state (`coverage_state.py`) carries progress **between ticks and between sessions**, so each time you open the session it resumes where the last one left off and keeps advancing toward "every query evaluated."

## Why a cadence, not a busy loop

Verification is wall-clock-paced: after forcing a plan or setting a hint you must **wait for real production traffic** to accumulate enough executions before Query Store can prove it helped. So a tick does not block on its own changes — it applies a small batch, then later ticks (often a different session, hours later) verify them once `verify_after` has elapsed and enough executions exist. Space ticks roughly every 10–30 minutes of working time; let in-flight changes ride across as many ticks/sessions as they need.

## Session start (resume)

```bash
python3 coverage_state.py status        # evaluated_count, by_state, pending_verify
python3 enforcement_ledger.py --pending # every control currently in place + its rollback
```

Run the `SafetyGuide.md` preflight (kill switch / apply mode / allowlist), state the run mode (dry-run vs gated apply), and report resume progress in one line, e.g. `Resumed: 412 queries evaluated, 3 controls active, dry-run mode.`

## One tick

1. **Scan + rank** (`ScanGuide.md`): run the four read-only Query Store scans on `mid`, merge rows, `scan_rank.py --eligible-only`.

2. **Plan the tick** — feed the ranked candidates to the state store:

   ```bash
   python3 coverage_state.py select --candidates /tmp/ranked.json > /tmp/batch.json
   ```

   `batch.json` gives you: `due_verify` (in-flight controls whose window elapsed), `to_enforce` (new candidates within the blast radius), `handoffs` (rewrite/index → `sql_optimizer`), and `deferred` (over the cap; next tick).

3. **Verify the due in-flight changes first** (`EnforceGuide.md` §6): for each `due_verify` query, fetch current metrics via `query_geneva_db`, run `verify_decision.py`. On `rollback`, execute the recorded rollback and log it; on `keep`, leave it; on `hold`, leave it for a later tick.

4. **Enforce the new batch** (`EnforceGuide.md` §1–5): per candidate — choose lever, record baseline, write the ledger (fail-closed), then apply through the gate (or emit script if dry-run).

5. **Record outcomes** — fold every transition back into state:

   ```bash
   python3 coverage_state.py record --outcomes /tmp/transitions.json
   ```

   Each transition is `{query_id, state, ...}` where `state` is one of `evaluated`, `pending_verify`, `kept`, `reverted`, `already_optimal`, `handed_off`, `skipped`. Applied controls become `pending_verify` (carry `lever`, `plan_id`, `baseline_metrics`, `rollback_sql`); handoffs become `handed_off`; deferred become `skipped`; queries with nothing wrong become `already_optimal` (so the long tail gets marked covered, not re-scanned every tick).

6. **Report** the tick: verified (kept/reverted), newly enforced, handed off, and updated coverage (`evaluated_count`, `pending_verify`).

## Between ticks

Wait ~10–30 minutes (you are working; let traffic accrue). Then run the next tick. If you self-pace with the harness loop, schedule the next tick on that order of delay — not seconds. The point of waiting is to give in-flight controls executions to be judged on.

## Progressive coverage ("every single query")

- **Event-driven priority** rides on top: regressions and failing forced plans (`scan_rank.py` tiers 0–1) jump the queue every tick.
- **The long tail** is reached because resolved/evaluated queries enter a cooldown (`reevaluate_after`, default 7 days), so each tick's batch naturally moves on to queries not yet covered, while still re-checking old ones after the TTL (plans and data drift).
- Coverage is "done for now" when `status` shows the active query population in a resolved state; it is never permanently done — the TTL brings everything back around.

## Session end

Nothing to tear down. State is durable on disk (`~/.copilot/skills/sql_plan_enforcer/state/coverage.json`, override `SQL_PLAN_ENFORCER_STATE`) and the ledger records every active control. The next session resumes from `coverage_state.py status` and `enforcement_ledger.py --pending`.
