# Report Guide

How a finished triage becomes the Triage Report, the handoff packs, and the log entry.

## 1. Normalize findings

Each observation from `TriageGuide.md` becomes one finding object:

```json
{
  "domain": "resource | blocking | deadlock | tempdb | memory | compile | waits | io | connections | config | other",
  "metric": "avg_cpu_percent",
  "value": 94.0,
  "threshold": 100.0,
  "query_id": 42,
  "summary": "CPU at 94% of the service objective ceiling over the last 30 min",
  "recommended_action": "top consumer is query 42 (68% of CPU) — optimize it before considering scale-up",
  "owner": "sql-optimizer",
  "evidence": {
    "tool": "get_resource_utilization",
    "window_minutes": 30,
    "truncated": false
  }
}
```

- `value`/`threshold` carry the number-vs-limit that Hard Rule 4 demands; `threshold` is the governance ceiling (`get_resource_limits`), capacity, or baseline the value is judged against.
- `evidence.tool` is mandatory and names the diagnostic tool that produced the number. Include the window and `truncated` flag when the tool exposes them; truncated evidence must be called out rather than presented as complete.
- `query_id` is present whenever a specific query is implicated — it is what makes the handoff pack buildable.
- `owner` is exactly one of `sql-optimizer`, `sql-plan-enforcer`, `human`.

`triage_report.py` preserves the input objects in the compatible `findings` field and
also returns three explicit buckets:

- `actionable_findings` — a threshold crossed with complete evidence; only these can carry an owner handoff or corrective action.
- `observations` — complete evidence that remains within the threshold; these are informational and do not create a handoff.
- `incomplete_evidence` — evidence with `evidence.truncated: true`; these are always inconclusive, have no owner, and must not carry a corrective action.

`healthy` is true when `actionable_findings` is empty, including when the report has
observations or incomplete evidence. `actionable_by_severity` and
`actionable_by_owner` summarize only work that may be routed. The compatible
`by_severity` and `by_owner` fields include complete observations as well.

## 2. Classify and render

```bash
python3 triage_report.py --input /tmp/findings.json --database <db>       # text report
python3 triage_report.py --input /tmp/findings.json --database <db> --json
```

Severity is rule-based (see `triage_report.py`'s docstring — resource ≥90% of ceiling is critical; active blocking, deadlocks, tempdb ≥80%, pending memory grants are high; exceeded thresholds are medium; in-range observations are info). Truncated evidence is `inconclusive` regardless of its value. Do not hand-assign severities in prose that disagree with the classifier — if a rule feels wrong, that is a change to propose to the rule table, not to improvise around.

An empty findings list, an all-in-range findings list, and a list containing only
truncated evidence all render a `healthy` report. Observations remain visible under
`OBSERVATIONS`; truncated evidence remains visible under `INCONCLUSIVE EVIDENCE` with
the instruction to narrow or re-query. Never render the caller-provided corrective
action or owner for truncated evidence.

## 3. Hand off

**Query-shaped culprits → sql-optimizer**, as evidence packs in the shared queue (the enforcer skill owns the queue module):

```bash
# build the pack from the finding: query_id, category "triage_<domain>", the metrics you
# observed, and the query text (fetch read-only from sys.query_store_query_text if needed)
python3 <sql_plan_enforcer dir>/handoff_queue.py add --input /tmp/pack.json
```

Set `source: "sql_health_triage"` in the pack. Record the returned pack ids for the report and the log. The queue is fail-closed — if `add` fails, say so in the report; do not pretend the handoff happened.

**Plan instability → sql-plan-enforcer.** No pack needed: the report's recommended action is to run its review mode (`plan_health_review` first). Include the query_ids and the regression evidence so that session starts warm.

**Human-owned findings** get the exact command or decision, never the execution — e.g. a kill recommendation is the `session_id`, the blocking evidence, and `KILL <spid>;` as text.

## 4. The Triage Report

```
Triage Report — <database> (<incident: "everything is slow"> | health sweep)
Capabilities: <what the principal could / could not see, if it limited the triage>

CRITICAL
  • resource/avg_cpu_percent — CPU at 94% of ceiling ... → ... (owner: sql-optimizer)
HIGH
  • blocking/waiting_tasks [query_id 87] — 14 sessions behind SPID 63 ... → KILL 63 recommended (owner: human)
...

Handoffs: pack 20260702T..__q42 (sql-optimizer) · plan review pointer (sql-plan-enforcer)
For the human: KILL 63 (head blocker, 41 min open txn) — command above, not executed
Logged: ~/.../sql_health_triage/audits/runs/<id>.md
```

Order is the classifier's order. Every actionable finding line ends with its owner.
Observation lines explicitly say they are informational only. Incomplete evidence
lines explicitly say to narrow or re-query and that there is no owner handoff. The
"For the human" block repeats anything a human must run — a recommendation buried
mid-report is a recommendation missed at 3am.

## 5. Log the session

```bash
python3 record_triage.py --input /tmp/triage.json
```

The session document carries `environment`, `mode` (`triage`/`sweep`), `symptom`, `outcome` (`resolved` / `handed_off_optimizer` / `handed_off_enforcer` / `escalated_human` / `inconclusive`), the normalized `findings`, `handoff_pack_ids`, and `notes`. The audit root and `runs/` directory are mode `0700`; audit files are mode `0600`. Detail creation and the durable `index.jsonl` append are serialized under one lock, and both are flushed before the lock is released. Confirm in one line at the end of the response — path on success, one-line note on failure, `logging disabled` when `SQL_HEALTH_TRIAGE_AUDIT=0`. The log is best-effort: a failed write never fails the triage.
