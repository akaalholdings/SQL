# Audit Guide

The final, non-user-facing step of an optimization. After `RunGuide.md` finishes and result
equivalence has been evaluated, record exactly what was done to the durable audit corpus. The
corpus is what makes the skill self-improving: `ImproveGuide.md` mines it (on demand) for recurring
anti-patterns, the biggest measured wins, and the gaps where these guides fell short.

This step **does not change the seven-part response.** Produce the full answer first, then write the
audit and add a single one-line confirmation that the run was logged (see Privacy) — the write is
never silent, but it never alters the analysis above. A failed audit write is reported in one line
and never fails the optimization.

## When to record

Record once per run that completes through `RunGuide.md` — including runs that did **not** win. The
`outcome` field distinguishes quality; negatives (no-change, regressed, equivalence failures) are
high-value learning signal, not noise.

| outcome | use when |
|---|---|
| `improved` | optimized (or optimized+indexes) measurably beat baseline, equivalence proven |
| `no_change` | rewrite was valid but metrics were within noise |
| `already_optimal` | query was already well optimized; nothing meaningful to change |
| `regressed` | a candidate change made things worse (record it so we learn why) |
| `equivalence_failed` | a rewrite changed results; it was rejected |
| `abandoned` | run could not complete (missing plan, no environment, blocked DDL) |

Do not record purely hypothetical "no plan" responses that never ran through `RunGuide.md`.

## How to record

Same pattern as `RunGuide.md` writing `/tmp/*.sql`: write a run document to `/tmp/audit.json`, then
call the bundled writer (it lives next to this file in the installed skill directory):

```bash
python3 record_audit.py --input /tmp/audit.json
```

The writer derives `id`, `query_hash`, and `timestamp`; validates the record against the corpus
contract (`validate_audit.py`); appends one line to `audits/index.jsonl`; and writes the full detail
file `audits/runs/<id>.md`. It creates the corpus directory (with a `.gitignore` and `README.md`)
on first write.

### Run document shape (`/tmp/audit.json`)

Fill every field you can from the work you just did. Required: `query` and `outcome`. Everything
else defaults safely if omitted, but the more you record, the more useful the corpus.

```json
{
  "environment": "mid",
  "query": "<RAW original query, exactly as supplied>",
  "rewrite": "<RAW optimized query>",
  "scripts": { "index": "<CREATE INDEX ...>", "rollback": "<DROP ...>", "deploy": "<CREATE ... ONLINE=ON>" },
  "tables": ["dbo.orders", "Customers"],
  "anti_patterns": ["sargability", "select_star", "key_lookup"],
  "rules_applied": ["rule1_sargability", "rule2_select_star", "death_add_index"],
  "index_changes": { "adds": 1, "drops": 0, "alters": 0 },
  "metrics": {
    "baseline":          { "duration_ms": 0, "cpu_ms": 0, "logical_reads": 0, "physical_reads": 0, "rows": 0 },
    "optimized":         { "duration_ms": 0, "cpu_ms": 0, "logical_reads": 0, "physical_reads": 0, "rows": 0 },
    "optimized_indexed": { "duration_ms": 0, "cpu_ms": 0, "logical_reads": 0, "physical_reads": 0, "rows": 0 }
  },
  "improvement": { "duration_pct": 0, "logical_reads_pct": 0 },
  "equivalence_proven": true,
  "outcome": "improved",
  "guidance_gaps": [],
  "plan_findings": "<short narrative of the plan findings>",
  "what_changed": "<short narrative mapping each change to a rule/metric>",
  "notes": "<anything else worth remembering>"
}
```

### Field conventions

- **`query` / `rewrite` / `scripts`** — store them **raw and verbatim** (the corpus deliberately keeps
  the exact SQL). Preserve naming and schema qualifiers exactly as written; do not reformat.
- **`tables`** — objects referenced, schema-qualified exactly as in the query (unqualified stays
  unqualified, per `SchemaGuide.md`).
- **`anti_patterns` / `rules_applied`** — short stable slugs tied to `queryguide.md` (Rules 1–10 and
  the D.E.A.T.H. method). Reuse existing slugs across runs so they aggregate; do not invent a new
  slug for an existing rule.
- **`improvement`** — percentages vs baseline (positive = better). Compute from `metrics`.
- **`guidance_gaps`** — **the most important field for self-improvement.** Whenever a guide was
  ambiguous, silent, or insufficient for this query — you had to improvise, guess a convention, or
  wished a rule existed — capture it here as a short, specific note (e.g.
  `"StyleGuide silent on formatting FILTER-style conditional aggregates"`). Leave empty `[]` only when
  the guides genuinely covered the case cleanly.

## Privacy

The corpus persists **raw SQL** (original query, rewrite, and scripts) to
`~/.copilot/skills/sql_optimizer/audits/` (override with `SQL_OPTIMIZER_AUDIT_DIR`). Because the
content is sensitive, logging is never silent and is opt-out:

- **Surface it.** After writing, add one line to the response with what was logged and where, e.g.
  `Logged this run to the audit corpus: ~/.copilot/skills/sql_optimizer/audits/ (raw SQL; set SQL_OPTIMIZER_AUDIT=0 to disable).`
  Relay the path `record_audit.py` prints rather than guessing it.
- **Opt out.** Logging is on by default. If `SQL_OPTIMIZER_AUDIT` is `0`/`false`/`off`/`no`,
  `record_audit.py` records nothing; say so in the same one-line slot.
- Treat the corpus directory as sensitive: secure and back it up, and do not commit it. The writer
  drops a `.gitignore` as insurance, but the directory is the user's to protect.

## Failure handling

If `record_audit.py` exits non-zero, surface a single line (e.g. "Note: audit log write failed —
<reason>") and stop. Never retry destructively, never alter the optimization answer, and never block
the user on the audit step.
