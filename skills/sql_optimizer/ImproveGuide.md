# Improve Guide (self-improvement review)

On-demand only. This is **not** part of the per-query guide order — run it only when the user
explicitly asks to "review audits", "improve the guides", or similar. It reads the accumulated audit
corpus (`AuditGuide.md` writes it) and produces a **report** of what the skill should learn.

**Report-only. This pass never edits the guides.** It proposes concrete wording and names the target
guide, but every change is a suggestion for the user to accept or reject. Do not modify
`queryguide.md`, `StyleGuide.md`, `SchemaGuide.md`, `RunGuide.md`, `SKILL.md`, or `Examples.md` here.

## Process (built to scale to hundreds of runs)

1. **Aggregate first — cheap.** Run the bundled summarizer over the compact index; do not read every
   detail file:

   ```bash
   python3 summarize_audits.py            # human-readable
   python3 summarize_audits.py --json     # machine-readable, for precise counts
   ```

   It reports outcome distribution, top anti-patterns, rule-application frequency, mean improvement
   overall and **by rule**, recurring `guidance_gaps`, repeated queries, and equivalence failures.
   Malformed rows are skipped automatically.

2. **Open detail files only for the signal.** For the top recurring patterns, the biggest wins/losses,
   and the most frequent `guidance_gaps`, open the specific `audits/runs/<id>.md` files the summary
   points to. Do not bulk-read the corpus.

3. **Write the report.** Save it to `audits/reports/<YYYY-MM-DD>-review.md` and also return it in chat.

## What the report must cover

- **Recurring anti-patterns** — most frequent, with counts. These are the cases worth making the guides
  sharper about.
- **Biggest measured wins by rule** — which `rules_applied` correlate with the largest `improvement`.
  Evidence for promoting a rule's prominence or adding a worked example.
- **Rules that rarely or never fire** — possible dead weight or unclear guidance in `queryguide.md`.
- **Recurring `guidance_gaps`** — the heart of the pass. Each recurring gap becomes a proposal:
  - the target guide (`queryguide.md` / `StyleGuide.md` / `SchemaGuide.md` / `RunGuide.md`),
  - proposed wording (a concrete sentence or rule), **as a suggestion only**,
  - the audit `id`s that motivate it (evidence).
- **Equivalence-failure patterns** — rewrites that changed results. Surface the shared cause so a
  guard can be proposed for `queryguide.md`.
- **Suggested new `Examples.md` cases** — distinct, instructive runs worth turning into a worked example, prepared per "Promoting field examples" below.

## Promoting field examples

Real before/afters from the audit corpus become the "Field examples" section of `Examples.md` — measured numbers instead of illustrative ones. This is a **proposal** like everything else in this pass: prepare the candidate in the report; the user approves the edit.

**Prerequisite.** Promotion needs the raw SQL, which the corpus only holds when runs were recorded with `SQL_OPTIMIZER_AUDIT_FULL_SQL=1` (see `AuditGuide.md` Privacy). A redacted corpus can still *identify* promotion candidates by rule/metrics, but the example must then be reconstructed with the user, not from the corpus.

**Selection criteria** — an example earns a slot only when it teaches something the synthetic examples (Rules 1–16 shapes) don't:

- **Recurrence**: the same anti-pattern fired across 3+ distinct queries (the summary's top anti-patterns / repeated queries).
- **Negatives outrank wins**: an `equivalence_failed` or `regressed` run — a rewrite that looked right and changed results or made things worse — is the most instructive artifact the corpus produces. Prefer one real failure over several routine wins.
- **Surprise**: a rule interaction, plan behavior, or measured outcome that contradicts the guide's default expectation.
- Routine wins that a synthetic example already covers do not qualify, however large the improvement.

**Anonymization — mandatory, no exceptions.** `Examples.md` is shipped, installed content; the corpus is sensitive and gitignored. Before anything leaves the corpus:

- Rename every object to the neutral example domain (`dbo.orders`, `dbo.customers`, ...), preserving the *shape* exactly — same column count, same join topology, same predicate structure, same data types.
- Strip or neutralize every literal that could identify a business, person, product, or environment; parameter names become generic (`@customer_id`, `@order_date_from`).
- Keep the measured numbers verbatim (durations, reads, row counts, medians) — they are the value of a field example and identify nothing.
- The finished example must read like the synthetic ones; if the shape cannot be preserved through renaming, it does not get promoted.

**Format and cap.** Each field example follows the compact Rules 11–16 example shape, plus one line naming the corpus run id (e.g. `Promoted from audit run 20260815T…`) and the real results-matrix row (median, min/max). The section holds **at most 5** examples — `Examples.md` is loaded on every run, so each slot costs context. When the section is full, a new candidate must displace the weakest current one (state which and why in the report).

## Boundaries

- Tie every proposal to audit evidence (`id`s / counts). No proposals from intuition alone — that would
  reintroduce the guesswork the skill is built to avoid.
- Keep raw SQL out of the report unless a specific snippet is needed to make a proposal concrete; the
  report is meant to be shareable, the corpus is not.
- Make no guide edits. End the report with a short "Proposed edits (await approval)" list so the user
  can act on it deliberately.
