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
- **Suggested new `Examples.md` cases** — distinct, instructive runs worth turning into a worked example.

## Boundaries

- Tie every proposal to audit evidence (`id`s / counts). No proposals from intuition alone — that would
  reintroduce the guesswork the skill is built to avoid.
- Keep raw SQL out of the report unless a specific snippet is needed to make a proposal concrete; the
  report is meant to be shareable, the corpus is not.
- Make no guide edits. End the report with a short "Proposed edits (await approval)" list so the user
  can act on it deliberately.
