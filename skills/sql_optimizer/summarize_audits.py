#!/usr/bin/env python3
"""Aggregate the sql_optimizer audit corpus into cheap, scannable stats.

This is the aggregation layer that makes the self-improvement review pass
(``ImproveGuide.md``) viable across hundreds of records: scan ``index.jsonl`` once
and surface what to look at, so only outlier/recurring detail files get opened.

    python3 summarize_audits.py                 # default corpus, text report
    python3 summarize_audits.py path/to/index.jsonl
    python3 summarize_audits.py --json          # machine-readable

Malformed rows are skipped (and counted) so one bad line never blocks the report.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import statistics
import sys
from collections import Counter, defaultdict

from validate_audit import validate


def default_index() -> pathlib.Path:
    override = os.environ.get("SQL_OPTIMIZER_AUDIT_DIR")
    if override:
        return pathlib.Path(override).expanduser() / "index.jsonl"
    legacy = pathlib.Path.home() / ".copilot" / "skills" / "sql_optimizer" / "audits"
    root = legacy if legacy.exists() else (
        pathlib.Path.home() / ".sql-skills" / "sql_optimizer" / "audits"
    )
    return root / "index.jsonl"


def load_records(index_path: pathlib.Path) -> tuple[list[dict], int]:
    records: list[dict] = []
    skipped = 0
    if not index_path.exists():
        return records, skipped
    for raw in index_path.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            skipped += 1
            continue
        if validate(record):
            skipped += 1
            continue
        records.append(record)
    return records, skipped


def _mean(values: list[float]) -> float:
    return round(statistics.fmean(values), 1) if values else 0.0


def summarize(records: list[dict]) -> dict:
    outcomes = Counter(r["outcome"] for r in records)
    anti_patterns = Counter(ap for r in records for ap in r["anti_patterns"])
    rules = Counter(rule for r in records for rule in r["rules_applied"])
    gaps = Counter(gap for r in records for gap in r["guidance_gaps"])

    # Improvement aggregated overall and per applied rule, keyed by metric (e.g. duration_pct).
    overall: dict[str, list[float]] = defaultdict(list)
    per_rule: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in records:
        for metric, value in r["improvement"].items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                overall[metric].append(float(value))
                for rule in r["rules_applied"]:
                    per_rule[rule][metric].append(float(value))

    repeats = Counter(r["query_hash"] for r in records)
    repeated = {h: n for h, n in repeats.items() if n > 1}

    return {
        "total": len(records),
        "outcomes": dict(outcomes.most_common()),
        # A rejected rewrite (outcome == "equivalence_failed") is distinct from a run
        # where equivalence simply was not proven (abandoned, already-optimal, no DDL, ...).
        "equivalence_failed": outcomes.get("equivalence_failed", 0),
        "equivalence_not_proven": sum(1 for r in records if not r["equivalence_proven"]),
        "top_anti_patterns": anti_patterns.most_common(15),
        "rules_applied": rules.most_common(),
        "top_guidance_gaps": gaps.most_common(15),
        "improvement_overall": {m: _mean(v) for m, v in sorted(overall.items())},
        "improvement_by_rule": {
            rule: {m: _mean(v) for m, v in sorted(metrics.items())}
            for rule, metrics in sorted(per_rule.items())
        },
        "repeated_queries": len(repeated),
    }


def _render_pairs(pairs: list, empty: str = "  (none)") -> str:
    if not pairs:
        return empty
    return "\n".join(f"  {name}: {count}" for name, count in pairs)


def render_text(summary: dict, skipped: int) -> str:
    lines = [
        "sql_optimizer audit summary",
        "===========================",
        f"runs recorded: {summary['total']}  (skipped malformed rows: {skipped})",
        f"repeated queries (same hash seen >1x): {summary['repeated_queries']}",
        f"rejected rewrites (outcome=equivalence_failed): {summary['equivalence_failed']}",
        f"equivalence not proven (any reason): {summary['equivalence_not_proven']}",
        "",
        "Outcomes:",
        _render_pairs(list(summary["outcomes"].items())),
        "",
        "Top anti-patterns:",
        _render_pairs(summary["top_anti_patterns"]),
        "",
        "Rules applied (frequency):",
        _render_pairs(summary["rules_applied"]),
        "",
        "Mean improvement overall (per metric, %):",
        _render_pairs(list(summary["improvement_overall"].items())),
        "",
        "Recurring guidance gaps (candidates for guide edits):",
        _render_pairs(summary["top_guidance_gaps"]),
    ]
    if summary["improvement_by_rule"]:
        lines += ["", "Mean improvement by rule (per metric, %):"]
        for rule, metrics in summary["improvement_by_rule"].items():
            metric_str = ", ".join(f"{m} {v}" for m, v in metrics.items())
            lines.append(f"  {rule}: {metric_str}")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Summarize the sql_optimizer audit corpus.")
    parser.add_argument("index", nargs="?", help="Path to index.jsonl (defaults to the corpus).")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args(argv[1:])

    index_path = pathlib.Path(args.index).expanduser() if args.index else default_index()
    records, skipped = load_records(index_path)
    summary = summarize(records)

    if args.json:
        print(json.dumps({"summary": summary, "skipped": skipped}, ensure_ascii=False, indent=2))
    else:
        print(render_text(summary, skipped))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
