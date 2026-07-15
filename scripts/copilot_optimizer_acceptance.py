#!/usr/bin/env python3
"""Print or validate the clean-room sql-optimizer Copilot acceptance scenario."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


PROMPT = """Use sql-optimizer in clean-room static mode.

Synthetic contract:
- dbo.SyntheticOrders(OrderId bigint NOT NULL, CreatedAt datetime2 NOT NULL)
- @TargetDate is date and is limited to 2000-01-01 through 9999-12-30
- preserve output names/types, duplicates, NULL behavior, and unordered row semantics
- MCP is unavailable

Query:
```sql
SELECT o.OrderId, o.CreatedAt
FROM dbo.SyntheticOrders AS o
WHERE CONVERT(date, o.CreatedAt) = @TargetDate;
```

A prior isolated index candidate was 12 percent slower and cleanup was confirmed.
Reject only that index and continue. Return the semantic contract, at least one
concrete rewrite labelled unmeasured, the losing experiment as regressed, and
the next evidence/experiment steps. Do not invent any other measurements.
"""


def validate_response(response: str) -> list[str]:
    lowered = response.casefold()
    compact = re.sub(r"\s+", " ", lowered)
    checks = {
        "concrete SQL code block": "```sql" in lowered,
        "unmeasured label": "unmeasured" in lowered,
        "semantic contract": (
            "semantic contract" in lowered
            or all(term in lowered for term in ("duplicate", "null", "order"))
        ),
        "losing index recorded": "index" in lowered and "regressed" in lowered,
        "session continues": any(
            phrase in lowered
            for phrase in (
                "continue",
                "next candidate",
                "next experiment",
                "next evidence",
                "next steps",
                "session_continues",
            )
        ),
        "SARGable lower bound": bool(
            re.search(r"createdat\s*(?:\]|\))?\s*>=\s*@targetdate", compact)
        ),
        "SARGable upper bound": (
            "createdat" in lowered
            and "<" in response
            and "dateadd" in lowered
            and "@targetdate" in lowered
        ),
    }
    return [name for name, passed in checks.items() if not passed]


def _read_response(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).expanduser().read_text(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--print-prompt", action="store_true")
    mode.add_argument("--response", help="Response file, or - for stdin.")
    args = parser.parse_args(argv)

    if args.print_prompt:
        print(PROMPT.rstrip())
        return 0

    response = _read_response(args.response)
    missing = validate_response(response)
    if missing:
        for requirement in missing:
            print(f"missing acceptance requirement: {requirement}", file=sys.stderr)
        return 1
    print("Copilot sql-optimizer clean-room acceptance passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
