#!/usr/bin/env python3
"""The apply gate for autonomous enforcement.

Autonomous apply only happens when three independent conditions all hold (fail-closed —
any one missing means dry-run only). This module makes the gate a tested decision instead
of prose the agent might skip (see ``SafetyGuide.md``):

1. Kill switch is OFF        — ``SQL_PLAN_ENFORCER_DISABLE`` is not truthy.
2. Apply mode is ON          — ``SQL_PLAN_ENFORCER_APPLY`` is truthy (default: dry-run).
3. Target is allowlisted     — environment + query_id pass the allowlist file.

The allowlist is the human's one-time, config-time approval (which moves the per-change
``mid_prod`` sign-off to config time, reconciling autonomy with the existing
"mid_prod = approval-only" convention).

Allowlist file (JSON), default ``~/.copilot/skills/sql_plan_enforcer/allowlist.json`` or
``$SQL_PLAN_ENFORCER_ALLOWLIST``::

    {
      "environments": ["mid_dev", "mid_prod"],
      "query_ids": "*",                 // or an explicit list of ints
      "deny_query_ids": [101, 102]      // always wins over allow
    }
"""

from __future__ import annotations

import json
import os
import pathlib

_TRUTHY = {"1", "true", "on", "yes"}


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _TRUTHY


def kill_switch_engaged() -> bool:
    """Global stop. When truthy, nothing is applied, no matter what else is set."""
    return _truthy("SQL_PLAN_ENFORCER_DISABLE")


def apply_mode() -> bool:
    """Live apply is OFF by default; the human turns it on explicitly."""
    return _truthy("SQL_PLAN_ENFORCER_APPLY")


def allowlist_path() -> pathlib.Path:
    override = os.environ.get("SQL_PLAN_ENFORCER_ALLOWLIST")
    if override:
        return pathlib.Path(override).expanduser()
    return pathlib.Path.home() / ".copilot" / "skills" / "sql_plan_enforcer" / "allowlist.json"


def load_allowlist(path: pathlib.Path | str | None = None) -> dict:
    """Load the allowlist file. A missing/invalid file denies everything (fail-closed)."""
    path = pathlib.Path(path).expanduser() if path else allowlist_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def is_allowed(environment: str, query_id, allowlist: dict) -> tuple[bool, str]:
    """Check one target against an already-loaded allowlist."""
    environments = allowlist.get("environments")
    if not isinstance(environments, list) or environment not in environments:
        return False, f"environment {environment!r} not in allowlist"

    deny = allowlist.get("deny_query_ids") or []
    if isinstance(deny, list) and query_id in deny:
        return False, f"query_id {query_id} is denylisted"

    allowed = allowlist.get("query_ids")
    if allowed == "*":
        return True, "allowlisted (all query_ids in environment)"
    if isinstance(allowed, list) and query_id in allowed:
        return True, "allowlisted"
    return False, f"query_id {query_id} not in allowlist"


def can_apply(
    environment: str, query_id, allowlist: dict | None = None
) -> tuple[bool, str]:
    """Full gate. Returns (allowed, reason); fail-closed at every step."""
    if kill_switch_engaged():
        return False, "kill switch engaged (SQL_PLAN_ENFORCER_DISABLE)"
    if not apply_mode():
        return False, "dry-run mode (SQL_PLAN_ENFORCER_APPLY not set) — emit scripts only"
    allowlist = load_allowlist() if allowlist is None else allowlist
    return is_allowed(environment, query_id, allowlist)
