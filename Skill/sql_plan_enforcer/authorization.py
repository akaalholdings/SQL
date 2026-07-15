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

Allowlist file (JSON): ``$SQL_PLAN_ENFORCER_ALLOWLIST``, else the legacy
``~/.copilot/skills/sql_plan_enforcer/allowlist.json`` when it exists, else the
host-neutral ``~/.sql-skills/sql_plan_enforcer/allowlist.json``::

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

from enforcer_storage import secure_file

_TRUTHY = {"1", "true", "on", "yes"}
_FALSEY = {"", "0", "false", "off", "no"}


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _TRUTHY


def kill_switch_engaged() -> bool:
    """Global stop. When truthy, nothing is applied, no matter what else is set."""
    raw = os.environ.get("SQL_PLAN_ENFORCER_DISABLE")
    if raw is None:
        return False
    # A typo in the emergency stop must halt applies, not silently disable the stop.
    return raw.strip().casefold() not in _FALSEY


def apply_mode() -> bool:
    """Live apply is OFF by default; the human turns it on explicitly."""
    return _truthy("SQL_PLAN_ENFORCER_APPLY")


def allowlist_path() -> pathlib.Path:
    override = os.environ.get("SQL_PLAN_ENFORCER_ALLOWLIST")
    if override:
        return pathlib.Path(override).expanduser()
    legacy = pathlib.Path.home() / ".copilot" / "skills" / "sql_plan_enforcer" / "allowlist.json"
    if legacy.exists():
        return legacy  # an established allowlist keeps gating after a host switch
    return pathlib.Path.home() / ".sql-skills" / "sql_plan_enforcer" / "allowlist.json"


def load_allowlist(path: pathlib.Path | str | None = None) -> dict:
    """Load the allowlist file. A missing/invalid file denies everything (fail-closed)."""
    path = pathlib.Path(path).expanduser() if path else allowlist_path()
    try:
        path = secure_file(path)
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def is_allowed(environment: str, query_id, allowlist: dict) -> tuple[bool, str]:
    """Check one target against an already-loaded allowlist."""
    if (
        not isinstance(environment, str)
        or not environment.strip()
        or environment.strip().casefold() in {"unknown", "none", "null"}
    ):
        return False, "environment is invalid"
    if not isinstance(query_id, int) or isinstance(query_id, bool) or query_id <= 0:
        return False, "query_id must be a positive integer"

    environments = allowlist.get("environments")
    normalized_environments = {
        item.strip().casefold()
        for item in environments or []
        if isinstance(item, str) and item.strip()
    }
    if (
        not isinstance(environments, list)
        or environment.strip().casefold() not in normalized_environments
    ):
        return False, f"environment {environment!r} not in allowlist"

    deny = allowlist.get("deny_query_ids") or []
    if isinstance(deny, list) and query_id in {
        item for item in deny if isinstance(item, int) and not isinstance(item, bool)
    }:
        return False, f"query_id {query_id} is denylisted"

    allowed = allowlist.get("query_ids")
    if allowed == "*":
        return True, "allowlisted (all query_ids in environment)"
    if isinstance(allowed, list) and query_id in {
        item for item in allowed if isinstance(item, int) and not isinstance(item, bool)
    }:
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
