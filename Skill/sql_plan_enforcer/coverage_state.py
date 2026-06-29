#!/usr/bin/env python3
"""Durable per-query lifecycle state for the continuous loop.

The enforcer runs as a loop *inside* a Copilot CLI session (see ``LoopGuide.md``), and the
user opens that session intermittently. This module is what makes the loop **resumable and
progressive** rather than repetitive: it remembers, per Query Store ``query_id``, where the
query is in its lifecycle so each tick (and each new session) advances the frontier instead
of re-doing the same top-N.

Lifecycle states::

    (unseen) -> evaluated         looked at, nothing to do right now
             -> pending_verify    a control was applied; awaiting post-change traffic
             -> kept              verified win; control left in place
             -> reverted          verified loss/no-op; control rolled back
             -> already_optimal   no improvement available
             -> handed_off        needs a rewrite/index -> sql_optimizer
             -> skipped           eligible but deferred (blast-radius cap, etc.)

Two pure functions do the thinking (unit tested directly); the CLI just reads/writes the
JSON state file around them:

- ``select_batch`` — given current state + freshly-ranked candidates + now, decide what is
  due for verification and which new candidates to enforce this tick (honoring in-flight
  exclusion, cooldown TTL, and the per-tick blast radius). Does NOT mutate state.
- ``record_outcomes`` — fold the tick's results back into state, set verify/re-evaluate
  timers, and advance the coverage counter.

Nothing here touches the database — the agent runs ``query_geneva_db`` and feeds JSON in/out.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

RESOLVED_STATES = {"kept", "reverted", "already_optimal", "handed_off", "skipped"}
ALL_STATES = RESOLVED_STATES | {"evaluated", "pending_verify"}

_TS = "%Y-%m-%dT%H:%M:%SZ"


def _now() -> datetime:
    # Naive UTC (matches the stored 'Z' strings parsed by _parse) without the
    # deprecated utcnow(); comparisons stay naive-vs-naive.
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _iso(dt: datetime) -> str:
    return dt.strftime(_TS)


def _parse(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, _TS)
    except (ValueError, TypeError):
        return None


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    try:
        return int(raw) if raw and raw.strip() else default
    except ValueError:
        return default


@dataclass(frozen=True)
class LoopConfig:
    max_enforce_per_tick: int = 3
    verify_wait_minutes: int = 60
    reevaluate_ttl_days: int = 7

    @classmethod
    def from_env(cls) -> "LoopConfig":
        return cls(
            max_enforce_per_tick=_env_int("SQL_PLAN_ENFORCER_MAX_ENFORCE_PER_TICK", 3),
            verify_wait_minutes=_env_int("SQL_PLAN_ENFORCER_VERIFY_WAIT_MINUTES", 60),
            reevaluate_ttl_days=_env_int("SQL_PLAN_ENFORCER_REEVALUATE_TTL_DAYS", 7),
        )


def empty_state() -> dict:
    return {"version": 1, "queries": {}, "cursor": {"evaluated_count": 0, "updated_at": None}}


def state_path() -> pathlib.Path:
    override = os.environ.get("SQL_PLAN_ENFORCER_STATE")
    if override:
        return pathlib.Path(override).expanduser()
    return pathlib.Path.home() / ".copilot" / "skills" / "sql_plan_enforcer" / "state" / "coverage.json"


def load_state(path: pathlib.Path | str | None = None) -> dict:
    path = pathlib.Path(path).expanduser() if path else state_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty_state()
    if not isinstance(data, dict) or "queries" not in data:
        return empty_state()
    return data


def save_state(state: dict, path: pathlib.Path | str | None = None) -> None:
    path = pathlib.Path(path).expanduser() if path else state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _entry(state: dict, query_id) -> dict | None:
    return state.get("queries", {}).get(str(query_id))


def select_batch(
    state: dict,
    ranked_candidates: list,
    *,
    now: datetime | None = None,
    config: LoopConfig | None = None,
) -> dict:
    """Decide this tick's work. Pure: reads state, mutates nothing."""
    now = now or _now()
    config = config or LoopConfig()
    queries = state.get("queries", {})

    in_flight = {
        int(q["query_id"])
        for q in queries.values()
        if q.get("state") == "pending_verify" and "query_id" in q
    }

    # 1. In-flight controls whose verification window has elapsed -> fetch fresh metrics + judge.
    due_verify = []
    for q in queries.values():
        if q.get("state") != "pending_verify":
            continue
        verify_after = _parse(q.get("verify_after"))
        if verify_after is None or verify_after <= now:
            due_verify.append(q)

    # 2. New eligible candidates to enforce, honoring exclusions + blast radius.
    to_enforce = []
    handoffs = []
    deferred = []
    for cand in ranked_candidates:
        if not cand.get("eligible"):
            continue
        qid = cand.get("query_id")
        if cand.get("proposed_lever") == "handoff_optimizer":
            handoffs.append(cand)
            continue
        if qid in in_flight:
            continue  # never stack a second control on an in-flight query
        existing = _entry(state, qid)
        if existing and existing.get("state") in RESOLVED_STATES:
            reevaluate_after = _parse(existing.get("reevaluate_after"))
            if reevaluate_after and reevaluate_after > now:
                continue  # cooling down — resolved recently, don't churn it
        if len(to_enforce) >= config.max_enforce_per_tick:
            deferred.append(cand)  # over the blast radius this tick; next tick
            continue
        to_enforce.append(cand)

    return {
        "now": _iso(now),
        "due_verify": due_verify,
        "to_enforce": to_enforce,
        "handoffs": handoffs,
        "deferred": deferred,
        "in_flight": sorted(in_flight),
    }


def record_outcomes(
    state: dict,
    transitions: list,
    *,
    now: datetime | None = None,
    config: LoopConfig | None = None,
) -> dict:
    """Fold tick results into state; set verify/re-evaluate timers; advance coverage."""
    now = now or _now()
    config = config or LoopConfig()
    queries = state.setdefault("queries", {})

    for t in transitions:
        qid = t.get("query_id")
        if qid is None:
            continue
        new_state = t.get("state")
        if new_state not in ALL_STATES:
            raise ValueError(f"unknown state {new_state!r} for query_id {qid}")

        key = str(qid)
        entry = queries.get(key, {"query_id": int(qid), "attempts": 0})
        entry["query_id"] = int(qid)
        entry["state"] = new_state
        entry["last_evaluated"] = _iso(now)
        for field in ("lever", "plan_id", "baseline_metrics", "rollback_sql", "notes", "category"):
            if field in t:
                entry[field] = t[field]

        if new_state == "pending_verify":
            entry["attempts"] = entry.get("attempts", 0) + 1
            entry["verify_after"] = _iso(now + timedelta(minutes=config.verify_wait_minutes))
            entry["reevaluate_after"] = None
        elif new_state in RESOLVED_STATES:
            entry["verify_after"] = None
            entry["reevaluate_after"] = _iso(now + timedelta(days=config.reevaluate_ttl_days))
        queries[key] = entry

    state["cursor"] = {
        "evaluated_count": len(queries),
        "updated_at": _iso(now),
    }
    return state


def status(state: dict) -> dict:
    counts: dict[str, int] = {}
    for q in state.get("queries", {}).values():
        counts[q.get("state", "unknown")] = counts.get(q.get("state", "unknown"), 0) + 1
    return {
        "evaluated_count": len(state.get("queries", {})),
        "by_state": counts,
        "pending_verify": counts.get("pending_verify", 0),
        "updated_at": state.get("cursor", {}).get("updated_at"),
    }


def _read_json(path: str):
    raw = sys.stdin.read() if path in (None, "-") else pathlib.Path(path).read_text("utf-8")
    return json.loads(raw)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Continuous-loop coverage state.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="Print coverage summary.")
    p_status.add_argument("--state", default=None)

    p_select = sub.add_parser("select", help="Decide this tick's verify + enforce batch.")
    p_select.add_argument("--candidates", "-c", default="-", help="Ranked candidates JSON; '-' stdin.")
    p_select.add_argument("--state", default=None)

    p_record = sub.add_parser("record", help="Fold tick outcomes back into state.")
    p_record.add_argument("--outcomes", "-o", default="-", help="Transitions JSON list; '-' stdin.")
    p_record.add_argument("--state", default=None)

    args = parser.parse_args(argv[1:])
    config = LoopConfig.from_env()
    state = load_state(args.state)

    if args.cmd == "status":
        print(json.dumps(status(state), ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "select":
        payload = _read_json(args.candidates)
        candidates = payload.get("candidates", payload) if isinstance(payload, dict) else payload
        batch = select_batch(state, candidates, config=config)
        print(json.dumps(batch, ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "record":
        transitions = _read_json(args.outcomes)
        if isinstance(transitions, dict):
            transitions = transitions.get("transitions", [])
        state = record_outcomes(state, transitions, config=config)
        save_state(state, args.state)
        print(json.dumps(status(state), ensure_ascii=False, indent=2))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
