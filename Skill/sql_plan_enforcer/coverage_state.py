#!/usr/bin/env python3
"""Durable environment-scoped lifecycle state for the continuous loop.

Query Store ``query_id`` values are database-scoped.  Coverage therefore keys every
entry by ``(environment, query_id)`` and never lets a query observed in one database
 suppress work in another.  v1 state is migrated conservatively: entries without an
environment are retained as quarantined legacy records and cannot participate in a
new enforcement decision.

The pure functions are unit tested directly; the CLI only reads/writes the JSON state
file around them.  No function here touches a database.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import pathlib
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from enforcer_storage import atomic_write_text, exclusive_lock, secure_file

STATE_VERSION = 2
RESOLVED_STATES = {"kept", "reverted", "already_optimal", "handed_off", "skipped"}
IN_FLIGHT_STATES = {"pending_verify", "emitted", "redeploy_verify"}
ALL_STATES = RESOLVED_STATES | IN_FLIGHT_STATES | {"evaluated"}
_INITIAL_STATES = {
    "evaluated", "emitted", "pending_verify", "already_optimal", "handed_off", "skipped"
}
_ALLOWED_TRANSITIONS = {
    "evaluated": _INITIAL_STATES,
    "already_optimal": _INITIAL_STATES,
    "skipped": _INITIAL_STATES,
    "kept": _INITIAL_STATES,
    "reverted": _INITIAL_STATES,
    "handed_off": {"handed_off", "redeploy_verify", "evaluated", "already_optimal", "skipped"},
    "emitted": {"emitted", "pending_verify", "skipped"},
    # A live control cannot disappear from coverage as evaluated/skipped. It
    # remains in verification until explicitly kept or reverted.
    "pending_verify": {"pending_verify", "kept", "reverted"},
    "redeploy_verify": {"redeploy_verify", "kept", "evaluated", "skipped"},
}

_TS = "%Y-%m-%dT%H:%M:%SZ"
_LEGACY_PREFIX = "legacy-v1::"
_CANDIDATE_LEVERS = {"force_plan", "set_hints", "unforce_plan", "handoff_optimizer"}
_ACTIVE_CONTROL_STATES = {"pending_verify", "emitted"}


class StateCorruptError(ValueError):
    """Raised when existing coverage state cannot be trusted."""


def _now() -> datetime:
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


def _usable_environment(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
        and value.strip().lower() not in {"unknown", "none", "null"}
    )


def _normalize_environment(value: str) -> str:
    return value.strip().casefold()


def _usable_query_id(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _canonical_sql(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().rstrip(";").split()).casefold()


def _expected_rollback(lever: str, query_id: int, plan_id: int | None) -> str | None:
    if lever == "force_plan" and _usable_query_id(plan_id):
        return (
            f"exec sys.sp_query_store_unforce_plan @query_id = {query_id}, "
            f"@plan_id = {plan_id}"
        )
    if lever == "set_hints" and plan_id is None:
        return f"exec sys.sp_query_store_clear_hints @query_id = {query_id}"
    return None


def _contains_nonfinite_number(value: object) -> bool:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return False
    if isinstance(value, (int, float)):
        return not math.isfinite(float(value))
    if isinstance(value, dict):
        return any(
            not isinstance(key, str) or _contains_nonfinite_number(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_nonfinite_number(item) for item in value)
    return True


def coverage_key(environment: str, query_id: int) -> str:
    """Return a collision-safe JSON key for one environment/query identity."""
    if not _usable_environment(environment):
        raise ValueError("environment must be a non-empty, known string")
    if not _usable_query_id(query_id):
        raise ValueError("query_id must be an integer")
    return json.dumps(
        [_normalize_environment(environment), query_id],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _legacy_key(query_id: int) -> str:
    return f"{_LEGACY_PREFIX}{query_id}"


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
    return {
        "version": STATE_VERSION,
        "queries": {},
        "cursor": {"evaluated_count": 0, "updated_at": None},
    }


def state_path() -> pathlib.Path:
    override = os.environ.get("SQL_PLAN_ENFORCER_STATE")
    if override:
        return pathlib.Path(override).expanduser()
    legacy = pathlib.Path.home() / ".copilot" / "skills" / "sql_plan_enforcer" / "state" / "coverage.json"
    if legacy.exists():
        return legacy
    return pathlib.Path.home() / ".sql-skills" / "sql_plan_enforcer" / "state" / "coverage.json"


def _validate_entry(key: str, entry: object, *, allow_legacy: bool) -> None:
    if not isinstance(entry, dict) or not _usable_query_id(entry.get("query_id")):
        raise StateCorruptError(f"coverage state has an invalid query entry: {key!r}")
    state_name = entry.get("state")
    if state_name not in ALL_STATES:
        raise StateCorruptError(
            f"coverage state has an unknown state for query {key!r}: {state_name!r}"
        )
    environment = entry.get("environment")
    if allow_legacy and entry.get("legacy_v1_unscoped") is True and environment is None:
        if key != _legacy_key(entry["query_id"]):
            raise StateCorruptError(f"legacy coverage entry has an invalid key: {key!r}")
        return
    if not _usable_environment(environment):
        raise StateCorruptError(f"coverage state entry {key!r} has no usable environment")
    if key != coverage_key(environment, entry["query_id"]):
        raise StateCorruptError(f"coverage state key does not match its environment/query: {key!r}")
    attempts = entry.get("attempts", 0)
    if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 0:
        raise StateCorruptError(f"coverage state entry {key!r} has invalid attempts")
    for field in ("last_evaluated", "verify_after", "reevaluate_after"):
        value = entry.get(field)
        if value is not None and _parse(value) is None:
            raise StateCorruptError(f"coverage state entry {key!r} has invalid {field}")
    if "plan_id" in entry and entry["plan_id"] is not None and not _usable_query_id(entry["plan_id"]):
        raise StateCorruptError(f"coverage state entry {key!r} has invalid plan_id")
    if "baseline_metrics" in entry and not isinstance(entry["baseline_metrics"], dict):
        raise StateCorruptError(f"coverage state entry {key!r} has invalid baseline_metrics")
    if isinstance(entry.get("baseline_metrics"), dict) and _contains_nonfinite_number(
        entry["baseline_metrics"]
    ):
        raise StateCorruptError(
            f"coverage state entry {key!r} has invalid baseline_metrics values"
        )
    if "rollback_sql" in entry and not isinstance(entry["rollback_sql"], str):
        raise StateCorruptError(f"coverage state entry {key!r} has invalid rollback_sql")
    lever = entry.get("lever")
    if lever is not None and lever not in _CANDIDATE_LEVERS:
        raise StateCorruptError(f"coverage state entry {key!r} has invalid lever")
    if state_name in _ACTIVE_CONTROL_STATES:
        if lever not in {"force_plan", "set_hints"}:
            raise StateCorruptError(
                f"coverage state entry {key!r} must identify its active control lever"
            )
        if state_name == "emitted" and lever != "set_hints":
            raise StateCorruptError(
                f"coverage state entry {key!r} emitted state is only valid for set_hints"
            )
        baseline = entry.get("baseline_metrics")
        if not isinstance(baseline, dict) or not baseline:
            raise StateCorruptError(
                f"coverage state entry {key!r} must retain non-empty baseline_metrics"
            )
        expected_rollback = _expected_rollback(
            lever,
            entry["query_id"],
            entry.get("plan_id"),
        )
        if expected_rollback is None:
            raise StateCorruptError(
                f"coverage state entry {key!r} has invalid plan_id for {lever}"
            )
        if _canonical_sql(entry.get("rollback_sql")) != expected_rollback:
            raise StateCorruptError(
                f"coverage state entry {key!r} has unsafe or mismatched rollback_sql"
            )
    if state_name == "redeploy_verify":
        baseline = entry.get("baseline_metrics")
        if not isinstance(baseline, dict) or not baseline:
            raise StateCorruptError(
                f"coverage state entry {key!r} must retain non-empty baseline_metrics"
            )


def _validate_v2(state: dict) -> dict:
    if state.get("version") != STATE_VERSION or not isinstance(state.get("queries"), dict):
        raise StateCorruptError("coverage state must be version 2 with a queries object")
    for key, entry in state["queries"].items():
        _validate_entry(key, entry, allow_legacy=True)
    cursor = state.get("cursor")
    if not isinstance(cursor, dict):
        raise StateCorruptError("coverage state cursor must be an object")
    evaluated_count = cursor.get("evaluated_count")
    if (
        not isinstance(evaluated_count, int)
        or isinstance(evaluated_count, bool)
        or evaluated_count < 0
    ):
        raise StateCorruptError("coverage state cursor has invalid evaluated_count")
    updated_at = cursor.get("updated_at")
    if updated_at is not None and _parse(updated_at) is None:
        raise StateCorruptError("coverage state cursor has invalid updated_at")
    return state


def _migrate_v1(data: dict) -> dict:
    """Re-key v1 entries without inventing an environment.

    Entries that already carry a valid environment are re-keyed.  Older entries are
    preserved under a unique legacy key with no enforcement identity; they are visible
    for cleanup but never match a candidate in ``select_batch``.
    """
    old_queries = data.get("queries")
    if not isinstance(old_queries, dict):
        raise StateCorruptError("v1 coverage state has an invalid queries object")
    migrated = empty_state()
    cursor = data.get("cursor")
    if isinstance(cursor, dict):
        migrated["cursor"] = copy.deepcopy(cursor)
    migrated["migration"] = {
        "from_version": 1,
        "unscoped_entries": 0,
        "note": "legacy entries without environment are quarantined and never suppress work",
    }
    for old_key, raw_entry in old_queries.items():
        if not isinstance(raw_entry, dict) or not _usable_query_id(raw_entry.get("query_id")):
            raise StateCorruptError(f"v1 coverage state has an invalid query entry: {old_key!r}")
        entry = copy.deepcopy(raw_entry)
        environment = entry.get("environment")
        if _usable_environment(environment):
            key = coverage_key(environment, entry["query_id"])
            entry["environment"] = _normalize_environment(environment)
        else:
            key = _legacy_key(entry["query_id"])
            entry["environment"] = None
            entry["legacy_v1_unscoped"] = True
            migrated["migration"]["unscoped_entries"] += 1
        if key in migrated["queries"]:
            raise StateCorruptError(f"v1 migration found an ambiguous duplicate identity: {key!r}")
        _validate_entry(key, entry, allow_legacy=True)
        migrated["queries"][key] = entry
    return migrated


def _normalize_state(data: object) -> dict:
    if not isinstance(data, dict):
        raise StateCorruptError("coverage state must be a JSON object")
    version = data.get("version", 1)
    if version == 1:
        return _validate_v2(_migrate_v1(data))
    if version == STATE_VERSION:
        return _validate_v2(data)
    raise StateCorruptError(f"unsupported coverage state version: {version!r}")


def load_state(path: pathlib.Path | str | None = None) -> dict:
    path = pathlib.Path(path).expanduser() if path else state_path()
    if not path.exists():
        return empty_state()
    try:
        secure_file(path)
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise StateCorruptError(f"could not read coverage state {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise StateCorruptError(f"coverage state is not valid JSON: {path}: {exc}") from exc
    return _normalize_state(data)


def _save_state_unlocked(state: dict, path: pathlib.Path) -> None:
    _validate_v2(state)
    atomic_write_text(path, json.dumps(state, ensure_ascii=False, indent=2) + "\n")


def save_state(state: dict, path: pathlib.Path | str | None = None) -> None:
    path = pathlib.Path(path).expanduser() if path else state_path()
    with exclusive_lock(path.parent):
        _save_state_unlocked(state, path)


def _entry(state: dict, environment: str, query_id: int) -> dict | None:
    return state.get("queries", {}).get(coverage_key(environment, query_id))


def _candidate_identity(candidate: object) -> tuple[str, int] | None:
    if not isinstance(candidate, dict):
        return None
    environment = candidate.get("environment")
    query_id = candidate.get("query_id")
    if not _usable_environment(environment) or not _usable_query_id(query_id):
        return None
    return _normalize_environment(environment), query_id


def _contains_truncation(candidate: object) -> bool:
    if isinstance(candidate, dict):
        flag = candidate.get("truncated")
        if flag is True or (
            isinstance(flag, str) and flag.strip().lower() in {"1", "true", "yes", "y"}
        ):
            return True
        return any(_contains_truncation(value) for value in candidate.values())
    if isinstance(candidate, (list, tuple)):
        return any(_contains_truncation(value) for value in candidate)
    return False


def select_batch(
    state: dict,
    ranked_candidates: list,
    *,
    now: datetime | None = None,
    config: LoopConfig | None = None,
) -> dict:
    """Decide this tick's work. Invalid or unscoped candidates are never enforced."""
    state = _normalize_state(state)
    now = now or _now()
    config = config or LoopConfig()
    _validate_config(config)
    queries = state.get("queries", {})

    in_flight: set[tuple[str, int]] = set()
    for entry in queries.values():
        identity = _candidate_identity(entry)
        if identity and entry.get("state") in IN_FLIGHT_STATES:
            in_flight.add(identity)

    due_verify = []
    due_confirm = []
    due_redeploy = []
    due_lists = {
        "pending_verify": due_verify,
        "emitted": due_confirm,
        "redeploy_verify": due_redeploy,
    }
    for entry in queries.values():
        if entry.get("state") not in IN_FLIGHT_STATES or not _candidate_identity(entry):
            continue
        verify_after = _parse(entry.get("verify_after"))
        if verify_after is None or verify_after <= now:
            due_lists[entry["state"]].append(entry)

    to_enforce = []
    handoffs = []
    deferred = []
    rejected = []
    for candidate in ranked_candidates:
        if not isinstance(candidate, dict):
            rejected.append({
                "eligible": False,
                "reason": "candidate must be an object",
            })
            continue
        identity = _candidate_identity(candidate)
        if identity is None:
            rejected.append({
                **candidate,
                "eligible": False,
                "reason": "environment and integer query_id are required for coverage identity",
            })
            continue
        environment, query_id = identity
        eligible = candidate.get("eligible")
        if not isinstance(eligible, bool):
            rejected.append({
                **candidate,
                "eligible": False,
                "reason": "eligible must be boolean",
            })
            continue
        if not eligible:
            continue
        if _contains_truncation(candidate):
            rejected.append({
                **candidate,
                "eligible": False,
                "reason": "truncated candidate evidence cannot be enforced",
            })
            continue
        if candidate.get("review_only") or candidate.get("automatic_tuning_review_only"):
            rejected.append({
                **candidate,
                "eligible": False,
                "reason": candidate.get("review_reason", "review-only candidate"),
            })
            continue
        lever = candidate.get("proposed_lever")
        if lever not in _CANDIDATE_LEVERS:
            rejected.append({
                **candidate,
                "eligible": False,
                "reason": f"unsupported proposed lever {lever!r}",
            })
            continue
        if lever == "force_plan" and not _usable_query_id(candidate.get("proposed_plan_id")):
            rejected.append({
                **candidate,
                "eligible": False,
                "reason": "force_plan requires a positive proposed_plan_id",
            })
            continue
        if lever == "unforce_plan" and not _usable_query_id(candidate.get("current_plan_id")):
            rejected.append({
                **candidate,
                "eligible": False,
                "reason": "unforce_plan requires a positive current_plan_id",
            })
            continue
        existing = _entry(state, environment, query_id)
        if identity in in_flight:
            continue
        if existing and existing.get("state") in RESOLVED_STATES:
            reevaluate_after = _parse(existing.get("reevaluate_after"))
            if reevaluate_after and reevaluate_after > now:
                continue
        if lever == "handoff_optimizer":
            if existing and existing.get("state") == "handed_off":
                continue
            handoffs.append(candidate)
            continue
        if len(to_enforce) >= config.max_enforce_per_tick:
            deferred.append(candidate)
            continue
        to_enforce.append(candidate)

    return {
        "now": _iso(now),
        "due_verify": due_verify,
        "due_confirm": due_confirm,
        "due_redeploy": due_redeploy,
        "to_enforce": to_enforce,
        "handoffs": handoffs,
        "deferred": deferred,
        "rejected": rejected,
        "in_flight": [
            {"environment": environment, "query_id": query_id}
            for environment, query_id in sorted(in_flight)
        ],
    }


def _transition_allowed(current: str | None, new_state: str) -> bool:
    if current is None:
        return new_state in _INITIAL_STATES
    return new_state in _ALLOWED_TRANSITIONS.get(current, set())


def record_outcomes(
    state: dict,
    transitions: list,
    *,
    now: datetime | None = None,
    config: LoopConfig | None = None,
) -> dict:
    """Fold tick results into state, rejecting invalid identities and transitions."""
    state = _normalize_state(copy.deepcopy(state))
    if not isinstance(transitions, list):
        raise ValueError("transitions must be a JSON list")
    now = now or _now()
    config = config or LoopConfig()
    _validate_config(config)
    queries = state.setdefault("queries", {})

    for transition in transitions:
        if not isinstance(transition, dict):
            raise ValueError("each transition must be an object")
        environment = transition.get("environment")
        query_id = transition.get("query_id")
        if not _usable_environment(environment):
            raise ValueError("environment is required for every coverage transition")
        if not _usable_query_id(query_id):
            raise ValueError("query_id must be an integer for every coverage transition")
        new_state = transition.get("state")
        if new_state not in ALL_STATES:
            raise ValueError(f"unknown state {new_state!r} for {environment}/{query_id}")

        environment = _normalize_environment(environment)
        key = coverage_key(environment, query_id)
        entry = queries.get(key, {
            "environment": environment,
            "query_id": query_id,
            "attempts": 0,
        })
        current = entry.get("state")
        if not _transition_allowed(current, new_state):
            raise ValueError(
                f"illegal coverage transition for {environment}/{query_id}: "
                f"{current or 'unseen'} -> {new_state}"
            )
        entry["environment"] = environment
        entry["query_id"] = query_id
        entry["state"] = new_state
        entry["last_evaluated"] = _iso(now)
        for field in (
            "lever", "plan_id", "baseline_metrics", "rollback_sql", "notes", "category"
        ):
            if field in transition:
                entry[field] = transition[field]

        if new_state in IN_FLIGHT_STATES:
            entry["attempts"] = entry.get("attempts", 0) + 1
            entry["verify_after"] = _iso(now + timedelta(minutes=config.verify_wait_minutes))
            entry["reevaluate_after"] = None
        elif new_state in RESOLVED_STATES:
            entry["verify_after"] = None
            entry["reevaluate_after"] = _iso(now + timedelta(days=config.reevaluate_ttl_days))
        else:
            entry["verify_after"] = None
            entry["reevaluate_after"] = None
        queries[key] = entry

    state["cursor"] = {"evaluated_count": len(queries), "updated_at": _iso(now)}
    return _validate_v2(state)


def _validate_config(config: LoopConfig) -> None:
    values = (
        config.max_enforce_per_tick,
        config.verify_wait_minutes,
        config.reevaluate_ttl_days,
    )
    if any(not isinstance(value, int) or isinstance(value, bool) for value in values):
        raise ValueError("loop configuration values must be integers")
    if config.max_enforce_per_tick < 0:
        raise ValueError("max_enforce_per_tick must be zero or greater")
    if config.verify_wait_minutes <= 0:
        raise ValueError("verify_wait_minutes must be greater than zero")
    if config.reevaluate_ttl_days < 0:
        raise ValueError("reevaluate_ttl_days must be zero or greater")


def status(state: dict) -> dict:
    state = _normalize_state(state)
    counts: dict[str, int] = {}
    legacy_count = 0
    for entry in state.get("queries", {}).values():
        counts[entry.get("state", "unknown")] = counts.get(entry.get("state", "unknown"), 0) + 1
        if entry.get("legacy_v1_unscoped") is True:
            legacy_count += 1
    return {
        "evaluated_count": len(state.get("queries", {})),
        "by_state": counts,
        "pending_verify": counts.get("pending_verify", 0),
        "legacy_v1_unscoped": legacy_count,
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
    state_file = pathlib.Path(args.state).expanduser() if args.state else state_path()
    if args.cmd == "record":
        try:
            with exclusive_lock(state_file.parent):
                state = load_state(state_file)
                transitions = _read_json(args.outcomes)
                if isinstance(transitions, dict):
                    transitions = transitions.get("transitions", [])
                state = record_outcomes(state, transitions, config=config)
                _save_state_unlocked(state, state_file)
        except (StateCorruptError, OSError, ValueError, TypeError) as exc:
            print(f"coverage state record rejected: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(status(state), ensure_ascii=False, indent=2))
        return 0

    try:
        state = load_state(state_file)
    except StateCorruptError as exc:
        print(f"coverage state rejected: {exc}", file=sys.stderr)
        return 2

    if args.cmd == "status":
        print(json.dumps(status(state), ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "select":
        try:
            payload = _read_json(args.candidates)
            candidates = payload.get("candidates", payload) if isinstance(payload, dict) else payload
            batch = select_batch(state, candidates, config=config)
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            print(f"coverage selection rejected: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(batch, ensure_ascii=False, indent=2))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
