"""Coverage state is what makes the loop resumable and progressive: it must not stack
controls on in-flight queries, must respect the per-tick blast radius and the cooldown TTL,
must surface in-flight changes once their verify window elapses, and must round-trip to disk
so a new session resumes where the last left off."""

import json
import stat
from datetime import datetime, timedelta

import pytest

from coverage_state import (
    LoopConfig,
    StateCorruptError,
    coverage_key,
    empty_state,
    load_state,
    record_outcomes,
    save_state,
    select_batch,
    status,
)

NOW = datetime(2026, 6, 28, 12, 0, 0)
CFG = LoopConfig(max_enforce_per_tick=2, verify_wait_minutes=60, reevaluate_ttl_days=7)
ENV = "awlt_prod"


def cand(query_id, lever="force_plan", eligible=True):
    return {"environment": ENV, "query_id": query_id, "proposed_lever": lever, "eligible": eligible,
            "proposed_plan_id": 7, "current_plan_id": 8, "category": "regression"}


def control_transition(query_id=1, *, state="pending_verify", environment=ENV,
                       lever="force_plan"):
    if lever == "force_plan":
        plan_id = 7
        rollback_sql = (
            f"EXEC sys.sp_query_store_unforce_plan @query_id = {query_id}, "
            f"@plan_id = {plan_id};"
        )
    else:
        plan_id = None
        rollback_sql = f"EXEC sys.sp_query_store_clear_hints @query_id = {query_id};"
    return {
        "environment": environment,
        "query_id": query_id,
        "state": state,
        "lever": lever,
        "plan_id": plan_id,
        "rollback_sql": rollback_sql,
        "baseline_metrics": {"avg_duration": 1000, "count_executions": 50},
    }


def test_blast_radius_caps_enforced_per_tick():
    batch = select_batch(empty_state(), [cand(1), cand(2), cand(3)], now=NOW, config=CFG)
    assert [c["query_id"] for c in batch["to_enforce"]] == [1, 2]
    assert [c["query_id"] for c in batch["deferred"]] == [3]


def test_ineligible_and_handoff_are_not_enforced():
    cands = [cand(1, eligible=False), cand(2, lever="handoff_optimizer")]
    batch = select_batch(empty_state(), cands, now=NOW, config=CFG)
    assert batch["to_enforce"] == []
    assert [c["query_id"] for c in batch["handoffs"]] == [2]


def test_in_flight_query_is_not_re_enforced():
    state = record_outcomes(
        empty_state(),
        [control_transition()],
        now=NOW, config=CFG,
    )
    batch = select_batch(state, [cand(1)], now=NOW, config=CFG)
    assert batch["to_enforce"] == []
    assert batch["in_flight"] == [{"environment": ENV, "query_id": 1}]


def test_in_flight_or_cooldown_query_is_not_handed_off():
    state = record_outcomes(
        empty_state(), [control_transition()], now=NOW, config=CFG,
    )
    handoff = cand(1, lever="handoff_optimizer")
    assert select_batch(state, [handoff], now=NOW, config=CFG)["handoffs"] == []

    state = record_outcomes(
        state,
        [{"environment": ENV, "query_id": 1, "state": "kept"}],
        now=NOW,
        config=CFG,
    )
    assert select_batch(
        state, [handoff], now=NOW + timedelta(days=1), config=CFG,
    )["handoffs"] == []


def test_pending_verify_surfaces_only_after_window():
    state = record_outcomes(
        empty_state(),
        [control_transition()],
        now=NOW, config=CFG,
    )
    # 30 min later: not yet due (60 min window)
    early = select_batch(state, [], now=NOW + timedelta(minutes=30), config=CFG)
    assert early["due_verify"] == []
    # 90 min later: due for verification
    later = select_batch(state, [], now=NOW + timedelta(minutes=90), config=CFG)
    assert [q["query_id"] for q in later["due_verify"]] == [1]


def test_recently_resolved_query_is_in_cooldown():
    state = record_outcomes(
        empty_state(), [control_transition()], now=NOW, config=CFG
    )
    state = record_outcomes(
        state, [{"environment": ENV, "query_id": 1, "state": "kept"}], now=NOW, config=CFG
    )
    # Next day: still inside the 7-day re-evaluate TTL -> skip.
    soon = select_batch(state, [cand(1)], now=NOW + timedelta(days=1), config=CFG)
    assert soon["to_enforce"] == []
    # After the TTL: eligible again (data/plans drift, so we re-check).
    later = select_batch(state, [cand(1)], now=NOW + timedelta(days=8), config=CFG)
    assert [c["query_id"] for c in later["to_enforce"]] == [1]


def test_record_sets_verify_timer_and_increments_attempts():
    state = record_outcomes(
        empty_state(), [control_transition()], now=NOW, config=CFG
    )
    entry = state["queries"][coverage_key(ENV, 1)]
    assert entry["verify_after"] == "2026-06-28T13:00:00Z"
    assert entry["attempts"] == 1
    # A second enforce attempt on the same query bumps the counter.
    state = record_outcomes(
        state, [control_transition()], now=NOW, config=CFG
    )
    assert state["queries"][coverage_key(ENV, 1)]["attempts"] == 2


def test_resolved_state_sets_reevaluate_ttl_and_clears_verify():
    state = record_outcomes(
        empty_state(), [control_transition()], now=NOW, config=CFG
    )
    state = record_outcomes(
        state, [{"environment": ENV, "query_id": 1, "state": "reverted"}], now=NOW, config=CFG
    )
    entry = state["queries"][coverage_key(ENV, 1)]
    assert entry["verify_after"] is None
    assert entry["reevaluate_after"] == "2026-07-05T12:00:00Z"


def test_unknown_state_rejected():
    try:
        record_outcomes(empty_state(), [{"query_id": 1, "state": "bogus"}], now=NOW)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_emitted_query_is_in_flight_and_not_re_enforced():
    state = record_outcomes(
        empty_state(),
        [control_transition(state="emitted", lever="set_hints")],
        now=NOW, config=CFG,
    )
    batch = select_batch(state, [cand(1, lever="set_hints")], now=NOW, config=CFG)
    assert batch["to_enforce"] == []
    assert batch["in_flight"] == [{"environment": ENV, "query_id": 1}]


def test_emitted_surfaces_in_due_confirm_only_after_window():
    state = record_outcomes(
        empty_state(),
        [control_transition(state="emitted", lever="set_hints")],
        now=NOW, config=CFG,
    )
    early = select_batch(state, [], now=NOW + timedelta(minutes=30), config=CFG)
    assert early["due_confirm"] == []
    later = select_batch(state, [], now=NOW + timedelta(minutes=90), config=CFG)
    assert [q["query_id"] for q in later["due_confirm"]] == [1]
    # emitted never lands in due_verify — it is a confirmation, not a metrics judgement
    assert later["due_verify"] == []


def test_emitted_sets_verify_timer_and_attempts():
    state = record_outcomes(
        empty_state(), [control_transition(state="emitted", lever="set_hints")],
        now=NOW, config=CFG,
    )
    entry = state["queries"][coverage_key(ENV, 1)]
    assert entry["verify_after"] == "2026-06-28T13:00:00Z"
    assert entry["attempts"] == 1


def test_emitted_promotes_to_pending_verify_on_confirmation():
    state = record_outcomes(
        empty_state(), [control_transition(state="emitted", lever="set_hints")],
        now=NOW, config=CFG,
    )
    state = record_outcomes(
        state, [{"environment": ENV, "query_id": 1, "state": "pending_verify"}],
        now=NOW + timedelta(hours=2), config=CFG,
    )
    entry = state["queries"][coverage_key(ENV, 1)]
    assert entry["state"] == "pending_verify"
    assert entry["verify_after"] == "2026-06-28T15:00:00Z"


def test_redeploy_verify_is_in_flight_with_timer_and_due_list():
    state = record_outcomes(
        empty_state(),
        [{"environment": ENV, "query_id": 1, "state": "handed_off",
          "notes": "pack 20260702T120000Z_ab12cd__q1 shipped"}],
        now=NOW, config=CFG,
    )
    state = record_outcomes(
        state,
        [{"environment": ENV, "query_id": 1, "state": "redeploy_verify",
          "baseline_metrics": {"avg_duration": 250000, "count_executions": 500},
          "notes": "pack 20260702T120000Z_ab12cd__q1 shipped"}],
        now=NOW, config=CFG,
    )
    entry = state["queries"][coverage_key(ENV, 1)]
    assert entry["verify_after"] == "2026-06-28T13:00:00Z"

    batch = select_batch(state, [cand(1)], now=NOW, config=CFG)
    assert batch["to_enforce"] == []          # no stacking a control on a shipped rewrite
    assert batch["in_flight"] == [{"environment": ENV, "query_id": 1}]

    later = select_batch(state, [], now=NOW + timedelta(minutes=90), config=CFG)
    assert [q["query_id"] for q in later["due_redeploy"]] == [1]
    assert later["due_verify"] == [] and later["due_confirm"] == []


def test_handed_off_to_redeploy_verify_to_kept_sequence():
    state = record_outcomes(
        empty_state(), [{"environment": ENV, "query_id": 1, "state": "handed_off", "notes": "pack x"}],
        now=NOW, config=CFG,
    )
    assert state["queries"][coverage_key(ENV, 1)]["reevaluate_after"] is not None  # resolved: TTL set

    state = record_outcomes(
        state, [{"environment": ENV, "query_id": 1, "state": "redeploy_verify",
                 "baseline_metrics": {"avg_duration": 1000, "count_executions": 50}}],
        now=NOW + timedelta(days=2), config=CFG,
    )
    assert state["queries"][coverage_key(ENV, 1)]["verify_after"] is not None

    state = record_outcomes(
        state, [{"environment": ENV, "query_id": 1, "state": "kept", "notes": "rewrite verified"}],
        now=NOW + timedelta(days=2, hours=2), config=CFG,
    )
    entry = state["queries"][coverage_key(ENV, 1)]
    assert entry["state"] == "kept"
    assert entry["verify_after"] is None
    assert entry["reevaluate_after"] is not None


def test_coverage_counter_advances_across_ticks():
    state = empty_state()
    state = record_outcomes(state, [{"environment": ENV, "query_id": 1, "state": "evaluated"}], now=NOW, config=CFG)
    state = record_outcomes(state, [control_transition()], now=NOW, config=CFG)
    state = record_outcomes(state, [{"environment": ENV, "query_id": 1, "state": "kept"}], now=NOW, config=CFG)
    state = record_outcomes(state, [{"environment": ENV, "query_id": 2, "state": "evaluated"}], now=NOW, config=CFG)
    assert status(state)["evaluated_count"] == 2
    assert status(state)["by_state"] == {"kept": 1, "evaluated": 1}


def test_state_round_trips_to_disk_for_resume(tmp_path):
    path = tmp_path / "state" / "coverage.json"
    state = record_outcomes(
        empty_state(),
        [control_transition(42)],
        now=NOW, config=CFG,
    )
    save_state(state, path)

    # A fresh "session" reloads and sees the in-flight control still tracked.
    reloaded = load_state(path)
    assert reloaded["queries"][coverage_key(ENV, 42)]["state"] == "pending_verify"
    assert "unforce_plan" in reloaded["queries"][coverage_key(ENV, 42)]["rollback_sql"]


def test_load_missing_state_is_empty():
    assert load_state("/nonexistent/path/coverage.json") == empty_state()


def test_state_path_resolution(tmp_path, monkeypatch):
    import pathlib

    import coverage_state as cs

    monkeypatch.setattr(pathlib.Path, "home", lambda: tmp_path)
    monkeypatch.delenv("SQL_PLAN_ENFORCER_STATE", raising=False)
    neutral = tmp_path / ".sql-skills" / "sql_plan_enforcer" / "state" / "coverage.json"
    assert cs.state_path() == neutral

    legacy = tmp_path / ".copilot" / "skills" / "sql_plan_enforcer" / "state" / "coverage.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("{}", encoding="utf-8")
    assert cs.state_path() == legacy

    monkeypatch.setenv("SQL_PLAN_ENFORCER_STATE", str(tmp_path / "override.json"))
    assert cs.state_path() == tmp_path / "override.json"


def test_query_id_is_scoped_by_environment():
    state = record_outcomes(
        empty_state(), [{"environment": "awlt_prod", "query_id": 42, "state": "evaluated"}], now=NOW
    )
    state = record_outcomes(
        state, [{"environment": "awlt_dev", "query_id": 42, "state": "evaluated"}], now=NOW
    )
    assert set(state["queries"]) == {
        coverage_key("awlt_prod", 42), coverage_key("awlt_dev", 42)
    }


def test_environment_identity_is_case_insensitive_and_query_id_is_positive():
    state = record_outcomes(
        empty_state(),
        [control_transition(42, environment="AWLT_PROD")],
        now=NOW,
    )
    batch = select_batch(state, [cand(42)], now=NOW, config=CFG)
    assert batch["to_enforce"] == []
    assert batch["in_flight"] == [{"environment": "awlt_prod", "query_id": 42}]
    with pytest.raises(ValueError, match="query_id"):
        record_outcomes(
            empty_state(),
            [{"environment": ENV, "query_id": 0, "state": "evaluated"}],
            now=NOW,
        )


def test_nested_list_truncation_is_rejected():
    candidate = cand(42)
    candidate["evidence"] = {"pages": [{"truncated": True}]}
    batch = select_batch(empty_state(), [candidate], now=NOW, config=CFG)
    assert batch["to_enforce"] == []
    assert "truncated" in batch["rejected"][0]["reason"]


def test_string_eligible_and_missing_plan_identity_are_rejected():
    string_flag = cand(42)
    string_flag["eligible"] = "true"
    missing_plan = cand(43)
    missing_plan.pop("proposed_plan_id")

    batch = select_batch(empty_state(), [string_flag, missing_plan], now=NOW, config=CFG)

    assert batch["to_enforce"] == []
    assert {item["query_id"] for item in batch["rejected"]} == {42, 43}


def test_invalid_loop_config_fails_closed():
    with pytest.raises(ValueError, match="verify_wait_minutes"):
        select_batch(
            empty_state(),
            [cand(1)],
            now=NOW,
            config=LoopConfig(2, 0, 7),
        )


def test_active_control_requires_exact_rollback_and_finite_baseline():
    unsafe = control_transition()
    unsafe["rollback_sql"] = "DROP TABLE dbo.Users"
    with pytest.raises(StateCorruptError, match="rollback_sql"):
        record_outcomes(empty_state(), [unsafe], now=NOW, config=CFG)

    nonfinite = control_transition()
    nonfinite["baseline_metrics"]["avg_duration"] = float("nan")
    with pytest.raises(StateCorruptError, match="baseline_metrics"):
        record_outcomes(empty_state(), [nonfinite], now=NOW, config=CFG)


def test_corrupt_cursor_or_entry_metadata_is_rejected():
    state = record_outcomes(
        empty_state(),
        [{"environment": ENV, "query_id": 42, "state": "evaluated"}],
        now=NOW,
    )
    bad_cursor = json.loads(json.dumps(state))
    bad_cursor["cursor"]["evaluated_count"] = -1
    with pytest.raises(StateCorruptError, match="evaluated_count"):
        select_batch(bad_cursor, [], now=NOW, config=CFG)

    bad_entry = json.loads(json.dumps(state))
    bad_entry["queries"][coverage_key(ENV, 42)]["plan_id"] = True
    with pytest.raises(StateCorruptError, match="plan_id"):
        select_batch(bad_entry, [], now=NOW, config=CFG)


def test_v1_unscoped_entries_are_quarantined_and_do_not_suppress_new_environment(tmp_path):
    path = tmp_path / "coverage.json"
    path.write_text(json.dumps({
        "version": 1,
        "queries": {"42": {"query_id": 42, "state": "kept"}},
        "cursor": {"evaluated_count": 1, "updated_at": None},
    }), encoding="utf-8")
    migrated = load_state(path)
    legacy = migrated["queries"]["legacy-v1::42"]
    assert migrated["version"] == 2
    assert legacy["environment"] is None
    assert legacy["legacy_v1_unscoped"] is True

    batch = select_batch(migrated, [cand(42)], now=NOW, config=CFG)
    assert [c["query_id"] for c in batch["to_enforce"]] == [42]


def test_v1_duplicate_unscoped_identity_fails_closed(tmp_path):
    path = tmp_path / "coverage.json"
    path.write_text(json.dumps({
        "version": 1,
        "queries": {
            "first": {"query_id": 42, "state": "kept"},
            "second": {"query_id": 42, "state": "reverted"},
        },
    }), encoding="utf-8")
    with pytest.raises(StateCorruptError, match="ambiguous duplicate"):
        load_state(path)


def test_missing_environment_transition_is_rejected():
    with pytest.raises(ValueError, match="environment"):
        record_outcomes(empty_state(), [{"query_id": 42, "state": "evaluated"}], now=NOW)


def test_illegal_lifecycle_transition_is_rejected():
    state = record_outcomes(
        empty_state(), [control_transition(42, state="emitted", lever="set_hints")],
        now=NOW,
    )
    with pytest.raises(ValueError, match="illegal coverage transition"):
        record_outcomes(
            state, [{"environment": ENV, "query_id": 42, "state": "kept"}], now=NOW
        )


@pytest.mark.parametrize("terminal_state", ["evaluated", "skipped"])
def test_active_control_cannot_be_forgotten_without_keep_or_revert(terminal_state):
    state = record_outcomes(
        empty_state(), [control_transition(42)], now=NOW, config=CFG,
    )
    with pytest.raises(ValueError, match="illegal coverage transition"):
        record_outcomes(
            state,
            [{"environment": ENV, "query_id": 42, "state": terminal_state}],
            now=NOW,
            config=CFG,
        )


def test_state_directory_file_and_lock_are_owner_only(tmp_path):
    path = tmp_path / "nested" / "state" / "coverage.json"
    state = record_outcomes(
        empty_state(), [{"environment": ENV, "query_id": 42, "state": "evaluated"}], now=NOW
    )
    save_state(state, path)
    assert stat.S_IMODE(path.parent.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE((path.parent / ".lock").stat().st_mode) == 0o600
