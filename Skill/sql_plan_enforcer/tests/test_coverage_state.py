"""Coverage state is what makes the loop resumable and progressive: it must not stack
controls on in-flight queries, must respect the per-tick blast radius and the cooldown TTL,
must surface in-flight changes once their verify window elapses, and must round-trip to disk
so a new session resumes where the last left off."""

from datetime import datetime, timedelta

from coverage_state import (
    LoopConfig,
    empty_state,
    load_state,
    record_outcomes,
    save_state,
    select_batch,
    status,
)

NOW = datetime(2026, 6, 28, 12, 0, 0)
CFG = LoopConfig(max_enforce_per_tick=2, verify_wait_minutes=60, reevaluate_ttl_days=7)


def cand(query_id, lever="force_plan", eligible=True):
    return {"query_id": query_id, "proposed_lever": lever, "eligible": eligible,
            "plan_id": 7, "category": "regression"}


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
        [{"query_id": 1, "state": "pending_verify", "lever": "force_plan",
          "rollback_sql": "EXEC sys.sp_query_store_unforce_plan @query_id = 1;"}],
        now=NOW, config=CFG,
    )
    batch = select_batch(state, [cand(1)], now=NOW, config=CFG)
    assert batch["to_enforce"] == []
    assert batch["in_flight"] == [1]


def test_pending_verify_surfaces_only_after_window():
    state = record_outcomes(
        empty_state(),
        [{"query_id": 1, "state": "pending_verify", "rollback_sql": "X"}],
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
        empty_state(), [{"query_id": 1, "state": "kept"}], now=NOW, config=CFG
    )
    # Next day: still inside the 7-day re-evaluate TTL -> skip.
    soon = select_batch(state, [cand(1)], now=NOW + timedelta(days=1), config=CFG)
    assert soon["to_enforce"] == []
    # After the TTL: eligible again (data/plans drift, so we re-check).
    later = select_batch(state, [cand(1)], now=NOW + timedelta(days=8), config=CFG)
    assert [c["query_id"] for c in later["to_enforce"]] == [1]


def test_record_sets_verify_timer_and_increments_attempts():
    state = record_outcomes(
        empty_state(), [{"query_id": 1, "state": "pending_verify"}], now=NOW, config=CFG
    )
    entry = state["queries"]["1"]
    assert entry["verify_after"] == "2026-06-28T13:00:00Z"
    assert entry["attempts"] == 1
    # A second enforce attempt on the same query bumps the counter.
    state = record_outcomes(
        state, [{"query_id": 1, "state": "pending_verify"}], now=NOW, config=CFG
    )
    assert state["queries"]["1"]["attempts"] == 2


def test_resolved_state_sets_reevaluate_ttl_and_clears_verify():
    state = record_outcomes(
        empty_state(), [{"query_id": 1, "state": "reverted"}], now=NOW, config=CFG
    )
    entry = state["queries"]["1"]
    assert entry["verify_after"] is None
    assert entry["reevaluate_after"] == "2026-07-05T12:00:00Z"


def test_unknown_state_rejected():
    try:
        record_outcomes(empty_state(), [{"query_id": 1, "state": "bogus"}], now=NOW)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_coverage_counter_advances_across_ticks():
    state = empty_state()
    state = record_outcomes(state, [{"query_id": 1, "state": "kept"}], now=NOW, config=CFG)
    state = record_outcomes(state, [{"query_id": 2, "state": "evaluated"}], now=NOW, config=CFG)
    assert status(state)["evaluated_count"] == 2
    assert status(state)["by_state"] == {"kept": 1, "evaluated": 1}


def test_state_round_trips_to_disk_for_resume(tmp_path):
    path = tmp_path / "state" / "coverage.json"
    state = record_outcomes(
        empty_state(),
        [{"query_id": 42, "state": "pending_verify",
          "rollback_sql": "EXEC sys.sp_query_store_unforce_plan @query_id = 42;"}],
        now=NOW, config=CFG,
    )
    save_state(state, path)

    # A fresh "session" reloads and sees the in-flight control still tracked.
    reloaded = load_state(path)
    assert reloaded["queries"]["42"]["state"] == "pending_verify"
    assert "unforce_plan" in reloaded["queries"]["42"]["rollback_sql"]


def test_load_missing_state_is_empty():
    assert load_state("/nonexistent/path/coverage.json") == empty_state()
