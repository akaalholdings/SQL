"""The ledger is the undo button: every applied control must round-trip with a real
rollback, malformed rows must be rejected (fail-closed), and pending_rollbacks must
reconstruct exactly the controls still in place."""

import json

import pytest

import enforcement_ledger as led
from enforcement_ledger import pending_rollbacks, validate, write_ledger

FORCE_ACTION = {
    "environment": "mid_prod",
    "query_id": 42,
    "category": "regression",
    "lever": "force_plan",
    "plan_id": 7,
    "action_sql": "EXEC sys.sp_query_store_force_plan @query_id = 42, @plan_id = 7;",
    "rollback_sql": "EXEC sys.sp_query_store_unforce_plan @query_id = 42, @plan_id = 7;",
    "baseline_metrics": {"avg_duration": 5000, "count_executions": 500},
    "mode": "apply",
    "outcome": "applied",
    "reason": "regressed 220% vs plan 7",
}


def _index_lines(root):
    return (root / "index.jsonl").read_text(encoding="utf-8").splitlines()


def test_force_action_round_trips_with_rollback(tmp_path):
    record = write_ledger(FORCE_ACTION, root=tmp_path)

    lines = _index_lines(tmp_path)
    assert len(lines) == 1
    persisted = json.loads(lines[0])
    assert validate(persisted) == []
    assert persisted["query_id"] == 42
    assert persisted["plan_id"] == 7
    assert "sp_query_store_unforce_plan" in persisted["rollback_sql"]

    detail = tmp_path / record["detail_file"]
    assert detail.exists()
    assert "sp_query_store_force_plan" in detail.read_text(encoding="utf-8")


def test_applied_control_without_rollback_is_invalid():
    bad = dict(FORCE_ACTION, rollback_sql="")
    errors = validate(led.build_record(bad))
    assert any("rollback_sql" in e for e in errors)


def test_force_plan_requires_plan_id():
    bad = dict(FORCE_ACTION, plan_id=None)
    errors = validate(led.build_record(bad))
    assert any("plan_id" in e for e in errors)


def test_write_ledger_raises_on_invalid(tmp_path):
    bad = dict(FORCE_ACTION, lever="force_plan", rollback_sql="")
    with pytest.raises(ValueError):
        write_ledger(bad, root=tmp_path)


def test_bad_lever_rejected():
    errors = validate(led.build_record(dict(FORCE_ACTION, lever="force_plan")))
    assert errors == []  # control: valid one passes
    with pytest.raises(ValueError):
        led.build_record(dict(FORCE_ACTION, lever="drop_table"))


def test_corpus_scaffolding_created(tmp_path):
    write_ledger(FORCE_ACTION, root=tmp_path)
    assert (tmp_path / "runs").is_dir()
    assert (tmp_path / ".gitignore").exists()
    assert (tmp_path / "README.md").exists()


def test_pending_rollbacks_reports_active_control():
    records = [led.build_record(FORCE_ACTION)]
    pending = pending_rollbacks(records)
    assert len(pending) == 1
    assert pending[0]["query_id"] == 42
    assert "unforce_plan" in pending[0]["rollback_sql"]


def test_rollback_action_clears_the_active_control():
    forced = led.build_record(FORCE_ACTION)
    unforced = led.build_record({
        "environment": "mid_prod",
        "query_id": 42,
        "category": "regression",
        "lever": "unforce_plan",
        "plan_id": 7,
        "action_sql": "EXEC sys.sp_query_store_unforce_plan @query_id = 42, @plan_id = 7;",
        "rollback_sql": "",
        "mode": "apply",
        "outcome": "rolled_back",
        "reason": "no improvement after force",
    })
    assert pending_rollbacks([forced, unforced]) == []


def test_force_and_hint_are_independent_families():
    forced = led.build_record(FORCE_ACTION)
    hinted = led.build_record({
        "environment": "mid_prod",
        "query_id": 42,
        "category": "param_sensitive",
        "lever": "set_hints",
        "plan_id": None,
        "action_sql": "EXEC sys.sp_query_store_set_hints @query_id = 42, @value = N'OPTION(RECOMPILE)';",
        "rollback_sql": "EXEC sys.sp_query_store_clear_hints @query_id = 42;",
        "mode": "apply",
        "outcome": "applied",
        "reason": "param sensitive",
    })
    pending = pending_rollbacks([forced, hinted])
    levers = {p["lever"] for p in pending}
    assert levers == {"force_plan", "set_hints"}


def test_dry_run_action_leaves_nothing_to_roll_back():
    dry = led.build_record(dict(FORCE_ACTION, mode="dry_run", outcome="dry_run"))
    assert pending_rollbacks([dry]) == []


def test_cli_uses_env_ledger_dir(tmp_path, monkeypatch):
    ledger = tmp_path / "corpus"
    monkeypatch.setenv("SQL_PLAN_ENFORCER_AUDIT_DIR", str(ledger))
    action_file = tmp_path / "action.json"
    action_file.write_text(json.dumps(FORCE_ACTION), encoding="utf-8")

    rc = led.main(["enforcement_ledger.py", "--input", str(action_file)])
    assert rc == 0
    assert len(_index_lines(ledger)) == 1
