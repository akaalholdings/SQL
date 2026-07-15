"""The ledger is the undo button: every applied control must round-trip with a real
rollback, malformed rows must be rejected (fail-closed), and pending_rollbacks must
reconstruct exactly the controls still in place."""

import json
import stat

import pytest

import enforcement_ledger as led
from enforcement_ledger import pending_rollbacks, unresolved_prepared, validate, write_ledger

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


def test_unforce_requires_plan_id_and_hint_levers_reject_one():
    unforce = dict(
        FORCE_ACTION,
        lever="unforce_plan",
        plan_id=None,
        outcome="rolled_back",
        rollback_sql="",
    )
    assert any("unforce_plan requires" in error for error in validate(led.build_record(unforce)))

    hint = dict(
        FORCE_ACTION,
        lever="set_hints",
        plan_id=7,
        outcome="applied",
    )
    assert any("plan_id to be null" in error for error in validate(led.build_record(hint)))


def test_ids_and_mode_outcome_combinations_fail_closed():
    with pytest.raises(ValueError, match="positive integer"):
        led.build_record(dict(FORCE_ACTION, query_id=0))
    errors = validate(led.build_record(dict(FORCE_ACTION, plan_id=True)))
    assert any("plan_id" in error for error in errors)
    errors = validate(led.build_record(dict(FORCE_ACTION, mode="dry_run")))
    assert any("requires apply mode" in error for error in errors)


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


def test_pending_rejects_valid_json_with_invalid_record():
    bad = led.build_record(FORCE_ACTION)
    bad["query_id"] = 0
    with pytest.raises(ValueError, match="invalid"):
        pending_rollbacks([bad])


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
        "action_sql": "EXEC sys.sp_query_store_set_hints @query_id = 42, @query_hints = N'OPTION(RECOMPILE)';",
        "rollback_sql": "EXEC sys.sp_query_store_clear_hints @query_id = 42;",
        "baseline_metrics": {"avg_duration": 8000, "count_executions": 80},
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


HINT_EMITTED = {
    "environment": "mid_prod",
    "query_id": 77,
    "category": "param_sensitive",
    "lever": "set_hints",
    "plan_id": None,
    "action_sql": "EXEC sys.sp_query_store_set_hints @query_id = 77, @query_hints = N'OPTION(RECOMPILE)';",
    "rollback_sql": "EXEC sys.sp_query_store_clear_hints @query_id = 77;",
    "baseline_metrics": {"avg_duration": 9000, "count_executions": 120},
    "mode": "apply",
    "outcome": "emitted",
    "reason": "no execution channel for hints; script handed to operator",
}


def test_emitted_is_a_valid_outcome():
    assert validate(led.build_record(HINT_EMITTED)) == []


def test_prepared_row_is_durable_before_apply_but_not_active():
    prepared = led.build_record(dict(FORCE_ACTION, outcome="prepared"))
    assert validate(prepared) == []
    assert pending_rollbacks([prepared]) == []
    assert unresolved_prepared([prepared])[0]["query_id"] == 42


def test_prepared_requires_apply_mode_action_and_rollback():
    errors = validate(led.build_record(dict(
        FORCE_ACTION,
        outcome="prepared",
        mode="dry_run",
        action_sql="",
        rollback_sql="",
    )))
    assert any("requires apply mode" in error for error in errors)
    assert any("action_sql" in error for error in errors)
    assert any("rollback_sql" in error for error in errors)


def test_confirmed_outcome_resolves_prepared_uncertainty():
    prepared = led.build_record(dict(FORCE_ACTION, outcome="prepared"))
    applied = led.build_record(FORCE_ACTION)
    assert unresolved_prepared([prepared, applied]) == []


def test_unrelated_or_duplicate_rows_do_not_hide_prepared_uncertainty():
    first = led.build_record(dict(FORCE_ACTION, outcome="prepared"), nonce="first")
    second = led.build_record(dict(FORCE_ACTION, outcome="prepared"), nonce="second")
    unrelated = led.build_record(dict(
        FORCE_ACTION,
        outcome="skipped",
        plan_id=8,
        action_sql=(
            "EXEC sys.sp_query_store_force_plan @query_id = 42, @plan_id = 8;"
        ),
        rollback_sql=(
            "EXEC sys.sp_query_store_unforce_plan @query_id = 42, @plan_id = 8;"
        ),
    ))
    applied = led.build_record(FORCE_ACTION)

    assert len(unresolved_prepared([first, second, unrelated])) == 2
    remaining = unresolved_prepared([first, second, unrelated, applied])
    assert [item["prepared_record_id"] for item in remaining] == [first["id"]]


def test_prepared_can_be_resolved_by_its_exact_rollback_action():
    prepared = led.build_record(dict(FORCE_ACTION, outcome="prepared"))
    rolled_back = led.build_record({
        "environment": "mid_prod",
        "query_id": 42,
        "category": "regression",
        "lever": "unforce_plan",
        "plan_id": 7,
        "action_sql": (
            "EXEC sys.sp_query_store_unforce_plan @query_id = 42, @plan_id = 7;"
        ),
        "rollback_sql": "",
        "mode": "apply",
        "outcome": "rolled_back",
        "reason": "apply outcome was uncertain; rollback confirmed",
    })
    assert unresolved_prepared([prepared, rolled_back]) == []


def test_pending_cli_stops_on_unresolved_prepared(tmp_path, monkeypatch, capsys):
    write_ledger(dict(FORCE_ACTION, outcome="prepared"), root=tmp_path)
    monkeypatch.setenv("SQL_PLAN_ENFORCER_AUDIT_DIR", str(tmp_path))

    assert led.main(["enforcement_ledger.py", "--pending"]) == 3
    output = capsys.readouterr().out
    assert "UNRESOLVED PREPARED ACTION" in output
    assert "Verify read-only before any new apply" in output


def test_loaded_detail_path_cannot_escape_runs_directory():
    record = led.build_record(FORCE_ACTION)
    record["detail_file"] = "../outside.md"
    assert any("detail_file" in error for error in validate(record))


def test_baseline_timestamp_and_detail_identity_are_strict():
    record = led.build_record(FORCE_ACTION)
    record["baseline_metrics"] = {"avg_duration": float("nan")}
    record["timestamp"] = "yesterday"
    record["detail_file"] = "runs/a-different-record.md"
    errors = validate(record)
    assert any("finite" in error for error in errors)
    assert any("timestamp" in error for error in errors)
    assert any("match the ledger record id" in error for error in errors)

    no_baseline = led.build_record(dict(FORCE_ACTION, baseline_metrics={}))
    assert any("baseline_metrics" in error for error in validate(no_baseline))


def test_emitted_without_rollback_is_invalid():
    errors = validate(led.build_record(dict(HINT_EMITTED, rollback_sql="")))
    assert any("rollback_sql" in e for e in errors)


def test_emitted_without_action_sql_is_invalid():
    errors = validate(led.build_record(dict(HINT_EMITTED, action_sql="")))
    assert any("action_sql" in e for e in errors)


def test_dry_run_requires_action_and_rollback_scripts():
    errors = validate(led.build_record(dict(
        FORCE_ACTION,
        mode="dry_run",
        outcome="dry_run",
        action_sql="",
        rollback_sql="",
    )))
    assert any("action_sql" in error for error in errors)
    assert any("rollback_sql" in error for error in errors)


def test_control_sql_must_match_the_recorded_target_exactly():
    force = led.build_record(dict(
        FORCE_ACTION,
        action_sql="EXEC sys.sp_query_store_force_plan @query_id = 99, @plan_id = 7;",
        rollback_sql="DROP TABLE dbo.Users;",
    ))
    errors = validate(force)
    assert any("action_sql" in error for error in errors)
    assert any("rollback_sql" in error for error in errors)

    hint = led.build_record(dict(
        HINT_EMITTED,
        action_sql=(
            "EXEC sys.sp_query_store_set_hints @query_id = 77, "
            "@query_hints = N'OPTION(RECOMPILE)'; DROP TABLE dbo.Users;"
        ),
    ))
    assert any("single set_query_store_hints" in error for error in validate(hint))


def test_emitted_alone_activates_no_control():
    # The script is with the human; until a confirmed "applied" row lands,
    # --pending must not claim there is anything to roll back.
    assert pending_rollbacks([led.build_record(HINT_EMITTED)]) == []


def test_emitted_then_applied_then_rolled_back_nets_zero():
    emitted = led.build_record(HINT_EMITTED)
    applied = led.build_record(dict(HINT_EMITTED, outcome="applied",
                                    reason="human-executed script confirmed"))
    assert len(pending_rollbacks([emitted, applied])) == 1

    cleared = led.build_record({
        "environment": "mid_prod",
        "query_id": 77,
        "category": "param_sensitive",
        "lever": "clear_hints",
        "plan_id": None,
        "action_sql": "EXEC sys.sp_query_store_clear_hints @query_id = 77;",
        "rollback_sql": "",
        "mode": "apply",
        "outcome": "rolled_back",
        "reason": "no improvement",
    })
    assert pending_rollbacks([emitted, applied, cleared]) == []


def test_ledger_dir_resolution(tmp_path, monkeypatch):
    import pathlib
    monkeypatch.setattr(pathlib.Path, "home", lambda: tmp_path)
    monkeypatch.delenv("SQL_PLAN_ENFORCER_AUDIT_DIR", raising=False)
    # no legacy dir -> host-neutral default
    assert led.ledger_dir() == tmp_path / ".sql-skills" / "sql_plan_enforcer" / "audits"
    # legacy dir present -> keep using it (established ledger survives a host switch)
    legacy = tmp_path / ".copilot" / "skills" / "sql_plan_enforcer" / "audits"
    legacy.mkdir(parents=True)
    assert led.ledger_dir() == legacy
    # env override always wins
    monkeypatch.setenv("SQL_PLAN_ENFORCER_AUDIT_DIR", str(tmp_path / "override"))
    assert led.ledger_dir() == tmp_path / "override"


def test_cli_uses_env_ledger_dir(tmp_path, monkeypatch):
    ledger = tmp_path / "corpus"
    monkeypatch.setenv("SQL_PLAN_ENFORCER_AUDIT_DIR", str(ledger))
    action_file = tmp_path / "action.json"
    action_file.write_text(json.dumps(FORCE_ACTION), encoding="utf-8")

    rc = led.main(["enforcement_ledger.py", "--input", str(action_file)])
    assert rc == 0
    assert len(_index_lines(ledger)) == 1


def test_ledger_storage_is_owner_only(tmp_path):
    record = write_ledger(FORCE_ACTION, root=tmp_path)
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "runs").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "index.jsonl").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / record["detail_file"]).stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / ".gitignore").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "README.md").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / ".lock").stat().st_mode) == 0o600


def test_ledger_rejects_missing_environment(tmp_path):
    with pytest.raises(ValueError, match="environment"):
        write_ledger(dict(FORCE_ACTION, environment=""), root=tmp_path)


def test_index_read_rejects_semantically_invalid_rows(tmp_path):
    bad = led.build_record(FORCE_ACTION)
    bad["query_id"] = 0
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "index.jsonl").write_text(json.dumps(bad) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid at line 1"):
        led._load_index(tmp_path)


def test_storage_rejects_symlinked_parent_component(tmp_path):
    from enforcer_storage import secure_dir

    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "linked"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(OSError, match="symlinked"):
        secure_dir(link / "state")
