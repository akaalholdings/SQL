"""The handoff queue is the work-item contract between the enforcer and sql_optimizer:
packs must be fail-closed on enqueue (a lost pack is a lost work item), transitions must
follow the lifecycle (open -> claimed -> shipped/declined, reopen on regression), and one
query must never accumulate two open packs."""

import json
import stat

import pytest

import handoff_queue as hq
from handoff_queue import add, apply_transition, build_pack, load_packs, transition, validate

CANDIDATE = {
    "query_id": 42,
    "category": "top_consumer",
    "reason": "top aggregate resource consumer",
    "count_executions": 500.0,
    "avg_duration": 250_000.0,
    "total_duration": 125_000_000.0,
    "current_plan_id": 9,
    "adapted_from": "get_top_queries",
    "eligible": True,
    "proposed_lever": "handoff_optimizer",
}


def pack(**overrides):
    built = build_pack(
        CANDIDATE,
        environment="awlt_prod",
        query_text="SELECT o.id FROM dbo.orders AS o WHERE o.status = @p1",
    )
    built.update(overrides)
    return built


def test_build_pack_roundtrips_valid():
    record = pack()
    assert validate(record) == []
    assert record["status"] == "open"
    assert record["query_id"] == 42
    assert record["evidence"]["metrics"]["count_executions"] == 500.0
    assert record["evidence"]["plan_ids"] == {"current": 9}
    assert "orders" in record["evidence"]["query_sql_text"]
    assert record["evidence"]["notes"] == "scanned via get_top_queries"


def test_build_pack_requires_integer_query_id():
    with pytest.raises(ValueError, match="query_id"):
        build_pack({"category": "top_consumer"}, environment="awlt_prod")


def test_build_pack_rejects_nested_truncated_evidence():
    candidate = dict(CANDIDATE, evidence={"pages": [{"truncated": True}]})
    with pytest.raises(ValueError, match="truncated"):
        build_pack(candidate, environment="awlt_prod")


def test_nonfinite_metrics_and_invalid_plan_ids_are_rejected():
    record = pack()
    record["evidence"]["metrics"]["avg_duration"] = float("nan")
    record["evidence"]["plan_ids"]["current"] = True
    errors = validate(record)
    assert any("finite number" in error for error in errors)
    assert any("positive integers" in error for error in errors)


def test_validate_rejects_bad_source_and_status():
    assert any("source" in e for e in validate(pack(source="somewhere_else")))
    assert any("status" in e for e in validate(pack(status="in_review")))


def test_shipped_without_resolution_is_invalid():
    record = pack(status="shipped")
    assert any("resolution" in e for e in validate(record))


def test_transitions_follow_lifecycle():
    record = pack()
    claimed = apply_transition(record, "claimed")
    assert claimed["status"] == "claimed"

    shipped = apply_transition(
        claimed, "shipped", resolution={"outcome": "shipped", "rewrite_shipped": True}
    )
    assert shipped["status"] == "shipped"
    assert shipped["resolution"]["rewrite_shipped"] is True
    assert shipped["resolution"]["timestamp"]

    # post-deploy regression: shipped packs reopen, cleanly
    reopened = apply_transition(shipped, "open")
    assert reopened["status"] == "open"
    assert "resolution" not in reopened


def test_illegal_transitions_raise():
    record = pack()
    with pytest.raises(ValueError, match="cannot move"):
        apply_transition(record, "shipped", resolution={"outcome": "shipped"})  # open -> shipped
    with pytest.raises(ValueError, match="cannot move"):
        apply_transition(
            pack(status="declined", resolution={"outcome": "declined"}),
            "claimed",
        )  # declined is terminal
    with pytest.raises(ValueError, match="resolution"):
        apply_transition(pack(status="claimed"), "declined")  # verdict needs a resolution


def test_add_persists_pack_and_event_log(tmp_path):
    record = add(pack(), root=tmp_path)
    on_disk = json.loads((tmp_path / "packs" / f"{record['id']}.json").read_text("utf-8"))
    assert on_disk["query_id"] == 42
    index_lines = (tmp_path / "index.jsonl").read_text("utf-8").splitlines()
    assert len(index_lines) == 1


def test_add_fails_closed_on_invalid(tmp_path):
    bad = pack()
    del bad["evidence"]
    with pytest.raises(ValueError, match="invalid"):
        add(bad, root=tmp_path)
    assert load_packs(tmp_path) == []


def test_corrupt_pack_is_rejected_on_read(tmp_path):
    packs_dir = tmp_path / "packs"
    packs_dir.mkdir()
    (packs_dir / "broken.json").write_text("{not-json", encoding="utf-8")
    with pytest.raises(ValueError, match="not valid JSON"):
        load_packs(tmp_path)


def test_add_dedupes_open_packs_per_environment_and_query(tmp_path):
    add(pack(), root=tmp_path)
    with pytest.raises(ValueError, match="already exists"):
        add(pack(id="different_id__q42"), root=tmp_path)
    # same query in a different environment is a separate work item
    other_env = pack(id="other_env__q42", environment="awlt_dev")
    add(other_env, root=tmp_path)
    assert len(load_packs(tmp_path)) == 2


def test_add_dedupes_environment_case_insensitively(tmp_path):
    add(pack(environment="AWLT_PROD"), root=tmp_path)
    with pytest.raises(ValueError, match="already exists"):
        add(pack(id="same-db-different-case", environment="awlt_prod"), root=tmp_path)


def test_pack_id_cannot_escape_queue_directory(tmp_path):
    with pytest.raises(ValueError, match="plain file name"):
        add(pack(id="../outside"), root=tmp_path)
    with pytest.raises(ValueError, match="plain file name"):
        hq.find_pack("../outside", root=tmp_path)
    with pytest.raises(ValueError, match="plain file name"):
        add(pack(id="line\nbreak"), root=tmp_path)


def test_add_dedupes_claimed_packs_until_terminal_resolution(tmp_path):
    record = add(pack(), root=tmp_path)
    transition(record["id"], "claimed", root=tmp_path)
    with pytest.raises(ValueError, match="already exists"):
        add(pack(id="duplicate_claimed__q42"), root=tmp_path)


def test_queue_storage_is_owner_only(tmp_path):
    record = add(pack(), root=tmp_path)
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "packs").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "packs" / f"{record['id']}.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "index.jsonl").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / ".lock").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / ".gitignore").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "README.md").stat().st_mode) == 0o600
    assert "Never commit" in (tmp_path / ".gitignore").read_text("utf-8")


def test_full_lifecycle_on_disk_latest_wins(tmp_path):
    record = add(pack(), root=tmp_path)
    transition(record["id"], "claimed", root=tmp_path)
    final = transition(
        record["id"], "shipped",
        resolution={"outcome": "shipped", "rewrite_shipped": True, "optimizer_audit_id": "a1"},
        root=tmp_path,
    )
    assert final["status"] == "shipped"

    # packs/<id>.json holds the latest state; index.jsonl holds every event
    [reloaded] = load_packs(tmp_path)
    assert reloaded["status"] == "shipped"
    assert len((tmp_path / "index.jsonl").read_text("utf-8").splitlines()) == 3

    # once shipped resolves, a new open pack for the same query is allowed again
    add(pack(id="fresh__q42"), root=tmp_path)


def test_cli_lifecycle(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SQL_PLAN_ENFORCER_HANDOFF_DIR", str(tmp_path))
    pack_file = tmp_path / "pack.json"
    pack_file.write_text(json.dumps(pack()), encoding="utf-8")

    assert hq.main(["handoff_queue.py", "add", "--input", str(pack_file)]) == 0
    pack_id = capsys.readouterr().out.split()[1]

    assert hq.main(["handoff_queue.py", "claim", pack_id]) == 0
    capsys.readouterr()

    res_file = tmp_path / "res.json"
    res_file.write_text(json.dumps({"outcome": "declined", "notes": "already optimal"}), "utf-8")
    assert hq.main(["handoff_queue.py", "complete", pack_id, "--resolution", str(res_file)]) == 0
    capsys.readouterr()

    assert hq.main(["handoff_queue.py", "list", "--status", "declined"]) == 0
    listing = json.loads(capsys.readouterr().out)
    assert listing["count"] == 1
    assert listing["packs"][0]["resolution"]["notes"] == "already optimal"

    assert hq.main(["handoff_queue.py", "validate"]) == 0


def test_cli_invalid_input_is_rejected_without_traceback(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SQL_PLAN_ENFORCER_HANDOFF_DIR", str(tmp_path / "queue"))
    broken = tmp_path / "broken.json"
    broken.write_text("{not-json", encoding="utf-8")

    assert hq.main(["handoff_queue.py", "add", "--input", str(broken)]) == 2
    assert "handoff add rejected" in capsys.readouterr().err


def test_validate_command_detects_corrupt_event_index(tmp_path, monkeypatch, capsys):
    add(pack(), root=tmp_path)
    with (tmp_path / "index.jsonl").open("a", encoding="utf-8") as handle:
        handle.write("{broken\n")
    monkeypatch.setenv("SQL_PLAN_ENFORCER_HANDOFF_DIR", str(tmp_path))

    assert hq.main(["handoff_queue.py", "validate"]) == 1
    assert "invalid JSON" in capsys.readouterr().err


def test_resolution_outcome_must_match_terminal_status():
    claimed = apply_transition(pack(), "claimed")
    with pytest.raises(ValueError, match="matching resolution"):
        apply_transition(claimed, "shipped", resolution={"outcome": "declined"})


def test_queue_dir_resolution(tmp_path, monkeypatch):
    import pathlib
    monkeypatch.setattr(pathlib.Path, "home", lambda: tmp_path)
    monkeypatch.delenv("SQL_PLAN_ENFORCER_HANDOFF_DIR", raising=False)
    assert hq.queue_dir() == tmp_path / ".sql-skills" / "sql_plan_enforcer" / "handoffs"

    legacy = tmp_path / ".copilot" / "skills" / "sql_plan_enforcer" / "handoffs"
    legacy.mkdir(parents=True)
    assert hq.queue_dir() == legacy

    monkeypatch.setenv("SQL_PLAN_ENFORCER_HANDOFF_DIR", str(tmp_path / "override"))
    assert hq.queue_dir() == tmp_path / "override"


def test_cli_reopen_appends_note(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SQL_PLAN_ENFORCER_HANDOFF_DIR", str(tmp_path))
    record = add(pack(), root=tmp_path)
    transition(record["id"], "claimed", root=tmp_path)
    transition(record["id"], "shipped",
               resolution={"outcome": "shipped", "rewrite_shipped": True}, root=tmp_path)

    assert hq.main(["handoff_queue.py", "reopen", record["id"],
                    "--note", "post-deploy regression"]) == 0
    [reloaded] = load_packs(tmp_path)
    assert reloaded["status"] == "open"
    assert "post-deploy regression" in reloaded["reason"]
