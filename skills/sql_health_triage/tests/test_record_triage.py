"""The triage log is best-effort (a failed write must never fail the triage) but still
validated: bad outcomes are rejected, the corpus scaffolds itself on first write, and the
kill switch env var disables it cleanly."""

import json
from concurrent.futures import ThreadPoolExecutor
import stat

import pytest

import record_triage as rt
from record_triage import build_record, validate, write_triage

SESSION = {
    "environment": "awlt_prod",
    "mode": "triage",
    "symptom": "everything is slow",
    "outcome": "handed_off_optimizer",
    "findings": [
        {"domain": "resource", "metric": "avg_cpu_percent", "value": 94.0,
         "threshold": 100.0, "severity": "critical", "query_id": 42,
         "summary": "CPU at 94% of ceiling",
         "recommended_action": "optimize query 42", "owner": "sql-optimizer",
         "evidence": {"tool": "get_resource_stats_history", "truncated": False}},
    ],
    "handoff_pack_ids": ["20260702T120000Z_ab12cd__q42"],
    "notes": "top consumer accounted for 68% of CPU",
}


def test_write_creates_index_detail_and_scaffolding(tmp_path):
    record = write_triage(SESSION, root=tmp_path)
    assert record is not None
    assert validate(record) == []

    lines = (tmp_path / "index.jsonl").read_text("utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["outcome"] == "handed_off_optimizer"

    detail = (tmp_path / record["detail_file"]).read_text("utf-8")
    assert "everything is slow" in detail
    assert "20260702T120000Z_ab12cd__q42" in detail
    assert (tmp_path / ".gitignore").exists()
    assert (tmp_path / "README.md").exists()


def test_audit_tree_and_files_use_restrictive_permissions(tmp_path):
    record = write_triage(SESSION, root=tmp_path)

    assert stat.S_IMODE((tmp_path).stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "runs").stat().st_mode) == 0o700
    for path in (
        tmp_path / ".lock",
        tmp_path / ".gitignore",
        tmp_path / "README.md",
        tmp_path / "index.jsonl",
        tmp_path / record["detail_file"],
    ):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600, path


def test_storage_rejects_symlinked_parent_component(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "linked"
    link.symlink_to(real, target_is_directory=True)

    with pytest.raises(OSError, match="symlinked"):
        write_triage(SESSION, root=link / "audits")


def test_truncated_finding_is_sanitized_in_durable_detail(tmp_path):
    session = dict(
        SESSION,
        outcome="inconclusive",
        handoff_pack_ids=[],
        findings=[{
            "domain": "resource",
            "metric": "avg_cpu_percent",
            "value": 99.0,
            "threshold": 100.0,
            "severity": "critical",
            "owner": "sql-optimizer",
            "summary": "truncated CPU result",
            "recommended_action": "DO_NOT_RENDER_THIS_CORRECTIVE_ACTION",
            "evidence": {"tool": "get_resource_stats_history", "truncated": True},
        }],
    )
    record = write_triage(session, root=tmp_path)
    stored = record["findings"][0]
    detail = (tmp_path / record["detail_file"]).read_text("utf-8")

    assert stored["severity"] == "inconclusive"
    assert stored["owner"] is None
    assert stored["recommended_action"] is None
    assert "DO_NOT_RENDER_THIS_CORRECTIVE_ACTION" not in detail
    assert "narrow or re-query" in detail
    assert "No owner handoff" in detail


def test_truncated_only_session_cannot_claim_handoff_or_pack(tmp_path):
    session = dict(
        SESSION,
        outcome="handed_off_optimizer",
        handoff_pack_ids=["20260702T120000Z_ab12cd__q42"],
        findings=[{
            "domain": "resource",
            "metric": "avg_cpu_percent",
            "value": 99.0,
            "threshold": 100.0,
            "severity": "critical",
            "owner": "sql-optimizer",
            "summary": "truncated CPU result",
            "recommended_action": "optimize query 42",
            "evidence": {"tool": "get_resource_stats_history", "truncated": True},
        }],
    )

    record = write_triage(session, root=tmp_path)

    assert record["outcome"] == "inconclusive"
    assert record["handoff_pack_ids"] == []
    assert "Handoff packs" not in (tmp_path / record["detail_file"]).read_text("utf-8")


def test_nested_truncation_cannot_claim_handoff_or_pack(tmp_path):
    finding = dict(SESSION["findings"][0])
    finding["evidence"] = {
        "tool": "get_resource_stats_history",
        "page": {"truncated": "true"},
    }
    session = dict(
        SESSION,
        findings=[finding],
        handoff_pack_ids=["pack-1"],
    )

    record = write_triage(session, root=tmp_path)

    assert record["outcome"] == "inconclusive"
    assert record["handoff_pack_ids"] == []


def test_non_object_session_or_finding_is_rejected():
    with pytest.raises(ValueError, match="session must be"):
        build_record([])
    with pytest.raises(ValueError, match="list of objects"):
        build_record(dict(SESSION, findings=["bad"]))


def test_caller_cannot_spoof_actionable_severity_for_handoff():
    finding = dict(
        SESSION["findings"][0],
        value=1.0,
        threshold=100.0,
        severity="critical",
    )
    record = build_record(dict(
        SESSION,
        findings=[finding],
        outcome="handed_off_optimizer",
        handoff_pack_ids=["spoofed-pack"],
    ))
    assert record["findings"][0]["status"] == "observation"
    assert record["outcome"] == "inconclusive"
    assert record["handoff_pack_ids"] == []


def test_pack_ids_require_a_matching_optimizer_handoff_outcome():
    record = build_record(dict(
        SESSION,
        outcome="inconclusive",
        handoff_pack_ids=["pack-that-was-not-handed-off"],
    ))

    assert record["outcome"] == "inconclusive"
    assert record["handoff_pack_ids"] == []


def test_concurrent_writes_produce_complete_unique_index_rows(tmp_path):
    def write_one(index):
        return write_triage(dict(SESSION, environment=f"env-{index}"), root=tmp_path)

    with ThreadPoolExecutor(max_workers=12) as executor:
        records = list(executor.map(write_one, range(24)))

    lines = (tmp_path / "index.jsonl").read_text("utf-8").splitlines()
    indexed = [json.loads(line) for line in lines]
    assert len(indexed) == 24
    assert len({record["id"] for record in indexed}) == 24
    assert {record["id"] for record in indexed} == {record["id"] for record in records}
    for record in indexed:
        detail = tmp_path / record["detail_file"]
        assert detail.exists()
        detail_text = detail.read_text("utf-8")
        assert detail_text.startswith("---\n")
        assert f"Triage session `{record['id']}`" in detail_text


def test_invalid_outcome_rejected():
    with pytest.raises(ValueError, match="outcome"):
        build_record(dict(SESSION, outcome="fixed_everything"))


def test_disabled_via_env_records_nothing(tmp_path, monkeypatch):
    monkeypatch.setenv("SQL_HEALTH_TRIAGE_AUDIT", "0")
    assert write_triage(SESSION, root=tmp_path) is None
    assert not (tmp_path / "index.jsonl").exists()


def test_cli_failure_is_nonzero_but_only_one_line(tmp_path, monkeypatch, capsys):
    # Point the log at an unwritable location: the CLI must return non-zero with a
    # single stderr line — the caller surfaces it and moves on (best-effort contract).
    blocker = tmp_path / "not_a_dir"
    blocker.write_text("file, not dir", encoding="utf-8")
    monkeypatch.setenv("SQL_HEALTH_TRIAGE_AUDIT_DIR", str(blocker / "audits"))

    session_file = tmp_path / "session.json"
    session_file.write_text(json.dumps(SESSION), encoding="utf-8")
    rc = rt.main(["record_triage.py", "--input", str(session_file)])
    assert rc == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1
    assert "triage log write failed" in err[0]


def test_cli_env_dir_and_success_line(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SQL_HEALTH_TRIAGE_AUDIT_DIR", str(tmp_path / "audits"))
    session_file = tmp_path / "session.json"
    session_file.write_text(json.dumps(SESSION), encoding="utf-8")

    rc = rt.main(["record_triage.py", "--input", str(session_file)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "handed_off_optimizer" in out
    assert (tmp_path / "audits" / "index.jsonl").exists()


def test_audit_dir_resolution(tmp_path, monkeypatch):
    import pathlib

    monkeypatch.setattr(pathlib.Path, "home", lambda: tmp_path)
    monkeypatch.delenv("SQL_HEALTH_TRIAGE_AUDIT_DIR", raising=False)
    assert rt.audit_dir() == tmp_path / ".sql-skills" / "sql_health_triage" / "audits"

    legacy = tmp_path / ".copilot" / "skills" / "sql_health_triage" / "audits"
    legacy.mkdir(parents=True)
    assert rt.audit_dir() == legacy

    monkeypatch.setenv("SQL_HEALTH_TRIAGE_AUDIT_DIR", str(tmp_path / "override"))
    assert rt.audit_dir() == tmp_path / "override"
