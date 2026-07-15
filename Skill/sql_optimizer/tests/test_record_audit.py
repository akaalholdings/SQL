"""Writing an audit must append a valid index row and redact raw SQL by default."""

import json
from concurrent.futures import ThreadPoolExecutor
from stat import S_IMODE

import pytest

import record_audit
from validate_audit import validate

RAW_QUERY = "SELECT * FROM dbo.Orders WHERE YEAR(OrderDate) = 2023"

RUN = {
    "environment": "mid",
    "query": RAW_QUERY,
    "rewrite": "SELECT o.id FROM dbo.Orders AS o WHERE o.OrderDate >= '20230101'",
    "scripts": {"index": "CREATE INDEX ...", "rollback": "DROP INDEX ..."},
    "tables": ["dbo.Orders"],
    "anti_patterns": ["sargability", "select_star"],
    "rules_applied": ["rule1_sargability"],
    "index_changes": {"adds": 1, "drops": 0, "alters": 0},
    "metrics": {"baseline": {"duration_ms": 100}, "optimized": {"duration_ms": 40}},
    "improvement": {"duration_pct": 60},
    "equivalence_proven": True,
    "outcome": "improved",
    "guidance_gaps": ["StyleGuide silent on conditional aggregates"],
}


def _index_lines(root):
    return (root / "index.jsonl").read_text(encoding="utf-8").splitlines()


def test_write_audit_appends_valid_row_and_redacted_detail(tmp_path, monkeypatch):
    monkeypatch.delenv("SQL_OPTIMIZER_AUDIT_FULL_SQL", raising=False)

    record = record_audit.write_audit(RUN, root=tmp_path)

    lines = _index_lines(tmp_path)
    assert len(lines) == 1
    persisted = json.loads(lines[0])
    assert validate(persisted) == []
    assert persisted["id"] == record["id"]
    assert persisted["outcome"] == "improved"

    detail = tmp_path / record["detail_file"]
    assert detail.exists()
    detail_text = detail.read_text(encoding="utf-8")
    assert RAW_QUERY not in detail_text
    assert "SQL redacted by default" in detail_text


def test_write_audit_can_persist_raw_sql_when_explicitly_enabled(tmp_path, monkeypatch):
    monkeypatch.setenv("SQL_OPTIMIZER_AUDIT_FULL_SQL", "1")

    record = record_audit.write_audit(RUN, root=tmp_path)

    detail = tmp_path / record["detail_file"]
    assert RAW_QUERY in detail.read_text(encoding="utf-8")


def test_second_write_appends_without_overwrite(tmp_path):
    record_audit.write_audit(RUN, root=tmp_path)
    other = dict(RUN, query=RAW_QUERY + " AND Region = 'EU'")
    record_audit.write_audit(other, root=tmp_path)

    lines = _index_lines(tmp_path)
    assert len(lines) == 2
    ids = {json.loads(line)["id"] for line in lines}
    assert len(ids) == 2  # distinct queries -> distinct hashes -> distinct ids


def test_same_query_same_second_does_not_collide(tmp_path):
    # The same query recorded twice in quick succession must produce distinct ids
    # and distinct detail files (the nonce), not overwrite each other.
    r1 = record_audit.write_audit(RUN, root=tmp_path)
    r2 = record_audit.write_audit(RUN, root=tmp_path)

    assert r1["id"] != r2["id"]
    assert r1["query_hash"] == r2["query_hash"]  # dedup signal still shared
    assert (tmp_path / r1["detail_file"]).exists()
    assert (tmp_path / r2["detail_file"]).exists()
    assert len(_index_lines(tmp_path)) == 2


def test_corpus_scaffolding_created(tmp_path):
    record_audit.write_audit(RUN, root=tmp_path)
    assert (tmp_path / "runs").is_dir()
    assert (tmp_path / "reports").is_dir()
    assert (tmp_path / ".gitignore").exists()
    assert (tmp_path / "README.md").exists()


def test_missing_query_raises(tmp_path):
    with pytest.raises(ValueError):
        record_audit.write_audit({"outcome": "improved"}, root=tmp_path)


def test_bad_outcome_raises(tmp_path):
    with pytest.raises(ValueError):
        record_audit.write_audit({"query": RAW_QUERY, "outcome": "nope"}, root=tmp_path)


def test_malformed_run_fields_are_rejected_instead_of_coerced(tmp_path):
    with pytest.raises(ValueError, match="equivalence_proven"):
        record_audit.write_audit(
            dict(RUN, equivalence_proven="false"),
            root=tmp_path,
        )
    with pytest.raises(ValueError, match="tables must be a list"):
        record_audit.write_audit(dict(RUN, tables="dbo.Orders"), root=tmp_path)


def test_cli_uses_env_audit_dir(tmp_path, monkeypatch):
    audit_dir = tmp_path / "corpus"
    monkeypatch.setenv("SQL_OPTIMIZER_AUDIT_DIR", str(audit_dir))
    run_file = tmp_path / "audit.json"
    run_file.write_text(json.dumps(RUN), encoding="utf-8")

    rc = record_audit.main(["record_audit.py", "--input", str(run_file)])
    assert rc == 0
    assert len(_index_lines(audit_dir)) == 1


def test_cli_opt_out_writes_nothing(tmp_path, monkeypatch):
    audit_dir = tmp_path / "corpus"
    monkeypatch.setenv("SQL_OPTIMIZER_AUDIT_DIR", str(audit_dir))
    monkeypatch.setenv("SQL_OPTIMIZER_AUDIT", "0")
    run_file = tmp_path / "audit.json"
    run_file.write_text(json.dumps(RUN), encoding="utf-8")

    rc = record_audit.main(["record_audit.py", "--input", str(run_file)])
    assert rc == 0
    assert not audit_dir.exists()


def test_audit_dir_resolution(tmp_path, monkeypatch):
    import pathlib

    import record_audit as ra

    monkeypatch.setattr(pathlib.Path, "home", lambda: tmp_path)
    monkeypatch.delenv("SQL_OPTIMIZER_AUDIT_DIR", raising=False)
    assert ra.audit_dir() == tmp_path / ".sql-skills" / "sql_optimizer" / "audits"

    legacy = tmp_path / ".copilot" / "skills" / "sql_optimizer" / "audits"
    legacy.mkdir(parents=True)
    assert ra.audit_dir() == legacy

    monkeypatch.setenv("SQL_OPTIMIZER_AUDIT_DIR", str(tmp_path / "override"))
    assert ra.audit_dir() == tmp_path / "override"


def test_concurrent_audits_are_serialized_and_private(tmp_path):
    runs = [
        dict(RUN, query=f"{RAW_QUERY} AND OrderId = {index}")
        for index in range(12)
    ]
    with ThreadPoolExecutor(max_workers=6) as executor:
        records = list(
            executor.map(lambda run: record_audit.write_audit(run, root=tmp_path), runs)
        )

    lines = _index_lines(tmp_path)
    assert len(lines) == len(runs)
    assert all(validate(json.loads(line)) == [] for line in lines)
    assert len({record["id"] for record in records}) == len(runs)
    assert S_IMODE(tmp_path.stat().st_mode) == 0o700
    assert S_IMODE((tmp_path / "runs").stat().st_mode) == 0o700
    assert S_IMODE((tmp_path / "reports").stat().st_mode) == 0o700
    assert S_IMODE((tmp_path / ".lock").stat().st_mode) == 0o600
    assert S_IMODE((tmp_path / "index.jsonl").stat().st_mode) == 0o600
    assert all(
        S_IMODE((tmp_path / record["detail_file"]).stat().st_mode) == 0o600
        for record in records
    )


def test_storage_rejects_symlinked_parent_component(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "linked"
    link.symlink_to(real, target_is_directory=True)

    with pytest.raises(OSError, match="symlinked"):
        record_audit.write_audit(RUN, root=link / "audits")
