"""Writing an audit must append a valid index row and persist the raw query verbatim."""

import json

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


def test_write_audit_appends_valid_row_and_detail(tmp_path):
    record = record_audit.write_audit(RUN, root=tmp_path)

    lines = _index_lines(tmp_path)
    assert len(lines) == 1
    persisted = json.loads(lines[0])
    assert validate(persisted) == []
    assert persisted["id"] == record["id"]
    assert persisted["outcome"] == "improved"

    detail = tmp_path / record["detail_file"]
    assert detail.exists()
    # raw query stored verbatim
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
