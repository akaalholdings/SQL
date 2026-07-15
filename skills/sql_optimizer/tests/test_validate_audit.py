"""The corpus contract must reject malformed rows so the review pass can trust every record."""

import copy

import pytest

from validate_audit import validate

VALID = {
    "id": "20260625T120000Z__abc123def456",
    "timestamp": "2026-06-25T12:00:00Z",
    "query_hash": "abc123def456",
    "environment": "mid",
    "tables": ["dbo.orders"],
    "anti_patterns": ["sargability"],
    "rules_applied": ["rule1_sargability"],
    "index_changes": {"adds": 1, "drops": 0, "alters": 0},
    "metrics": {"baseline": {}, "optimized": {}, "optimized_indexed": {}},
    "improvement": {"duration_pct": 50},
    "equivalence_proven": True,
    "outcome": "improved",
    "guidance_gaps": [],
    "detail_file": "runs/20260625T120000Z__abc123def456.md",
}


def test_valid_record_passes():
    assert validate(VALID) == []


def test_missing_required_field_fails():
    record = copy.deepcopy(VALID)
    del record["outcome"]
    problems = validate(record)
    assert any("outcome" in p for p in problems)


def test_bad_outcome_enum_fails():
    record = copy.deepcopy(VALID)
    record["outcome"] = "made_it_faster"
    problems = validate(record)
    assert any("outcome must be one of" in p for p in problems)


@pytest.mark.parametrize(
    "field, bad_value",
    [
        ("tables", "dbo.orders"),          # must be a list, not a string
        ("anti_patterns", [1, 2]),          # must be a list of strings
        ("equivalence_proven", "true"),     # must be a real bool
        ("metrics", []),                    # must be an object
        ("query_hash", 12345),              # must be a string
    ],
)
def test_wrong_type_fails(field, bad_value):
    record = copy.deepcopy(VALID)
    record[field] = bad_value
    assert validate(record), f"expected {field}={bad_value!r} to be rejected"


def test_non_object_record_fails():
    assert validate(["not", "an", "object"])


def test_improved_requires_equivalence_and_finite_metrics():
    record = copy.deepcopy(VALID)
    record["equivalence_proven"] = False
    record["improvement"]["duration_pct"] = float("nan")
    problems = validate(record)
    assert any("equivalence_proven" in problem for problem in problems)
    assert any("finite number" in problem for problem in problems)


def test_counts_hash_timestamp_and_detail_path_are_strict():
    record = copy.deepcopy(VALID)
    record["index_changes"]["adds"] = True
    record["query_hash"] = "not-a-hash"
    record["timestamp"] = "yesterday"
    record["detail_file"] = "../outside.md"
    problems = validate(record)
    assert any("non-negative integer" in problem for problem in problems)
    assert any("query_hash" in problem for problem in problems)
    assert any("timestamp" in problem for problem in problems)
    assert any("detail_file" in problem for problem in problems)


def test_metrics_index_shape_and_detail_identity_are_strict():
    record = copy.deepcopy(VALID)
    del record["index_changes"]["drops"]
    record["metrics"]["baseline"] = {"duration_ms": float("inf")}
    record["detail_file"] = "runs/some-other-record.md"
    problems = validate(record)
    assert any("index_changes.drops is required" in problem for problem in problems)
    assert any("finite numbers" in problem for problem in problems)
    assert any("match the record id" in problem for problem in problems)
