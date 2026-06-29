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
