import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import copy_table as ct  # noqa: E402


# ── split_ranges ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("lo,hi,parts", [
    (1, 100, 8), (0, 0, 4), (1, 3, 10), (-50, 49, 3), (1, 1_000_003, 7),
])
def test_split_ranges_covers_every_int_exactly_once(lo, hi, parts):
    ranges = ct.split_ranges(lo, hi, parts)
    assert ranges[0][0] == lo and ranges[-1][1] == hi
    assert len(ranges) <= parts
    for (a, b), (c, _) in zip(ranges, ranges[1:]):
        assert a <= b and c == b + 1  # contiguous, non-overlapping


def test_split_ranges_caps_parts_at_span():
    assert ct.split_ranges(5, 7, 10) == [(5, 5), (6, 6), (7, 7)]


def test_split_ranges_single_part():
    assert ct.split_ranges(10, 20, 1) == [(10, 20)]


def test_split_ranges_rejects_inverted():
    with pytest.raises(ValueError):
        ct.split_ranges(5, 4, 2)


# ── identifiers / SQL ────────────────────────────────────────────────────────

def test_parse_table_defaults_schema():
    assert ct.parse_table("Orders") == ("dbo", "Orders")
    assert ct.parse_table("sales.Orders") == ("sales", "Orders")
    with pytest.raises(ValueError):
        ct.parse_table("a.b.c")


@pytest.mark.parametrize("bad", ["Orders]; DROP TABLE x;--", "1abc", "a b", "", "dbo.Orders"])
def test_ident_rejects_unsafe(bad):
    with pytest.raises(ValueError):
        ct.ident(bad)


def test_select_sql_uses_closed_range_on_bracketed_identifiers():
    sql = ct.select_sql("sales.Orders", ["OrderId", "Total"], "OrderId")
    assert sql == ("SELECT [OrderId], [Total] FROM [sales].[Orders] "
                   "WHERE [OrderId] >= ? AND [OrderId] <= ?")


# ── manifest ─────────────────────────────────────────────────────────────────

def test_manifest_json_round_trip():
    m = ct.Manifest(run_id="r1", source_table="dbo.T", partition_column="Id",
                    columns=["Id", "Name"], source_count=7, created_utc="2026-09-02T00:00:00+00:00",
                    parts=[ct.Part("part-0000", 1, 4, "r1/part-0000.parquet", 4),
                           ct.Part("part-0001", 5, 9, None, 0)])
    m2 = ct.Manifest.from_json(m.to_json())
    assert m2 == m
    assert m2.total_rows == 4
    assert ct.manifest_blob("r1") == "r1/manifest.json"
    assert ct.part_blob("r1", 3) == "r1/part-0003.parquet"


# ── arg validation ───────────────────────────────────────────────────────────

def test_extract_requires_table_and_key():
    with pytest.raises(SystemExit):
        ct.parse_args(["--phase", "extract", "--table", "dbo.T"])


def test_load_requires_run_id():
    with pytest.raises(SystemExit):
        ct.parse_args(["--phase", "load"])


def test_load_only_does_not_need_table():
    a = ct.parse_args(["--phase", "load", "--run-id", "r1"])
    assert a.run_id == "r1" and a.table is None
