from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from stat import S_IMODE

import pytest

import test_index_ledger as ledger


def make_record():
    return ledger.build_record(
        database="sandbox-db",
        schema="dbo",
        table="Orders",
        index="IX_Testing_Orders_Status",
        now=datetime(2026, 7, 9, 12, 0, tzinfo=timezone.utc),
        nonce="abc123",
    )


def test_lifecycle_is_durable_and_pending_until_drop(tmp_path):
    record = ledger.begin(make_record(), root=tmp_path)
    assert ledger.pending(tmp_path)[0]["status"] == "planned"

    ledger.transition(record["id"], "created", root=tmp_path)
    pending = ledger.pending(tmp_path)
    assert pending[0]["rollback_sql"] == "DROP INDEX [IX_Testing_Orders_Status] ON [dbo].[Orders]"

    ledger.transition(record["id"], "dropped", root=tmp_path)
    assert ledger.pending(tmp_path) == []


def test_invalid_transition_is_rejected(tmp_path):
    record = ledger.begin(make_record(), root=tmp_path)
    with pytest.raises(ValueError, match="cannot move"):
        ledger.transition(record["id"], "dropped", root=tmp_path)


def test_created_index_cannot_be_hidden_as_failed(tmp_path):
    record = ledger.begin(make_record(), root=tmp_path)
    ledger.transition(record["id"], "created", root=tmp_path)

    with pytest.raises(ValueError, match="cannot move"):
        ledger.transition(record["id"], "failed", root=tmp_path)

    assert ledger.pending(tmp_path)[0]["status"] == "created"


def test_begin_rejects_duplicate_record_id(tmp_path):
    record = ledger.begin(make_record(), root=tmp_path)

    with pytest.raises(ValueError, match="already exists"):
        ledger.begin(record, root=tmp_path)


def test_non_test_index_is_rejected():
    with pytest.raises(ValueError, match="IX_Testing_"):
        ledger.build_record(
            database="sandbox-db", schema="dbo", table="Orders", index="IX_Orders_Status",
        )


def test_custom_or_corrupt_rollback_sql_is_rejected():
    with pytest.raises(ValueError, match="exactly match"):
        ledger.build_record(
            database="sandbox-db",
            schema="dbo",
            table="Orders",
            index="IX_Testing_Orders_Status",
            rollback_sql="DROP TABLE dbo.Orders",
        )

    record = make_record()
    record["rollback_sql"] = "DROP TABLE dbo.Orders"
    assert any("rollback_sql" in problem for problem in ledger.validate(record))


def test_loaded_identifiers_and_timestamps_are_revalidated():
    record = make_record()
    record["table"] = "Orders; DROP TABLE x"
    record["created_at"] = "yesterday"
    problems = ledger.validate(record)
    assert any("table" in problem for problem in problems)
    assert any("created_at" in problem for problem in problems)


def test_experiment_id_cannot_escape_records_directory(tmp_path):
    with pytest.raises(ValueError, match="plain file name"):
        ledger.load("../outside", root=tmp_path)


def test_concurrent_experiments_are_serialized_and_private(tmp_path):
    records = [
        ledger.build_record(
            database="sandbox-db",
            schema="dbo",
            table="Orders",
            index=f"IX_Testing_Orders_Status_{index}",
            nonce=f"nonce{index:02d}",
        )
        for index in range(12)
    ]
    with ThreadPoolExecutor(max_workers=6) as executor:
        list(executor.map(lambda record: ledger.begin(record, root=tmp_path), records))

    assert len(list((tmp_path / "records").glob("*.json"))) == len(records)
    assert len((tmp_path / "index.jsonl").read_text(encoding="utf-8").splitlines()) == len(records)
    assert S_IMODE(tmp_path.stat().st_mode) == 0o700
    assert S_IMODE((tmp_path / "records").stat().st_mode) == 0o700
    assert S_IMODE((tmp_path / ".lock").stat().st_mode) == 0o600
    assert S_IMODE((tmp_path / "index.jsonl").stat().st_mode) == 0o600
    assert all(
        S_IMODE(path.stat().st_mode) == 0o600
        for path in (tmp_path / "records").glob("*.json")
    )


def test_concurrent_transition_allows_only_one_valid_state_change(tmp_path):
    record = ledger.begin(make_record(), root=tmp_path)

    def mark_created():
        try:
            ledger.transition(record["id"], "created", root=tmp_path)
        except ValueError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _unused: mark_created(), range(2)))

    assert sorted(outcomes) == [False, True]
    assert ledger.load(record["id"], root=tmp_path)["status"] == "created"
    assert len((tmp_path / "index.jsonl").read_text(encoding="utf-8").splitlines()) == 2


def test_load_rejects_symlinked_record(tmp_path):
    records = tmp_path / "records"
    records.mkdir(parents=True)
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    (records / "abc.json").symlink_to(outside)

    with pytest.raises(OSError, match="symlinked"):
        ledger.load("abc", root=tmp_path)
