from src.maxdop import build_maxdop_statement, compute_maxdop


def test_compute_maxdop_mapping() -> None:
    assert compute_maxdop(2) == 1
    assert compute_maxdop(4) == 2
    assert compute_maxdop(8) == 4
    assert compute_maxdop(12) == 6
    assert compute_maxdop(16) == 8


def test_compute_maxdop_caps_at_eight() -> None:
    assert compute_maxdop(32) == 8


def test_build_maxdop_statement() -> None:
    assert build_maxdop_statement(4) == "ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 4;"
