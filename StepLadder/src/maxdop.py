from __future__ import annotations


def compute_maxdop(vcores: int) -> int:
    if vcores < 1:
        raise ValueError("vcores must be >= 1")

    return min(8, max(1, vcores // 2))


def build_maxdop_statement(maxdop: int) -> str:
    if maxdop < 1:
        raise ValueError("maxdop must be >= 1")

    return f"ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = {maxdop};"
