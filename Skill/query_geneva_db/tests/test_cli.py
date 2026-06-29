import pytest

from query_geneva_db.cli import (
    DatasetProfile,
    enforce_query_safety,
    extract_numeric_predicates,
    generate_sql_from_nl,
    looks_like_sql,
    sanitize_query,
)


def test_sanitize_query_handles_fence_comments_and_go() -> None:
    raw = """
    ```sql
    -- comment
    SELECT TOP (10) [A]
    FROM [dbo].[T]
    GO
    ```
    """
    cleaned = sanitize_query(raw)
    assert cleaned == "SELECT TOP (10) [A]\nFROM [dbo].[T]"


def test_enforce_query_safety_rejects_select_star() -> None:
    with pytest.raises(SystemExit):
        enforce_query_safety("SELECT * FROM dbo.TableX")


def test_looks_like_sql_and_nl_detection() -> None:
    assert looks_like_sql("SELECT TOP (5) [ID] FROM [dbo].[Orders];")
    assert not looks_like_sql("show orders above 1000 today")


def test_generate_sql_from_nl_builds_top_query() -> None:
    profile = DatasetProfile(
        dataset_name="Orders",
        technical_name="F_ORDERS",
        resolved_table_name="[cns_sales].[F_ORDERS]",
        purpose="Consumption",
        columns={"ORDER_ID", "ORDER_VALUE", "ORDER_PROCESSED_DTTM"},
    )
    sql = generate_sql_from_nl(
        profile=profile,
        selected_columns=["ORDER_ID", "ORDER_VALUE", "ORDER_PROCESSED_DTTM"],
        request="show orders processed today above 1000",
        preview_rows=25,
    )

    assert "SELECT TOP (25)" in sql
    assert "FROM [cns_sales].[F_ORDERS] AS src" in sql
    assert "src.[ORDER_ID]" in sql
    assert "CAST(src.[ORDER_PROCESSED_DTTM] AS date) = CAST(GETDATE() AS date)" in sql


def test_extract_numeric_predicates_finds_mapped_column() -> None:
    predicates = extract_numeric_predicates(
        "order value greater than 1000",
        {"ORDER_ID", "ORDER_VALUE"},
        "src",
    )
    assert predicates == ["src.[ORDER_VALUE] > 1000"]
