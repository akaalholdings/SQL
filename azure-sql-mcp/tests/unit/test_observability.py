from __future__ import annotations

from azure_sql_mcp.observability import (
    compute_query_hash,
    extract_sql_error_info,
    redact_sql_literals,
    sanitize_error_message,
)


class TestExtractSqlErrorInfo:
    def test_sqlstate_from_string(self):
        exc = Exception("[42S02] Table not found (Error 208)")
        info = extract_sql_error_info(exc)
        assert info["sqlstate"] == "42S02"
        assert info["native_error_code"] == 208

    def test_nested_exception(self):
        inner = Exception("[HY000] something went wrong")
        outer = Exception("Wrapper")
        outer.__cause__ = inner
        info = extract_sql_error_info(outer)
        assert info["sqlstate"] == "HY000"

    def test_no_sql_info(self):
        exc = Exception("generic error")
        info = extract_sql_error_info(exc)
        assert info == {}

    def test_tuple_args(self):
        exc = Exception(("08S01", "Connection lost"))
        info = extract_sql_error_info(exc)
        assert info["sqlstate"] == "08S01"


class TestComputeQueryHash:
    def test_deterministic(self):
        h1 = compute_query_hash("SELECT 1 FROM t")
        h2 = compute_query_hash("SELECT 1 FROM t")
        assert h1 == h2

    def test_whitespace_normalized(self):
        h1 = compute_query_hash("SELECT  1   FROM   t")
        h2 = compute_query_hash("select 1 from t")
        assert h1 == h2

    def test_different_queries_different_hash(self):
        h1 = compute_query_hash("SELECT 1")
        h2 = compute_query_hash("SELECT 2")
        assert h1 != h2


class TestSanitizeErrorMessage:
    def test_strips_connection_string(self):
        msg = "Error connecting: Server=myserver.database.windows.net;User ID=admin;Password=secret123;"
        sanitized = sanitize_error_message(msg)
        assert "secret123" not in sanitized
        assert "admin" not in sanitized
        assert "myserver.database.windows.net" not in sanitized

    def test_strips_server_name(self):
        msg = "Cannot connect to prod-sql-01.database.windows.net"
        sanitized = sanitize_error_message(msg)
        assert "prod-sql-01.database.windows.net" not in sanitized
        assert "[server]" in sanitized

    def test_strips_ip(self):
        msg = "Connection refused by 10.0.1.5 on port 1433"
        sanitized = sanitize_error_message(msg)
        assert "10.0.1.5" not in sanitized
        assert "[ip]" in sanitized

    def test_strips_uid_pwd_connection_tokens(self):
        msg = "SERVER=tcp:prod.database.windows.net;DATABASE=appdb;UID=sa;PWD=secret!;"
        sanitized = sanitize_error_message(msg)
        assert "UID=sa" not in sanitized
        assert "PWD=secret!" not in sanitized
        assert "prod.database.windows.net" not in sanitized

    def test_preserves_generic_messages(self):
        msg = "Timeout expired while waiting for query"
        assert sanitize_error_message(msg) == msg

    def test_preserves_apostrophe_in_generic_message(self):
        msg = "Can't connect because the session isn't available"
        assert sanitize_error_message(msg) == msg

    def test_strips_sql_literals(self):
        msg = "Incorrect syntax near N'SuperSecret-123!'"
        sanitized = sanitize_error_message(msg)
        assert "SuperSecret-123!" not in sanitized
        assert "N'[REDACTED]'" in sanitized


class TestRedactSqlLiterals:
    def test_redacts_plain_unicode_and_escaped_literals(self):
        sql = "SELECT 'plain', N'unicode', 'it''s private', 42"
        assert redact_sql_literals(sql) == (
            "SELECT '[REDACTED]', N'[REDACTED]', '[REDACTED]', 42"
        )

    def test_redacts_multiple_literals_without_changing_identifiers(self):
        sql = "EXEC dbo.RotateLogin @login = 'dba', @password = 's3cret'"
        assert redact_sql_literals(sql) == (
            "EXEC dbo.RotateLogin @login = '[REDACTED]', @password = '[REDACTED]'"
        )

    def test_redacts_unterminated_literal_through_end_of_input(self):
        assert redact_sql_literals("SELECT 'never closed") == "SELECT '[REDACTED]'"

    def test_redacts_double_quoted_values_for_quoted_identifier_off(self):
        sql = 'SET QUOTED_IDENTIFIER OFF; CREATE LOGIN x WITH PASSWORD = "S3cret!"'

        assert redact_sql_literals(sql) == (
            'SET QUOTED_IDENTIFIER OFF; CREATE LOGIN x WITH PASSWORD = "[REDACTED]"'
        )

    def test_redacts_literals_adjacent_to_keywords(self):
        assert redact_sql_literals("SELECT'synthetic-secret'") == (
            "SELECT'[REDACTED]'"
        )
        assert redact_sql_literals("PRINT'synthetic-secret'") == (
            "PRINT'[REDACTED]'"
        )
