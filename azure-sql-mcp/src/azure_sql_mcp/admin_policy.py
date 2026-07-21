from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

from .config import ServerConfig
from .config import WritePolicy
from .connection import BatchExecutionMode
from .connection import QueryResult
from .observability import redact_sql_literals
from .observability import sanitize_error_message


_LIFECYCLE_KEYWORDS = frozenset({"CREATE", "DROP"})
_DYNAMIC_SYSTEM_MODULE_SQL_POSITIONS = {
    "SP_EXECUTESQL": (0,),
    "SP_PREPEXEC": (2,),
    "SP_PREPARE": (2,),
    "SP_CURSORPREPEXEC": (3,),
    "SP_CURSORPREPARE": (2,),
    "SP_CURSOROPEN": (1,),
    "SP_EXECUTE_REMOTE": (1,),
    "SP_SQLEXEC": (0,),
    "SP_MSFOREACHDB": (0, 2, 3),
    "SP_MSFOREACHTABLE": (0, 2, 3),
}
_DYNAMIC_SYSTEM_MODULE_SQL_PARAMETERS = {
    "SP_EXECUTESQL": frozenset({"@STMT"}),
    "SP_PREPEXEC": frozenset({"@STMT"}),
    "SP_PREPARE": frozenset({"@STMT"}),
    "SP_CURSORPREPEXEC": frozenset({"@STMT", "@STATEMENT"}),
    "SP_CURSORPREPARE": frozenset({"@STMT", "@STATEMENT"}),
    "SP_CURSOROPEN": frozenset({"@STMT", "@STATEMENT"}),
    "SP_EXECUTE_REMOTE": frozenset({"@STMT"}),
    "SP_SQLEXEC": frozenset({"@CMD", "@STMT"}),
    "SP_MSFOREACHDB": frozenset(
        {"@COMMAND1", "@COMMAND2", "@COMMAND3", "@PRECOMMAND", "@POSTCOMMAND"}
    ),
    "SP_MSFOREACHTABLE": frozenset(
        {"@COMMAND1", "@COMMAND2", "@COMMAND3", "@PRECOMMAND", "@POSTCOMMAND"}
    ),
}
_DYNAMIC_EXECUTION_KEYWORDS = frozenset(
    {"EXEC", "EXECUTE", *_DYNAMIC_SYSTEM_MODULE_SQL_POSITIONS}
)
_ASSIGNMENT_BOUNDARY_KEYWORDS = frozenset(
    {"DECLARE", "EXEC", "EXECUTE", "PRINT", "SELECT", "SET"}
)
_CONSTANT_STRING_FUNCTIONS = frozenset(
    {
        "CHAR",
        "CONCAT",
        "CONCAT_WS",
        "LOWER",
        "NCHAR",
        "REPLACE",
        "REPLICATE",
        "REVERSE",
        "SPACE",
        "UPPER",
    }
)
_TIMEOUT_MARKERS = ("HYT00", "HYT01", "QUERY TIMEOUT", "TIMED OUT")
_MAX_CONSTANT_STRING_LENGTH = 100_000


@dataclass(frozen=True)
class _SqlToken:
    kind: str
    value: str


@dataclass(frozen=True)
class AdminAction:
    tool_name: str
    database_name: str
    action_type: str
    sql: str
    params: tuple[Any, ...] = ()
    rollback_sql: str | None = None
    trusted_generated: bool = False


class AdminPolicy:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.audit = AdminAuditLog(config.audit_dir, include_full_sql=config.audit_full_sql)

    def validate_sql(self, sql: str) -> None:
        _validate_database_lifecycle(sql)

    def preview(self, action: AdminAction) -> dict[str, Any]:
        self._validate_or_audit_block(action)
        audit_id = self.audit.record(action, outcome="preview")
        return self._payload(action, status="dry_run", audit_id=audit_id)

    async def execute(
        self,
        action: AdminAction,
        executor,
        *,
        dry_run: bool,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        if dry_run:
            return self.preview(action)
        self._validate_or_audit_block(action)
        if self.config.write_policy != WritePolicy.APPLY:
            audit_id = self.audit.record(
                action,
                outcome="blocked",
                error="AZURE_SQL_WRITE_POLICY=apply is required for write execution.",
            )
            raise PermissionError(
                "Write execution requires AZURE_SQL_WRITE_POLICY=apply "
                f"(audit_id={audit_id})."
            )
        audit_id = self.audit.record(action, outcome="apply_started")
        try:
            if action.action_type == "query":
                result = await executor.execute_batches(
                    action.database_name,
                    action.sql,
                    params=action.params,
                    max_rows=max_rows,
                    execution_mode=BatchExecutionMode.ADMIN,
                )
                self.audit.record(action, outcome="apply_completed", audit_id=audit_id)
                return self._payload(
                    action,
                    status="completed",
                    audit_id=audit_id,
                    result=_serialize_result_sets(result),
                )
            rowcount = await executor.execute_non_query(
                action.database_name,
                action.sql,
                params=action.params,
                execution_mode=BatchExecutionMode.ADMIN,
            )
            self.audit.record(action, outcome="apply_completed", audit_id=audit_id)
            return self._payload(action, status="completed", audit_id=audit_id, rowcount=rowcount)
        except asyncio.CancelledError as exc:
            self.audit.record(
                action,
                outcome="apply_outcome_unknown",
                audit_id=audit_id,
                error=str(exc) or type(exc).__name__,
            )
            raise
        except Exception as exc:
            self.audit.record(
                action,
                outcome=(
                    "apply_outcome_unknown" if _is_timeout_error(exc) else "apply_failed"
                ),
                audit_id=audit_id,
                error=str(exc),
            )
            raise

    def _validate_or_audit_block(self, action: AdminAction) -> None:
        try:
            self.validate_sql(action.sql)
        except PermissionError as exc:
            audit_id = self.audit.record(action, outcome="blocked", error=str(exc))
            raise PermissionError(f"{exc} (audit_id={audit_id})") from exc

    @staticmethod
    def _payload(
        action: AdminAction,
        *,
        status: str,
        audit_id: str,
        rowcount: int | None = None,
        result: Any | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "database_name": action.database_name,
            "tool_name": action.tool_name,
            "action_type": action.action_type,
            "status": status,
            "dry_run": status == "dry_run",
            "audit_id": audit_id,
            "sql_preview": _preview_sql(action.sql),
            "sql_hash": _hash_sql(action.sql),
        }
        if action.rollback_sql:
            payload["rollback_sql"] = action.rollback_sql
        if action.params:
            payload["param_count"] = len(action.params)
        if rowcount is not None:
            payload["rowcount"] = rowcount
        if result is not None:
            payload["result_sets"] = result
        return payload


class AdminAuditLog:
    def __init__(self, audit_dir: str, *, include_full_sql: bool):
        self.audit_dir = Path(audit_dir).expanduser()
        self.include_full_sql = include_full_sql

    def record(
        self,
        action: AdminAction,
        *,
        outcome: str,
        audit_id: str | None = None,
        error: str | None = None,
    ) -> str:
        event_id = audit_id or str(uuid.uuid4())
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.audit_dir.chmod(0o700)
        except OSError:
            pass
        event: dict[str, Any] = {
            "audit_id": event_id,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "tool_name": action.tool_name,
            "database_name": action.database_name,
            "action_type": action.action_type,
            "outcome": outcome,
            "sql_hash": _hash_sql(action.sql),
            "sql_preview": _preview_sql(action.sql),
            "param_count": len(action.params),
        }
        if action.rollback_sql:
            event["rollback_sql"] = redact_sql_literals(action.rollback_sql)
        if self.include_full_sql:
            event["sql"] = action.sql
        if error:
            event["error"] = sanitize_error_message(error)
        path = self.audit_dir / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"
        fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, default=str) + "\n")
        return event_id


def _hash_sql(sql: str) -> str:
    normalized = re.sub(r"\s+", " ", sql.strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _preview_sql(sql: str, limit: int = 500) -> str:
    normalized = re.sub(r"\s+", " ", redact_sql_literals(sql).strip())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def _serialize_result_sets(result_sets: Any) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    if not isinstance(result_sets, list):
        return serialized
    for result in result_sets:
        if isinstance(result, QueryResult):
            serialized.append(
                {
                    "columns": list(result.columns),
                    "rows": result.rows,
                    "row_count": len(result.rows),
                }
            )
        elif isinstance(result, dict):
            serialized.append(result)
    return serialized


def _is_timeout_error(exc: Exception) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TimeoutError):
            return True
        for argument in getattr(current, "args", ()):
            text = str(argument).upper()
            if any(marker in text for marker in _TIMEOUT_MARKERS):
                return True
        current = current.__cause__ or current.__context__
    return False


def _validate_database_lifecycle(sql: str) -> None:
    tokens = _tokenize_sql(sql)
    if not tokens:
        raise PermissionError("SQL rejected by admin policy: SQL cannot be empty.")
    if _contains_database_lifecycle(tokens):
        raise PermissionError(
            "SQL rejected by admin policy: CREATE DATABASE and DROP DATABASE are not allowed."
        )


def _contains_database_lifecycle(tokens: list[_SqlToken]) -> bool:
    if _contains_direct_database_lifecycle(tokens):
        return True
    if _contains_passthrough_database_lifecycle(tokens):
        return True

    for index, token in enumerate(tokens):
        if (
            token.kind not in {"word", "identifier"}
            or token.value not in _DYNAMIC_EXECUTION_KEYWORDS
        ):
            continue
        variables, unsafe_variables = _collect_literal_variables(tokens[:index])
        is_module_call, module_expression = _dynamic_module_argument_expression(
            tokens,
            index,
            variables,
            unsafe_variables,
        )
        if is_module_call:
            if module_expression is None:
                return True
            if not module_expression:
                continue
            expression = module_expression
        else:
            expression = _dynamic_sql_expression(tokens, index)
        if expression is None:
            continue
        if any(
            item.kind == "variable" and item.value in unsafe_variables
            for item in expression
        ):
            return True
        dynamic_sql = _literal_expression_text(expression, variables)
        if dynamic_sql is None or "\x00" in dynamic_sql:
            # Runtime-opaque dynamic SQL cannot prove the lifecycle invariant.
            return True
        if _contains_database_lifecycle(_tokenize_sql(dynamic_sql)):
            return True
    return False


def _dynamic_module_argument_expression(
    tokens: list[_SqlToken],
    index: int,
    variables: dict[str, str],
    unsafe_variables: set[str],
) -> tuple[bool, list[_SqlToken] | None]:
    if tokens[index].value not in {"EXEC", "EXECUTE"} or index + 1 >= len(tokens):
        return False, None
    module_index = index + 1
    module_token = tokens[module_index]
    if module_token.kind != "variable":
        return False, None
    cursor = module_index + 1
    has_return_status = (
        cursor < len(tokens)
        and tokens[cursor].kind == "symbol"
        and tokens[cursor].value == "="
    )
    if has_return_status:
        module_index = cursor + 1
        if module_index >= len(tokens) or tokens[module_index].kind != "variable":
            return False, None
        module_token = tokens[module_index]
        cursor = module_index + 1
    if cursor >= len(tokens) or (
        tokens[cursor].kind == "symbol" and tokens[cursor].value == ";"
    ):
        return (True, []) if has_return_status else (False, None)

    if module_token.value in unsafe_variables:
        return True, None
    module_name = variables.get(module_token.value)
    if module_name is None or "\x00" in module_name:
        return True, None
    normalized_name = re.sub(r"[\[\]\s]", "", module_name).upper().split(".")[-1]
    normalized_name = re.sub(r";\d+$", "", normalized_name)
    if normalized_name not in _DYNAMIC_SYSTEM_MODULE_SQL_POSITIONS:
        return True, []
    return True, _module_sql_argument_expression(
        tokens,
        cursor,
        normalized_name,
    )


def _contains_passthrough_database_lifecycle(tokens: list[_SqlToken]) -> bool:
    for index, token in enumerate(tokens):
        if token.kind != "word" or token.value not in {"OPENQUERY", "OPENROWSET"}:
            continue
        if (
            index + 1 >= len(tokens)
            or tokens[index + 1].kind != "symbol"
            or tokens[index + 1].value != "("
        ):
            continue
        close_index = _matching_closing_parenthesis(tokens, index + 1)
        if close_index is None:
            continue
        arguments = _split_function_arguments(tokens[index + 2 : close_index])
        query_index = 1 if token.value == "OPENQUERY" else 2
        if len(arguments) <= query_index:
            continue
        query = _literal_expression_text(arguments[query_index], {})
        if query and _contains_database_lifecycle(_tokenize_sql(query)):
            return True
    return False


def _contains_direct_database_lifecycle(tokens: list[_SqlToken]) -> bool:
    return any(
        token.kind == "word"
        and token.value in _LIFECYCLE_KEYWORDS
        and index + 1 < len(tokens)
        and tokens[index + 1].kind == "word"
        and tokens[index + 1].value == "DATABASE"
        for index, token in enumerate(tokens)
    )


def _tokenize_sql(sql: str) -> list[_SqlToken]:
    tokens: list[_SqlToken] = []
    index = 0
    while index < len(sql):
        character = sql[index]
        if character.isspace():
            index += 1
            continue
        if sql.startswith("--", index):
            index = _skip_line_comment(sql, index + 2)
            continue
        if sql.startswith("/*", index):
            index = _skip_block_comment(sql, index + 2)
            continue
        if character in "Nn" and index + 1 < len(sql) and sql[index + 1] == "'":
            value, index = _read_string_literal(sql, index + 1)
            tokens.append(_SqlToken("string", value))
            continue
        if character == "'":
            value, index = _read_string_literal(sql, index)
            tokens.append(_SqlToken("string", value))
            continue
        if character == "[":
            value, index = _read_bracket_identifier(sql, index + 1)
            tokens.append(_SqlToken("identifier", value.upper()))
            continue
        if character == '"':
            value, index = _read_quoted_identifier(sql, index + 1)
            tokens.append(_SqlToken("identifier", value.upper()))
            continue
        if character == "@":
            start = index
            index += 1
            while index < len(sql) and (sql[index].isalnum() or sql[index] in "_@$#"):
                index += 1
            tokens.append(_SqlToken("variable", sql[start:index].upper()))
            continue
        if character.isdigit():
            start = index
            index += 1
            if character == "0" and index < len(sql) and sql[index] in "Xx":
                index += 1
                while index < len(sql) and sql[index] in "0123456789abcdefABCDEF":
                    index += 1
            else:
                while index < len(sql) and sql[index].isdigit():
                    index += 1
            tokens.append(_SqlToken("number", sql[start:index]))
            continue
        if character.isalpha() or character in "_#$":
            start = index
            index += 1
            while index < len(sql) and (sql[index].isalnum() or sql[index] in "_#$"):
                index += 1
            tokens.append(_SqlToken("word", sql[start:index].upper()))
            continue
        tokens.append(_SqlToken("symbol", character))
        index += 1
    return tokens


def _skip_line_comment(sql: str, index: int) -> int:
    while index < len(sql) and sql[index] not in "\r\n":
        index += 1
    while index < len(sql) and sql[index] in "\r\n":
        index += 1
    return index


def _skip_block_comment(sql: str, index: int) -> int:
    depth = 1
    while index < len(sql) and depth:
        if sql.startswith("/*", index):
            depth += 1
            index += 2
        elif sql.startswith("*/", index):
            depth -= 1
            index += 2
        else:
            index += 1
    return index


def _read_string_literal(sql: str, index: int) -> tuple[str, int]:
    index += 1
    characters: list[str] = []
    while index < len(sql):
        if sql[index] != "'":
            characters.append(sql[index])
            index += 1
        elif index + 1 < len(sql) and sql[index + 1] == "'":
            characters.append("'")
            index += 2
        else:
            return "".join(characters), index + 1
    return "".join(characters), index


def _read_bracket_identifier(sql: str, index: int) -> tuple[str, int]:
    characters: list[str] = []
    while index < len(sql):
        if sql[index] == "]":
            if index + 1 < len(sql) and sql[index + 1] == "]":
                characters.append("]")
                index += 2
            else:
                return "".join(characters), index + 1
        else:
            characters.append(sql[index])
            index += 1
    return "".join(characters), index


def _read_quoted_identifier(sql: str, index: int) -> tuple[str, int]:
    characters: list[str] = []
    while index < len(sql):
        if sql[index] == '"':
            if index + 1 < len(sql) and sql[index + 1] == '"':
                characters.append('"')
                index += 2
            else:
                return "".join(characters), index + 1
        else:
            characters.append(sql[index])
            index += 1
    return "".join(characters), index


def _collect_literal_variables(
    tokens: list[_SqlToken],
) -> tuple[dict[str, str], set[str]]:
    variables: dict[str, str] = {}
    unsafe_variables: set[str] = set()
    for index, token in enumerate(tokens):
        if token.kind != "variable":
            continue
        if _variable_has_opaque_write(tokens, index):
            unsafe_variables.add(token.value)
        equals_index = index + 1
        while equals_index < len(tokens):
            candidate = tokens[equals_index]
            if candidate.kind == "symbol" and candidate.value == "=":
                break
            if candidate.kind == "symbol" and candidate.value == ";":
                equals_index = len(tokens)
                break
            if (
                candidate.kind == "word"
                and candidate.value in _ASSIGNMENT_BOUNDARY_KEYWORDS
                and equals_index > index + 1
            ):
                equals_index = len(tokens)
                break
            equals_index += 1
        if equals_index >= len(tokens):
            continue
        end = _expression_end(tokens, equals_index + 1)
        expression = tokens[equals_index + 1 : end]
        value = _literal_expression_text(expression, variables)
        if value is not None:
            append = (
                equals_index > index + 1
                and tokens[equals_index - 1].kind == "symbol"
                and tokens[equals_index - 1].value == "+"
            )
            variables[token.value] = variables.get(token.value, "") + value if append else value
        if (
            value is None
            or "\x00" in value
            or any(
                item.kind == "variable" and item.value in unsafe_variables
                for item in expression
            )
            or _contains_database_lifecycle(_tokenize_sql(value))
        ):
            # Keep the risk sticky across later assignments. SQL control flow can
            # make an earlier value the one that reaches an execution point even
            # when a later textual assignment looks safe.
            unsafe_variables.add(token.value)
    return variables, unsafe_variables


def _variable_has_opaque_write(tokens: list[_SqlToken], index: int) -> bool:
    if (
        index + 1 < len(tokens)
        and tokens[index + 1].kind == "word"
        and tokens[index + 1].value in {"OUT", "OUTPUT"}
    ):
        return True
    statement_start = index
    while statement_start > 0:
        previous = tokens[statement_start - 1]
        if previous.kind == "symbol" and previous.value == ";":
            break
        statement_start -= 1
    preceding_words = {
        item.value
        for item in tokens[statement_start:index]
        if item.kind == "word"
    }
    return "FETCH" in preceding_words and "INTO" in preceding_words


def _dynamic_sql_expression(
    tokens: list[_SqlToken], index: int,
) -> list[_SqlToken] | None:
    token = tokens[index]
    cursor = index + 1
    wrapped = False
    if token.value in {"EXEC", "EXECUTE"}:
        if cursor >= len(tokens):
            return None
        if tokens[cursor].kind == "word" and tokens[cursor].value == "AS":
            return None
        if tokens[cursor].kind == "symbol" and tokens[cursor].value == "(":
            wrapped = True
            cursor += 1
        else:
            module = _dynamic_system_module_name_end(tokens, cursor)
            if module is not None:
                module_end, module_name = module
                return _module_sql_argument_expression(
                    tokens,
                    module_end,
                    module_name,
                )
            elif tokens[cursor].kind not in {"string", "variable"}:
                return None
    else:
        module = _dynamic_system_module_name_end(tokens, index)
        if module is not None:
            module_end, module_name = module
            return _module_sql_argument_expression(
                tokens,
                module_end,
                module_name,
            )

    if cursor < len(tokens) and tokens[cursor].kind == "variable":
        if cursor + 1 < len(tokens) and tokens[cursor + 1].value == "=":
            cursor += 2
            if cursor < len(tokens):
                module = _dynamic_system_module_name_end(tokens, cursor)
                if module is not None:
                    module_end, module_name = module
                    return _module_sql_argument_expression(
                        tokens,
                        module_end,
                        module_name,
                    )
                elif tokens[cursor].kind in {"word", "identifier"}:
                    return None
    if cursor >= len(tokens):
        return None
    end = _expression_end(tokens, cursor, stop_at_closing=wrapped)
    return tokens[cursor:end]


def _dynamic_system_module_name_end(
    tokens: list[_SqlToken], index: int,
) -> tuple[int, str] | None:
    if index >= len(tokens) or tokens[index].kind not in {"word", "identifier"}:
        return None
    cursor = index + 1
    module_name = tokens[index].value
    while (
        cursor + 1 < len(tokens)
        and tokens[cursor].kind == "symbol"
        and tokens[cursor].value == "."
        and tokens[cursor + 1].kind in {"word", "identifier"}
    ):
        module_name = tokens[cursor + 1].value
        cursor += 2
    if module_name not in _DYNAMIC_SYSTEM_MODULE_SQL_POSITIONS:
        return None
    if (
        cursor + 1 < len(tokens)
        and tokens[cursor].kind == "symbol"
        and tokens[cursor].value == ";"
        and tokens[cursor + 1].kind == "number"
    ):
        cursor += 2
    return cursor, module_name


def _module_sql_argument_expression(
    tokens: list[_SqlToken],
    start: int,
    module_name: str,
) -> list[_SqlToken] | None:
    statement_end = _statement_end(tokens, start)
    arguments = _split_function_arguments(tokens[start:statement_end])
    positions = _DYNAMIC_SYSTEM_MODULE_SQL_POSITIONS[module_name]
    parameter_names = _DYNAMIC_SYSTEM_MODULE_SQL_PARAMETERS[module_name]
    selected: list[list[_SqlToken]] = []
    for position, argument in enumerate(arguments):
        is_named = (
            len(argument) >= 3
            and argument[0].kind == "variable"
            and argument[1].kind == "symbol"
            and argument[1].value == "="
        )
        if is_named:
            if argument[0].value in parameter_names:
                selected.append(argument[2:])
            continue
        if position in positions:
            selected.append(argument)
    if not selected:
        return None
    combined: list[_SqlToken] = []
    for expression in selected:
        if len(expression) == 1 and expression[0].kind == "word" and expression[0].value == "NULL":
            continue
        if combined:
            combined.append(_SqlToken("string", ";"))
        combined.extend(expression)
    return combined


def _statement_end(tokens: list[_SqlToken], index: int) -> int:
    depth = 0
    for cursor in range(index, len(tokens)):
        token = tokens[cursor]
        if token.kind != "symbol":
            continue
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            depth = max(0, depth - 1)
        elif token.value == ";" and depth == 0:
            return cursor
    return len(tokens)


def _expression_end(
    tokens: list[_SqlToken], index: int, *, stop_at_closing: bool = False,
) -> int:
    depth = 0
    for cursor in range(index, len(tokens)):
        token = tokens[cursor]
        if token.kind != "symbol":
            continue
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            if depth == 0 and stop_at_closing:
                return cursor
            depth = max(0, depth - 1)
        elif depth == 0 and token.value in {",", ";"}:
            return cursor
    return len(tokens)


def _literal_expression_text(
    tokens: list[_SqlToken], variables: dict[str, str],
) -> str | None:
    if not tokens:
        return None
    parts: list[str] = []
    found_value = False
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.kind == "string":
            parts.append(token.value)
            found_value = True
            index += 1
            continue
        if token.kind == "identifier":
            # With QUOTED_IDENTIFIER OFF, double-quoted tokens are strings. Treat
            # them as potential constant dynamic SQL while retaining identifier
            # handling everywhere outside an executed expression.
            parts.append(token.value)
            found_value = True
            index += 1
            continue
        if token.kind == "variable":
            parts.append(variables.get(token.value, "\x00"))
            found_value = True
            index += 1
            continue
        if token.kind == "number":
            # A varbinary literal can be implicitly converted to varchar and then
            # executed. Treat it as opaque instead of interpreting its hex bytes.
            parts.append("\x00" if token.value.upper().startswith("0X") else token.value)
            found_value = True
            index += 1
            continue
        if (
            token.kind == "word"
            and token.value in _CONSTANT_STRING_FUNCTIONS
            and index + 1 < len(tokens)
            and tokens[index + 1].kind == "symbol"
            and tokens[index + 1].value == "("
        ):
            close_index = _matching_closing_parenthesis(tokens, index + 1)
            if close_index is None:
                parts.append("\x00")
                found_value = True
                index += 1
                continue
            function_value = _constant_function_text(
                token.value,
                tokens[index + 2 : close_index],
                variables,
            )
            parts.append(function_value if function_value is not None else "\x00")
            found_value = True
            index = close_index + 1
            continue
        if token.kind == "symbol" and token.value in {"+", "(", ")", "="}:
            index += 1
            continue
        parts.append("\x00")
        index += 1
    if not found_value:
        return None
    value = "".join(parts)
    return value if len(value) <= _MAX_CONSTANT_STRING_LENGTH else None


def _matching_closing_parenthesis(
    tokens: list[_SqlToken], open_index: int,
) -> int | None:
    depth = 0
    for index in range(open_index, len(tokens)):
        token = tokens[index]
        if token.kind != "symbol":
            continue
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            depth -= 1
            if depth == 0:
                return index
    return None


def _constant_function_text(
    function_name: str,
    argument_tokens: list[_SqlToken],
    variables: dict[str, str],
) -> str | None:
    arguments = _split_function_arguments(argument_tokens)
    if function_name == "CONCAT":
        values: list[str] = []
        for argument in arguments:
            if len(argument) == 1 and argument[0].kind == "word" and argument[0].value == "NULL":
                values.append("")
                continue
            value = _literal_expression_text(argument, variables)
            values.append(value if value is not None else "\x00")
        result = "".join(values)
        return result if len(result) <= _MAX_CONSTANT_STRING_LENGTH else None

    if function_name == "CONCAT_WS" and len(arguments) >= 2:
        separator = _literal_expression_text(arguments[0], variables)
        raw_values = [
            _literal_expression_text(argument, variables)
            for argument in arguments[1:]
        ]
        if separator is None or any(value is None for value in raw_values):
            return None
        values = [value for value in raw_values if value is not None]
        result = separator.join(values)
        return result if len(result) <= _MAX_CONSTANT_STRING_LENGTH else None

    if function_name == "REPLACE" and len(arguments) == 3:
        raw_values = [
            _literal_expression_text(argument, variables) for argument in arguments
        ]
        if any(value is None for value in raw_values):
            return None
        source, old, new = [value for value in raw_values if value is not None]
        result = source.replace(old, new)
        return result if len(result) <= _MAX_CONSTANT_STRING_LENGTH else None

    if function_name == "REPLICATE" and len(arguments) == 2:
        value = _literal_expression_text(arguments[0], variables)
        count = _constant_integer(arguments[1])
        if value is None or count is None or count < 0:
            return None
        if len(value) * count > _MAX_CONSTANT_STRING_LENGTH:
            return None
        return value * count

    if len(arguments) != 1:
        return None
    if function_name == "REVERSE":
        value = _literal_expression_text(arguments[0], variables)
        return value[::-1] if value is not None else None
    if function_name in {"LOWER", "UPPER"}:
        value = _literal_expression_text(arguments[0], variables)
        if value is None:
            return None
        return value.lower() if function_name == "LOWER" else value.upper()

    number = _constant_integer(arguments[0])
    if number is None:
        return None
    if function_name == "SPACE":
        if not 0 <= number <= _MAX_CONSTANT_STRING_LENGTH:
            return None
        return " " * number
    if function_name == "CHAR" and not 0 <= number <= 255:
        return None
    if function_name == "NCHAR" and not 0 <= number <= 0x10FFFF:
        return None
    try:
        return chr(number)
    except ValueError:
        return None


def _split_function_arguments(tokens: list[_SqlToken]) -> list[list[_SqlToken]]:
    arguments: list[list[_SqlToken]] = []
    start = 0
    depth = 0
    for index, token in enumerate(tokens):
        if token.kind != "symbol":
            continue
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            depth = max(0, depth - 1)
        elif token.value == "," and depth == 0:
            arguments.append(tokens[start:index])
            start = index + 1
    arguments.append(tokens[start:])
    return arguments


def _constant_integer(tokens: list[_SqlToken]) -> int | None:
    while (
        len(tokens) >= 2
        and tokens[0].kind == "symbol"
        and tokens[0].value == "("
        and tokens[-1].kind == "symbol"
        and tokens[-1].value == ")"
    ):
        tokens = tokens[1:-1]
    if len(tokens) != 1 or tokens[0].kind != "number":
        return None
    try:
        return int(tokens[0].value, 0)
    except ValueError:
        return None
