#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pyodbc>=5",
#     "azure-identity>=1.15",
#     "databricks-sql-connector>=3",
# ]
# ///
"""Run read-only SQL queries against the Geneva Azure SQL databases and
Databricks Unity Catalog (Copilot skill), with compare and shape-inspection
modes for validating data between the two.

USAGE RULE
----------
This tool is a trusted *inspection interface for evidence*: ad hoc reads,
table-shape checks, and SQL-vs-Delta comparisons during investigations and
migration validation. SQL Server / Synapse aliases provide legacy evidence
and orchestration/reference-metadata checks; Databricks aliases provide Delta
table facts from Unity Catalog. It is NOT a data-access layer — never import
it from pipeline/runtime code or build scheduled jobs on top of it.

DEPENDENCIES
------------
Three ways to get them:
  1. uv run query_geneva_db.py ...        (reads the inline metadata above,
     creates an isolated env automatically — recommended)
  2. python query_geneva_db.py --install-deps   (pip-installs into the
     current interpreter)
  3. pip install pyodbc azure-identity databricks-sql-connector
Missing packages produce a friendly error naming the exact package; only
Databricks aliases need databricks-sql-connector.

SECURITY MODEL
--------------
The keyword filter in this script is defense-in-depth ONLY. The real read-only
boundary MUST be enforced platform-side:

Azure SQL — connect as a principal that can only read. One-time setup per
database (run as an admin):

    CREATE USER [GenevaReadOnly] FROM EXTERNAL PROVIDER;   /* Entra ID group */
    ALTER ROLE db_datareader ADD MEMBER [GenevaReadOnly];

Databricks — grant the Entra ID group only USE CATALOG / USE SCHEMA / SELECT
on the relevant Unity Catalog objects:

    GRANT USE CATALOG ON CATALOG my_catalog TO `GenevaReadOnly`;
    GRANT USE SCHEMA, SELECT ON SCHEMA my_catalog.my_schema TO `GenevaReadOnly`;

Note that ApplicationIntent=ReadOnly in the SQL connection string is a routing
hint for read scale-out replicas (Premium/Business Critical/Hyperscale tiers
only) — it is NOT a security control and does not block writes on its own.

AUTH
----
Both engines authenticate via 'az login' (AzureCliCredential). For Databricks,
a personal access token in the DATABRICKS_TOKEN environment variable takes
precedence if set.

AUDIT
-----
Every execution is appended as a JSON line to
~/.copilot/skills/query_geneva_db/audit.jsonl (disable with --no-audit).
"""

import argparse
import csv
import datetime
import decimal
import importlib
import json
import os
import pathlib
import re
import shutil
import struct
import subprocess
import sys
import time
import uuid
from collections import Counter
from typing import Any, Optional

# pyodbc / azure-identity / databricks-sql-connector are imported lazily via
# _require() so that --help and --install-deps work before anything is installed.

DB_TYPES = ("SQL Server", "Synapse", "Databricks")

DB_ENVIRONMENTS = {
    # Redacted by user.
    # Examples:
    # "mid_dev": {
    #     "type": "SQL Server",
    #     "server": "myserver.database.windows.net",
    #     "database": "mydb",
    #     "description": "MID dev SQL Server",
    #     "production": False,
    # },
    # "uc_dev": {
    #     "type": "Databricks",
    #     "server_hostname": "adb-1234567890123456.7.azuredatabricks.net",
    #     "http_path": "/sql/1.0/warehouses/abc123def456",
    #     "catalog": "main",            # optional default catalog
    #     "schema": "default",          # optional default schema
    #     "description": "Unity Catalog dev warehouse",
    #     "production": False,
    # },
}

DEFAULT_MAX_ROWS = 500
DEFAULT_TIMEOUT_SECONDS = 60
AUDIT_PATH = pathlib.Path.home() / ".copilot" / "skills" / "query_geneva_db" / "audit.jsonl"

# Well-known first-party application ID for Azure Databricks (same in every tenant).
DATABRICKS_AAD_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

PIP_PACKAGES = ("pyodbc", "azure-identity", "databricks-sql-connector")


def _require(module_name: str, pip_name: str):
    """Import a third-party module, or exit with install instructions."""
    try:
        return importlib.import_module(module_name)
    except ImportError:
        print(
            f"Error: missing dependency '{pip_name}'.\n"
            f"Install it with: {sys.executable} -m pip install {pip_name}\n"
            f"Or install everything: python {pathlib.Path(__file__).name} --install-deps\n"
            f"Or run via uv (auto-installs): uv run {pathlib.Path(__file__).name} ..."
        )
        sys.exit(1)


def install_deps() -> None:
    """pip-install all dependencies into the current interpreter."""
    cmd = [sys.executable, "-m", "pip", "install", "--upgrade", *PIP_PACKAGES]
    print("Running:", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode == 0:
        print("\nAll dependencies installed.")
    sys.exit(result.returncode)


def engine_for_alias(db_alias: str) -> str:
    """Return 'databricks' or 'tsql' for a configured alias."""
    env = DB_ENVIRONMENTS.get(db_alias, {})
    return "databricks" if env.get("type") == "Databricks" else "tsql"


def get_best_driver() -> str:
    """Find the latest installed MS ODBC Driver for SQL Server (v17 or newer)."""
    pyodbc = _require("pyodbc", "pyodbc")
    drivers = pyodbc.drivers()

    versioned: list[tuple[int, str]] = []
    for d in drivers:
        match = re.search(r"ODBC Driver (\d+) for SQL Server", d)
        if match and int(match.group(1)) >= 17:
            versioned.append((int(match.group(1)), d))

    if not versioned:
        raise RuntimeError(
            f"No modern MS ODBC Driver (v17+) found in: {drivers}. "
            "Please install 'ODBC Driver 17 for SQL Server' or 'ODBC Driver 18 for SQL Server'."
        )

    return max(versioned)[1]


def _get_cli_token(scope: str) -> str:
    azure_identity = _require("azure.identity", "azure-identity")
    try:
        credential = azure_identity.AzureCliCredential()
        return credential.get_token(scope).token
    except Exception as e:
        print(f"Error: Failed to obtain Azure authentication token. Have you run 'az login'?\nDetails: {e}")
        sys.exit(1)


def _get_databricks_module():
    return _require("databricks.sql", "databricks-sql-connector")


def get_db_connection(db_alias: str, timeout_seconds: int):
    """Return a connection for the provided database alias (pyodbc or databricks).

    Read-only access is expected to be enforced platform-side; see the module
    docstring.
    """
    if db_alias not in DB_ENVIRONMENTS:
        raise ValueError(
            f"Unknown database alias '{db_alias}'. Please choose one of: {', '.join(DB_ENVIRONMENTS.keys())}"
        )

    env = DB_ENVIRONMENTS[db_alias]

    if env.get("type") == "Databricks":
        databricks_sql = _get_databricks_module()
        # Prefer an explicit PAT; otherwise reuse the 'az login' session.
        access_token = os.environ.get("DATABRICKS_TOKEN") or _get_cli_token(DATABRICKS_AAD_SCOPE)
        connect_kwargs: dict[str, Any] = {
            "server_hostname": env["server_hostname"],
            "http_path": env["http_path"],
            "access_token": access_token,
        }
        if env.get("catalog"):
            connect_kwargs["catalog"] = env["catalog"]
        if env.get("schema"):
            connect_kwargs["schema"] = env["schema"]
        return databricks_sql.connect(**connect_kwargs)

    pyodbc = _require("pyodbc", "pyodbc")

    # IMPORTANT: scope must end with /.default for Azure SQL / Synapse SQL
    database_token = _get_cli_token("https://database.windows.net/.default")

    # The ODBC driver expects the token as length-prefixed UTF-16LE bytes.
    token_bytes = database_token.encode("utf-16-le")
    token_struct = struct.pack("=i", len(token_bytes)) + token_bytes

    driver = get_best_driver()
    # Encrypt=yes always (Driver 17 defaults to unencrypted). Azure SQL's TLS
    # certificate chains to a public CA, so certificate validation stays ON —
    # do not add TrustServerCertificate=yes.
    conn_string = (
        f"Driver={{{driver}}};"
        f"SERVER={env['server']};"
        f"DATABASE={env['database']};"
        "Encrypt=yes;"
        "ApplicationIntent=ReadOnly;"
        "Connection Timeout=30;"
    )

    SQL_COPT_SS_ACCESS_TOKEN = 1256
    conn = pyodbc.connect(conn_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    conn.timeout = timeout_seconds  # per-statement timeout (SQL Server only)

    return conn


def _find_skill_file(script_dir: pathlib.Path) -> Optional[pathlib.Path]:
    for name in ("SKILL.md", "skill.md", "skills.md"):
        p = script_dir / name
        if p.exists():
            return p
    return None


def install_skill() -> None:
    """Install the SKILL.md file to the local Copilot skills directory."""
    # Find the SKILL.md relative to this script
    script_dir = pathlib.Path(__file__).parent.absolute()
    source_path = _find_skill_file(script_dir)

    if not source_path:
        print(f"Error: SKILL.md not found in {script_dir} (looked for SKILL.md / skill.md / skills.md)")
        sys.exit(1)

    # Path.home() handles both Windows and Unix
    dest_dir = pathlib.Path.home() / ".copilot" / "skills" / "query_geneva_db"
    dest_path = dest_dir / "SKILL.md"

    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest_path)
        print(f"Successfully installed skill to: {dest_path}")
    except Exception as e:
        print(f"Error installing skill: {e}")
        sys.exit(1)


def check_skill_status() -> str:
    """Check if the local SKILL.md is newer than the installed one."""
    script_dir = pathlib.Path(__file__).parent.absolute()
    source_path = _find_skill_file(script_dir)
    dest_path = pathlib.Path.home() / ".copilot" / "skills" / "query_geneva_db" / "SKILL.md"

    if not source_path:
        return ""

    if not dest_path.exists():
        return "\nNOTE: Copilot skill is not installed. Run with --skill or --install-skill to install it."

    if source_path.stat().st_mtime > dest_path.stat().st_mtime:
        return "\nWARNING: A newer version of SKILL.md is available. Run with --skill or --install-skill to update it."

    return ""


def format_alias_descriptions() -> str:
    grouped_aliases: dict[str, list[str]] = {db_type: [] for db_type in DB_TYPES}
    for alias, info in DB_ENVIRONMENTS.items():
        db_type = info.get("type", "SQL Server")
        grouped_aliases.setdefault(db_type, []).append(f"      {alias}: {info.get('description', '')}")

    sections: list[str] = []
    for db_type, items in grouped_aliases.items():
        if not items:
            continue
        sections.append(f"  {db_type}:")
        sections.extend(items)

    return "\n".join(sections)


def build_arg_parser() -> argparse.ArgumentParser:
    skill_status = check_skill_status()
    parser = argparse.ArgumentParser(
        description="Run a read-only SQL query against the Geneva databases (Azure SQL or Databricks Unity Catalog).",
        epilog=skill_status,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--install-skill",
        "--skill",
        action="store_true",
        help="Install the Copilot skill file (SKILL.md) to the user's home directory.",
    )
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help=f"pip-install the required packages ({', '.join(PIP_PACKAGES)}) into the current interpreter.",
    )

    # Copilot-friendly ways to pass long SQL
    parser.add_argument(
        "--query-file",
        "-f",
        type=str,
        help="Read the SQL query from a file path instead of the positional 'query' argument.",
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read the SQL query from STDIN instead of the positional 'query' argument.",
    )
    parser.add_argument(
        "--show-query",
        action="store_true",
        help="Print the sanitized SQL that will be executed.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=DEFAULT_MAX_ROWS,
        help=f"Maximum rows to fetch/print (default {DEFAULT_MAX_ROWS}; 0 = unlimited).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-statement timeout in seconds, SQL Server aliases only (default {DEFAULT_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--format",
        choices=("table", "csv", "json"),
        default="table",
        help="Output format: table (tab-separated, default), csv, or json.",
    )
    parser.add_argument(
        "--plan",
        choices=("estimated", "actual"),
        help=(
            "Capture the execution plan.\n"
            "  SQL Server — estimated: plan XML only, query NOT executed;\n"
            "               actual: runs the query, prints results AND the plan XML.\n"
            "  Databricks — estimated: EXPLAIN FORMATTED output, query NOT executed;\n"
            "               actual: runs the query; see Query History in the UI for the profile.\n"
            "Paste SQL Server plan XML into the FixSQL Copilot Space for optimization."
        ),
    )
    parser.add_argument(
        "--compare",
        metavar="SECOND_ALIAS",
        choices=list(DB_ENVIRONMENTS.keys()),
        help=(
            "Run the query against db_alias AND this second alias, then diff the\n"
            "result sets (order-insensitive, values normalized across engines).\n"
            "Use --query2/--query-file2 when the second engine needs different\n"
            "syntax. Exit code 2 when differences are found."
        ),
    )
    parser.add_argument(
        "--query2",
        type=str,
        help="Query to run against the --compare alias (defaults to the primary query).",
    )
    parser.add_argument(
        "--query-file2",
        type=str,
        help="Read the --compare query from a file path.",
    )
    parser.add_argument(
        "--describe",
        metavar="TABLE",
        type=str,
        help=(
            "Inspect a table's shape (columns, types, nullability, row count)\n"
            "instead of running a query. SQL Server: [schema.]table;\n"
            "Databricks: [catalog.][schema.]table (alias defaults apply).\n"
            "Combine with --compare [--describe2 TABLE2] to diff shapes across\n"
            "engines. Exit code 2 when shapes differ."
        ),
    )
    parser.add_argument(
        "--describe2",
        metavar="TABLE",
        type=str,
        help="Table to describe on the --compare alias (defaults to the --describe table).",
    )
    parser.add_argument(
        "--allow-prod",
        action="store_true",
        help="Required to run against a production database alias.",
    )
    parser.add_argument(
        "--no-audit",
        action="store_true",
        help="Skip writing the local audit log entry.",
    )

    alias_descriptions = format_alias_descriptions()
    parser.add_argument(
        "db_alias",
        nargs="?",
        choices=list(DB_ENVIRONMENTS.keys()),
        help=(f"Select the database alias to connect to:\n{alias_descriptions}\n\n"),
    )
    parser.add_argument("query", nargs="?", help="The SQL query to execute (or omit if using --query-file/--stdin)")
    return parser


_CODE_FENCE_RE = re.compile(
    r"```(?:sql|tsql|t-sql)?\s*(.*?)\s*```",
    flags=re.IGNORECASE | re.DOTALL,
)
_TILDE_FENCE_RE = re.compile(
    r"~~~(?:sql|tsql|t-sql)?\s*(.*?)\s*~~~",
    flags=re.IGNORECASE | re.DOTALL,
)


def extract_code_block_if_present(text: str) -> str:
    """If Copilot returns ```sql ... ```, extract the SQL inside."""
    m = _CODE_FENCE_RE.search(text)
    if m:
        return m.group(1).strip()
    m = _TILDE_FENCE_RE.search(text)
    if m:
        return m.group(1).strip()
    return text


def strip_go_lines(text: str) -> str:
    """Remove SSMS GO batch separators."""
    return re.sub(r"(?im)^\s*GO\s*;?\s*$", "", text)


def strip_sql_comments(sql: str) -> str:
    """
    Strip -- and /* */ comments WITHOUT breaking strings/bracketed identifiers.
    """
    out: list[str] = []
    i = 0
    n = len(sql)

    in_single = False
    in_double = False
    in_bracket = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append(ch)
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if in_single:
            out.append(ch)
            if ch == "'":
                # handle escaped ''
                if nxt == "'":
                    out.append(nxt)
                    i += 2
                    continue
                in_single = False
            i += 1
            continue

        if in_double:
            out.append(ch)
            if ch == '"':
                if nxt == '"':
                    out.append(nxt)
                    i += 2
                    continue
                in_double = False
            i += 1
            continue

        if in_bracket:
            out.append(ch)
            if ch == "]":
                if nxt == "]":
                    out.append(nxt)
                    i += 2
                    continue
                in_bracket = False
            i += 1
            continue

        if in_backtick:
            out.append(ch)
            if ch == "`":
                if nxt == "`":
                    out.append(nxt)
                    i += 2
                    continue
                in_backtick = False
            i += 1
            continue

        # Enter safe zones
        if ch == "'":
            in_single = True
            out.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            out.append(ch)
            i += 1
            continue
        if ch == "[":
            in_bracket = True
            out.append(ch)
            i += 1
            continue
        if ch == "`":
            in_backtick = True
            out.append(ch)
            i += 1
            continue

        # Comment start
        if ch == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def extract_sql_block(text: str) -> str:
    """
    If user/Copilot includes preamble text, extract from the first *real* SQL block.
    A SELECT ... FROM ..., a WITH at line start, or a SHOW/DESCRIBE at line start
    all count; whichever appears first wins (so a CTE is never sliced at its
    inner SELECT).
    """
    stripped = text.strip()

    # Already starts with a statement — nothing to extract.
    if re.match(r"(?i)^;?\s*(SELECT|WITH|SHOW|DESCRIBE|DESC)\b", stripped):
        return stripped

    upper = text.upper()
    candidates: list[int] = []

    # First SELECT that has a FROM after it is usually a real query
    for m in re.finditer(r"\bSELECT\b", upper):
        if re.search(r"\bFROM\b", upper[m.end():]):
            candidates.append(m.start())
            break

    # CTE or metadata statement starting at line start (allow ;WITH)
    m_stmt = re.search(r"(?im)^\s*;?\s*(WITH|SHOW|DESCRIBE|DESC)\b", text)
    if m_stmt:
        candidates.append(m_stmt.start())

    if candidates:
        return text[min(candidates):].strip()

    return stripped


def sanitize_query(raw: str) -> str:
    """Normalize Copilot/Chat output into executable SQL."""
    q = (raw or "").lstrip("\ufeff").strip()
    q = extract_code_block_if_present(q)
    q = strip_go_lines(q)
    q = extract_sql_block(q)
    q = strip_sql_comments(q)
    q = strip_go_lines(q)
    return q.strip()


def _mask_safe_zones(query_body: str) -> str:
    """Blank out strings and quoted identifiers ([], "", ``) for keyword scanning."""
    pattern = (
        r"('[^']*(?:''[^']*)*('|$))"
        r"|(\[[^\]]*(\]|$))"
        r"|(\"[^\"]*(\"|$))"
        r"|(`[^`]*(?:``[^`]*)*(`|$))"
    )
    return re.sub(pattern, " ", query_body)


def _check_single_select(masked: str, statement_already_open: bool = False) -> Optional[str]:
    """
    Detect semicolon-less batches: a second SELECT at parenthesis depth 0 that
    is not part of a set operation (UNION [ALL] / EXCEPT / INTERSECT) means a
    second statement. Returns an error message or None.
    """
    depth = 0
    seen_select = statement_already_open
    last_word = ""
    prev_word = ""

    for m in re.finditer(r"\(|\)|\b[A-Za-z_][A-Za-z0-9_]*\b", masked):
        tok = m.group(0)
        if tok == "(":
            depth += 1
            continue
        if tok == ")":
            depth = max(0, depth - 1)
            continue

        word = tok.upper()
        if depth == 0 and word == "SELECT":
            if seen_select:
                set_op = last_word in ("UNION", "EXCEPT", "INTERSECT") or (
                    last_word == "ALL" and prev_word == "UNION"
                )
                if not set_op:
                    return (
                        "Error: Multiple SQL statements detected (a second SELECT outside "
                        "UNION/EXCEPT/INTERSECT). Only a single statement is allowed."
                    )
            seen_select = True

        prev_word = last_word
        last_word = word

    return None


# DECLARE/SET/WAITFOR catch semicolon-less extra statements; OPENROWSET/
# OPENQUERY/OPENDATASOURCE block ad hoc remote access; the rest are writes,
# permission changes, or server-level commands.
_BASE_FORBIDDEN = (
    "DELETE|UPDATE|INSERT|DROP|ALTER|TRUNCATE|MERGE|GRANT|REVOKE|DENY"
    "|EXEC|EXECUTE|CREATE|INTO|DECLARE|SET|WAITFOR"
    "|OPENROWSET|OPENQUERY|OPENDATASOURCE"
    "|DBCC|BACKUP|RESTORE|KILL|SHUTDOWN|RECONFIGURE"
)
# Databricks extras: Delta/UC maintenance and ingestion commands.
_DATABRICKS_FORBIDDEN = (
    _BASE_FORBIDDEN
    + "|COPY|VACUUM|OPTIMIZE|REFRESH|CALL|MSCK|CACHE|UNCACHE|CLONE|UNDROP|ANALYZE|FSCK"
)

FORBIDDEN_BY_ENGINE = {
    "tsql": rf"\b({_BASE_FORBIDDEN})\b",
    "databricks": rf"\b({_DATABRICKS_FORBIDDEN})\b",
}

# Databricks additionally allows read-only metadata statements.
ALLOWED_STARTERS = {
    "tsql": ("SELECT", "WITH"),
    "databricks": ("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC"),
}


def enforce_query_safety(query_body: str, engine: str = "tsql") -> None:
    """
    Enforce safety rules on the query. This is defense-in-depth — the real
    read-only boundary is platform-side permissions (see module docstring).

    1. Must start with an allowed read statement (SELECT/WITH; on Databricks
       also SHOW/DESCRIBE). A leading ';' is allowed for ';WITH'.
    2. No multiple statements (semicolon check + depth-0 SELECT scan).
    3. No forbidden keywords outside of identifiers/strings.
    """
    normalized = query_body.lstrip()
    normalized_for_start = re.sub(r"^;+\s*", "", normalized)  # allow ;WITH
    upper_query = normalized_for_start.upper()

    starters = ALLOWED_STARTERS.get(engine, ALLOWED_STARTERS["tsql"])
    starter = next((s for s in starters if upper_query.startswith(s)), None)
    if starter is None:
        allowed = "/".join(starters)
        print(
            f"Error: Only read statements ({allowed}) are allowed.\n"
            "If you entered natural language, ask Copilot to generate SQL and rerun.\n"
            "If you pasted a ```sql fenced block, that's ok — but ensure it contains a read statement."
        )
        sys.exit(1)

    check_query = _mask_safe_zones(query_body)

    # Multiple statements check:
    # - ignore leading semicolons (for ;WITH)
    # - ignore trailing semicolons/whitespace
    check_query = re.sub(r"^\s*;+\s*", "", check_query)
    stripped_check = re.sub(r"[;\s]+$", "", check_query)
    if ";" in stripped_check:
        print("Error: Multiple SQL statements detected. Only a single statement is allowed.")
        sys.exit(1)

    multi_error = _check_single_select(
        stripped_check,
        statement_already_open=starter in ("SHOW", "DESCRIBE", "DESC"),
    )
    if multi_error:
        print(multi_error)
        sys.exit(1)

    match = re.search(FORBIDDEN_BY_ENGINE[engine], check_query.upper())
    if match:
        print(f"Error: Forbidden keyword '{match.group(1)}' detected.")
        sys.exit(1)


def _jsonable(value: Any) -> Any:
    """Convert DB values to JSON-safe representations."""
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


def _canon(value: Any) -> str:
    """Engine-neutral canonical form of a value, for cross-engine comparison."""
    if value is None:
        return "<NULL>"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, decimal.Decimal)):
        try:
            return str(decimal.Decimal(str(value)).normalize())
        except decimal.InvalidOperation:
            return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


def _table_cell(value: Any) -> str:
    """Render a value for tab-separated output without corrupting the table."""
    if value is None:
        return "NULL"
    text = str(_jsonable(value))
    return text.replace("\t", "\\t").replace("\r", "\\r").replace("\n", "\\n")


def _fetch_rows(cursor, max_rows: int) -> tuple[list[str], list[tuple], bool]:
    """Fetch up to max_rows from the current result set. Returns (columns, rows, truncated)."""
    columns = [column[0] for column in cursor.description]
    truncated = False
    if max_rows and max_rows > 0:
        rows = [tuple(r) for r in cursor.fetchmany(max_rows)]
        if cursor.fetchone() is not None:
            truncated = True
    else:
        rows = [tuple(r) for r in cursor.fetchall()]
    return columns, rows, truncated


def _print_result_set(cursor, max_rows: int, fmt: str) -> tuple[int, bool]:
    """Print the current result set. Returns (row_count, truncated)."""
    columns, rows, truncated = _fetch_rows(cursor, max_rows)

    if fmt == "json":
        payload = {
            "columns": columns,
            "rows": [[_jsonable(v) for v in row] for row in rows],
            "row_count": len(rows),
            "truncated": truncated,
        }
        print(json.dumps(payload, ensure_ascii=False))
    elif fmt == "csv":
        writer = csv.writer(sys.stdout, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow(["" if v is None else _jsonable(v) for v in row])
        if truncated:
            print(f"-- truncated at {len(rows)} rows (use --max-rows to raise the cap) --")
    else:  # table
        print("\t".join(columns))
        for row in rows:
            print("\t".join(_table_cell(v) for v in row))
        if truncated:
            print(f"-- truncated at {len(rows)} rows (use --max-rows to raise the cap) --")

    return len(rows), truncated


_PLAN_HINT = "Tip: paste this XML into the FixSQL Copilot Space for an evidence-based optimization."


def _azure_error_hint(message: str) -> str:
    lowered = message.lower()
    if "40615" in message or "not allowed to access the server" in lowered:
        return (
            "Hint: your client IP is blocked by the server firewall. Add it in the Azure portal "
            "(SQL server -> Networking) or via: az sql server firewall-rule create"
        )
    if "token-identified principal" in lowered:
        return (
            "Hint: your Entra ID identity has no user in this database. An admin must run:\n"
            "  CREATE USER [you@yourdomain] FROM EXTERNAL PROVIDER;\n"
            "  ALTER ROLE db_datareader ADD MEMBER [you@yourdomain];"
        )
    if "permission" in lowered and ("use catalog" in lowered or "use schema" in lowered or "select" in lowered):
        return (
            "Hint: your identity lacks Unity Catalog privileges. An admin must run:\n"
            "  GRANT USE CATALOG ON CATALOG <catalog> TO `<group>`;\n"
            "  GRANT USE SCHEMA, SELECT ON SCHEMA <catalog>.<schema> TO `<group>`;"
        )
    if "timeout" in lowered:
        return "Hint: the query or login timed out. Raise --timeout, or check the query with --plan estimated first."
    return ""


def write_audit(record: dict) -> None:
    try:
        AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with AUDIT_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Warning: could not write audit log ({e})", file=sys.stderr)


def _execute_tsql(cursor, query: str, max_rows: int, fmt: str, plan: Optional[str], info: dict) -> None:
    if plan == "estimated":
        # SHOWPLAN_XML returns the plan as a result set; the query itself is NOT executed.
        cursor.execute("SET SHOWPLAN_XML ON")
        try:
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                print(row[0])
                print(f"\n{_PLAN_HINT}")
        finally:
            try:
                cursor.execute("SET SHOWPLAN_XML OFF")
            except Exception:
                pass
    elif plan == "actual":
        cursor.execute("SET STATISTICS XML ON")
        try:
            cursor.execute(query)
            plan_xml = None
            while True:
                if cursor.description:
                    columns = [c[0] for c in cursor.description]
                    if len(columns) == 1 and "showplan" in columns[0].lower():
                        row = cursor.fetchone()
                        if row:
                            plan_xml = row[0]
                    else:
                        rows, truncated = _print_result_set(cursor, max_rows, fmt)
                        info["rows"] += rows
                        info["truncated"] = info["truncated"] or truncated
                if not cursor.nextset():
                    break
            if plan_xml:
                print("\n-- actual execution plan XML --")
                print(plan_xml)
                print(f"\n{_PLAN_HINT}")
        finally:
            try:
                cursor.execute("SET STATISTICS XML OFF")
            except Exception:
                pass
    else:
        cursor.execute(query)
        if cursor.description:
            rows, truncated = _print_result_set(cursor, max_rows, fmt)
            info["rows"] = rows
            info["truncated"] = truncated
        else:
            print("Query executed successfully, but returned no result set.")


def _execute_databricks(cursor, query: str, max_rows: int, fmt: str, plan: Optional[str], info: dict) -> None:
    if plan == "estimated":
        # EXPLAIN does not execute the query.
        cursor.execute(f"EXPLAIN FORMATTED {query.rstrip().rstrip(';')}")
        for row in cursor.fetchall():
            print(row[0])
        return

    cursor.execute(query)
    if cursor.description:
        rows, truncated = _print_result_set(cursor, max_rows, fmt)
        info["rows"] = rows
        info["truncated"] = truncated
    else:
        print("Query executed successfully, but returned no result set.")

    if plan == "actual":
        print(
            "\nNote: Databricks has no inline actual-plan capture; open Query History "
            "in the Databricks UI for this query's execution profile.",
            file=sys.stderr,
        )


def execute_query(
    db_alias: str,
    query: str,
    max_rows: int,
    fmt: str,
    timeout_seconds: int,
    plan: Optional[str],
) -> dict:
    """Execute the query (and/or capture its plan). Returns audit info."""
    engine = engine_for_alias(db_alias)
    info: dict = {"rows": 0, "truncated": False, "plan": plan or "", "engine": engine}
    started = time.perf_counter()

    try:
        with get_db_connection(db_alias, timeout_seconds) as conn:
            with conn.cursor() as cursor:
                if engine == "databricks":
                    _execute_databricks(cursor, query, max_rows, fmt, plan, info)
                else:
                    _execute_tsql(cursor, query, max_rows, fmt, plan, info)

        info["duration_ms"] = round((time.perf_counter() - started) * 1000)
        info["status"] = "ok"
        print(
            f"\n-- {info['rows']} row(s){' (truncated)' if info['truncated'] else ''} "
            f"in {info['duration_ms']} ms --",
            file=sys.stderr,
        )
        return info
    except Exception as e:
        info["duration_ms"] = round((time.perf_counter() - started) * 1000)
        info["status"] = "error"
        info["error"] = str(e)
        print(f"Error executing query: {e}")
        hint = _azure_error_hint(str(e))
        if hint:
            print(hint)
        return info


def _diff_rows(rows_a: list[tuple], rows_b: list[tuple]) -> tuple[Counter, Counter]:
    """Multiset diff of canonicalized rows. Returns (only_in_a, only_in_b)."""
    ca = Counter(tuple(_canon(v) for v in row) for row in rows_a)
    cb = Counter(tuple(_canon(v) for v in row) for row in rows_b)
    return ca - cb, cb - ca


def run_compare(
    alias_a: str,
    query_a: str,
    alias_b: str,
    query_b: str,
    max_rows: int,
    fmt: str,
    timeout_seconds: int,
) -> dict:
    """Run both queries and diff the result sets. Returns audit info."""
    info: dict = {"compare_with": alias_b, "status": "error"}
    started = time.perf_counter()
    show_limit = 20

    def fetch(alias: str, query: str) -> tuple[list[str], list[tuple], bool]:
        with get_db_connection(alias, timeout_seconds) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                if not cursor.description:
                    raise RuntimeError(f"Query against '{alias}' returned no result set.")
                return _fetch_rows(cursor, max_rows)

    try:
        cols_a, rows_a, trunc_a = fetch(alias_a, query_a)
        cols_b, rows_b, trunc_b = fetch(alias_b, query_b)
    except Exception as e:
        info["duration_ms"] = round((time.perf_counter() - started) * 1000)
        info["error"] = str(e)
        print(f"Error executing comparison: {e}")
        hint = _azure_error_hint(str(e))
        if hint:
            print(hint)
        return info

    column_mismatch = len(cols_a) != len(cols_b)
    names_differ = [c.lower() for c in cols_a] != [c.lower() for c in cols_b]

    only_a: Counter = Counter()
    only_b: Counter = Counter()
    if not column_mismatch:
        only_a, only_b = _diff_rows(rows_a, rows_b)

    diff_count = sum(only_a.values()) + sum(only_b.values())
    matched = len(rows_a) - sum(only_a.values())
    truncated = trunc_a or trunc_b

    info.update(
        {
            "status": "ok",
            "rows": len(rows_a) + len(rows_b),
            "rows_a": len(rows_a),
            "rows_b": len(rows_b),
            "only_in_a": sum(only_a.values()),
            "only_in_b": sum(only_b.values()),
            "column_mismatch": column_mismatch,
            "truncated": truncated,
            "duration_ms": round((time.perf_counter() - started) * 1000),
        }
    )

    if fmt == "json":
        payload = {
            **{k: info[k] for k in ("rows_a", "rows_b", "only_in_a", "only_in_b", "column_mismatch", "truncated")},
            "alias_a": alias_a,
            "alias_b": alias_b,
            "columns_a": cols_a,
            "columns_b": cols_b,
            "matched_rows": matched,
            "sample_only_in_a": [list(r) for r, _ in only_a.most_common(show_limit)],
            "sample_only_in_b": [list(r) for r, _ in only_b.most_common(show_limit)],
        }
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"-- compare: {alias_a} vs {alias_b} --")
        print(f"{alias_a}: {len(rows_a)} row(s){' (truncated)' if trunc_a else ''}, columns: {', '.join(cols_a)}")
        print(f"{alias_b}: {len(rows_b)} row(s){' (truncated)' if trunc_b else ''}, columns: {', '.join(cols_b)}")
        if column_mismatch:
            print("MISMATCH: result sets have different column counts — row comparison skipped.")
        else:
            if names_differ:
                print("Note: column names differ; rows compared positionally.")
            print(f"matching rows: {matched}")
            print(f"only in {alias_a}: {sum(only_a.values())}")
            for row, count in only_a.most_common(show_limit):
                print("  " + "\t".join(row) + (f"  (x{count})" if count > 1 else ""))
            print(f"only in {alias_b}: {sum(only_b.values())}")
            for row, count in only_b.most_common(show_limit):
                print("  " + "\t".join(row) + (f"  (x{count})" if count > 1 else ""))
        if truncated:
            print("WARNING: one or both result sets were truncated by --max-rows; the diff may be incomplete.")

    if column_mismatch or diff_count > 0:
        info["exit_code"] = 2
    return info


# --- Table shape inspection (Unity Catalog / INFORMATION_SCHEMA) -----------

def _split_table_ref(name: str) -> list[str]:
    """Split a dotted table reference, respecting [bracket] and `backtick` quoting."""
    parts: list[str] = []
    cur = ""
    i = 0
    in_br = in_bt = False
    while i < len(name):
        ch = name[i]
        nxt = name[i + 1] if i + 1 < len(name) else ""
        if in_br:
            if ch == "]":
                if nxt == "]":
                    cur += "]"
                    i += 2
                    continue
                in_br = False
            else:
                cur += ch
            i += 1
            continue
        if in_bt:
            if ch == "`":
                if nxt == "`":
                    cur += "`"
                    i += 2
                    continue
                in_bt = False
            else:
                cur += ch
            i += 1
            continue
        if ch == "[":
            in_br = True
        elif ch == "`":
            in_bt = True
        elif ch == ".":
            parts.append(cur)
            cur = ""
        else:
            cur += ch
        i += 1
    parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def _bracket(s: str) -> str:
    return "[" + s.replace("]", "]]") + "]"


def _backtick(s: str) -> str:
    return "`" + s.replace("`", "``") + "`"


def _sq(s: str) -> str:
    return s.replace("'", "''")


# Engine type -> engine-neutral bucket, for cross-engine shape comparison.
_TSQL_TYPE_BUCKETS = {
    "bit": "boolean",
    "tinyint": "integer", "smallint": "integer", "int": "integer", "bigint": "integer",
    "decimal": "decimal", "numeric": "decimal", "money": "decimal", "smallmoney": "decimal",
    "float": "float", "real": "float",
    "date": "date", "time": "time",
    "datetime": "timestamp", "datetime2": "timestamp", "smalldatetime": "timestamp",
    "datetimeoffset": "timestamp",
    "char": "string", "varchar": "string", "nchar": "string", "nvarchar": "string",
    "text": "string", "ntext": "string", "xml": "string", "uniqueidentifier": "string",
    "binary": "binary", "varbinary": "binary", "image": "binary",
}
_DBX_TYPE_BUCKETS = {
    "boolean": "boolean",
    "tinyint": "integer", "smallint": "integer", "int": "integer", "integer": "integer",
    "bigint": "integer", "byte": "integer", "short": "integer", "long": "integer",
    "decimal": "decimal",
    "float": "float", "double": "float",
    "date": "date",
    "timestamp": "timestamp", "timestamp_ntz": "timestamp",
    "string": "string", "varchar": "string", "char": "string",
    "binary": "binary",
    "array": "complex", "map": "complex", "struct": "complex", "variant": "complex",
}


def _normalize_type(engine: str, raw_type: str) -> str:
    base = raw_type.lower().split("(")[0].split("<")[0].strip()
    buckets = _DBX_TYPE_BUCKETS if engine == "databricks" else _TSQL_TYPE_BUCKETS
    return buckets.get(base, "other")


def _tsql_display_type(data_type: str, char_len, precision, scale) -> str:
    dt = (data_type or "").lower()
    if dt in ("char", "varchar", "nchar", "nvarchar", "binary", "varbinary") and char_len is not None:
        return f"{dt}({'max' if char_len == -1 else char_len})"
    if dt in ("decimal", "numeric") and precision is not None:
        return f"{dt}({precision},{scale or 0})"
    return dt


def _fetch_shape_tsql(cursor, alias: str, table_ref: str, parts: list[str]) -> dict:
    if len(parts) > 2:
        parts = parts[-2:]  # connection is already scoped to one database
    schema = parts[0] if len(parts) == 2 else None
    table = parts[-1]

    sql = (
        "SELECT TABLE_SCHEMA, ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
        "CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
    )
    params: list = [table]
    if schema:
        sql += " AND TABLE_SCHEMA = ?"
        params.append(schema)
    sql += " ORDER BY TABLE_SCHEMA, ORDINAL_POSITION"
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f"Table '{table_ref}' not found on '{alias}'.")
    schemas = sorted({r[0] for r in rows})
    if len(schemas) > 1:
        raise RuntimeError(
            f"Table name '{table}' is ambiguous on '{alias}' (schemas: {', '.join(schemas)}). "
            "Qualify it as schema.table."
        )
    schema = schemas[0]

    columns = [
        {
            "ordinal": int(r[1]),
            "name": r[2],
            "type": _tsql_display_type(r[3], r[5], r[6], r[7]),
            "normalized": _normalize_type("tsql", r[3]),
            "nullable": str(r[4]).upper() == "YES",
        }
        for r in rows
    ]

    cursor.execute(f"SELECT COUNT_BIG(*) FROM {_bracket(schema)}.{_bracket(table)}")
    row_count = int(cursor.fetchone()[0])

    return {
        "alias": alias,
        "engine": "tsql",
        "table": f"{schema}.{table}",
        "columns": columns,
        "row_count": row_count,
    }


def _fetch_shape_databricks(cursor, alias: str, table_ref: str, parts: list[str], env: dict) -> dict:
    catalog = schema = None
    if len(parts) == 3:
        catalog, schema, table = parts
    elif len(parts) == 2:
        schema, table = parts
    else:
        table = parts[0]
    catalog = catalog or env.get("catalog")
    schema = schema or env.get("schema")
    if not schema:
        raise RuntimeError(
            f"No schema in '{table_ref}' and no default schema configured for '{alias}'. "
            "Use catalog.schema.table."
        )

    prefix = f"{_backtick(catalog)}." if catalog else ""
    cursor.execute(
        f"SELECT ordinal_position, column_name, full_data_type, is_nullable "
        f"FROM {prefix}information_schema.columns "
        f"WHERE lower(table_schema) = lower('{_sq(schema)}') "
        f"AND lower(table_name) = lower('{_sq(table)}') "
        f"ORDER BY ordinal_position"
    )
    rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f"Table '{table_ref}' not found on '{alias}'.")

    columns = [
        {
            "ordinal": int(r[0]),
            "name": r[1],
            "type": str(r[2]).lower(),
            "normalized": _normalize_type("databricks", str(r[2])),
            "nullable": str(r[3]).upper() == "YES",
        }
        for r in rows
    ]

    fq_parts = ([catalog] if catalog else []) + [schema, table]
    fq = ".".join(_backtick(p) for p in fq_parts)
    cursor.execute(f"SELECT COUNT(*) FROM {fq}")
    row_count = int(cursor.fetchone()[0])

    return {
        "alias": alias,
        "engine": "databricks",
        "table": ".".join(fq_parts),
        "columns": columns,
        "row_count": row_count,
    }


def fetch_shape(db_alias: str, table_ref: str, timeout_seconds: int) -> dict:
    """Return the normalized shape of a table on either engine."""
    if re.search(r";|--|/\*", table_ref):
        raise RuntimeError(f"Invalid table reference: {table_ref!r}")
    parts = _split_table_ref(table_ref)
    if not parts:
        raise RuntimeError(f"Invalid table reference: {table_ref!r}")

    engine = engine_for_alias(db_alias)
    with get_db_connection(db_alias, timeout_seconds) as conn:
        with conn.cursor() as cursor:
            if engine == "databricks":
                return _fetch_shape_databricks(cursor, db_alias, table_ref, parts, DB_ENVIRONMENTS[db_alias])
            return _fetch_shape_tsql(cursor, db_alias, table_ref, parts)


def _diff_shapes(a: dict, b: dict) -> dict:
    """Structural diff of two table shapes (columns matched by name, case-insensitive)."""
    cols_a = {c["name"].lower(): c for c in a["columns"]}
    cols_b = {c["name"].lower(): c for c in b["columns"]}
    only_a = sorted(set(cols_a) - set(cols_b))
    only_b = sorted(set(cols_b) - set(cols_a))

    type_mismatches = []
    nullability_mismatches = []
    for name in sorted(set(cols_a) & set(cols_b)):
        ca, cb = cols_a[name], cols_b[name]
        if ca["normalized"] != cb["normalized"]:
            type_mismatches.append(
                {"column": ca["name"], "a": f"{ca['type']} ({ca['normalized']})", "b": f"{cb['type']} ({cb['normalized']})"}
            )
        elif ca["nullable"] != cb["nullable"]:
            nullability_mismatches.append(
                {"column": ca["name"], "a": "NULL" if ca["nullable"] else "NOT NULL", "b": "NULL" if cb["nullable"] else "NOT NULL"}
            )

    row_count_diff = a["row_count"] - b["row_count"]
    structure_match = not (only_a or only_b or type_mismatches or nullability_mismatches)
    return {
        "only_in_a": only_a,
        "only_in_b": only_b,
        "type_mismatches": type_mismatches,
        "nullability_mismatches": nullability_mismatches,
        "row_count_diff": row_count_diff,
        "structure_match": structure_match,
        "identical": structure_match and row_count_diff == 0,
    }


def _print_shape(shape: dict, fmt: str) -> None:
    if fmt == "json":
        print(json.dumps(shape, ensure_ascii=False))
        return
    if fmt == "csv":
        writer = csv.writer(sys.stdout, lineterminator="\n")
        writer.writerow(["ordinal", "column", "type", "normalized", "nullable"])
        for c in shape["columns"]:
            writer.writerow([c["ordinal"], c["name"], c["type"], c["normalized"], "YES" if c["nullable"] else "NO"])
    else:
        print(f"-- shape: {shape['alias']} {shape['table']} --")
        print("ordinal\tcolumn\ttype\tnormalized\tnullable")
        for c in shape["columns"]:
            print(f"{c['ordinal']}\t{c['name']}\t{c['type']}\t{c['normalized']}\t{'YES' if c['nullable'] else 'NO'}")
    print(f"-- row_count: {shape['row_count']} --")


def _print_shape_compare(shape_a: dict, shape_b: dict, diff: dict, fmt: str) -> None:
    if fmt == "json":
        print(json.dumps({"a": shape_a, "b": shape_b, "diff": diff}, ensure_ascii=False))
        return
    print(f"-- shape compare: {shape_a['alias']} {shape_a['table']} vs {shape_b['alias']} {shape_b['table']} --")
    print(f"columns: {len(shape_a['columns'])} vs {len(shape_b['columns'])}")
    print(f"row counts: {shape_a['row_count']} vs {shape_b['row_count']} (diff {diff['row_count_diff']})")
    if diff["only_in_a"]:
        print(f"only in {shape_a['alias']}: {', '.join(diff['only_in_a'])}")
    if diff["only_in_b"]:
        print(f"only in {shape_b['alias']}: {', '.join(diff['only_in_b'])}")
    for tm in diff["type_mismatches"]:
        print(f"type mismatch: {tm['column']}: {tm['a']} vs {tm['b']}")
    for nm in diff["nullability_mismatches"]:
        print(f"nullability mismatch: {nm['column']}: {nm['a']} vs {nm['b']}")
    if diff["identical"]:
        print("SHAPES MATCH (structure and row counts)")
    elif diff["structure_match"]:
        print("STRUCTURE MATCHES, row counts differ")
    else:
        print("SHAPES DIFFER")


def run_describe(args) -> dict:
    """Describe a table's shape; with --compare, diff shapes across aliases."""
    info: dict = {"op": "describe", "table": args.describe, "status": "error"}
    started = time.perf_counter()
    try:
        shape_a = fetch_shape(args.db_alias, args.describe, args.timeout)
        if args.compare:
            table_b = args.describe2 or args.describe
            shape_b = fetch_shape(args.compare, table_b, args.timeout)
            diff = _diff_shapes(shape_a, shape_b)
            _print_shape_compare(shape_a, shape_b, diff, args.format)
            info.update(
                {
                    "compare_with": args.compare,
                    "table2": table_b,
                    "structure_match": diff["structure_match"],
                    "row_count_diff": diff["row_count_diff"],
                }
            )
            if not diff["identical"]:
                info["exit_code"] = 2
        else:
            _print_shape(shape_a, args.format)
            info["row_count"] = shape_a["row_count"]
        info["status"] = "ok"
        info["duration_ms"] = round((time.perf_counter() - started) * 1000)
        return info
    except Exception as e:
        info["duration_ms"] = round((time.perf_counter() - started) * 1000)
        info["error"] = str(e)
        print(f"Error describing table: {e}")
        hint = _azure_error_hint(str(e))
        if hint:
            print(hint)
        return info


def main() -> None:
    """Parse arguments, validate the query, and run it against the selected database(s)."""
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.install_deps:
        if args.db_alias or args.query or args.query_file or args.stdin or args.install_skill:
            parser.error("--install-deps cannot be used with other arguments.")
        install_deps()

    if args.install_skill:
        if args.db_alias or args.query or args.query_file or args.stdin:
            parser.error("--install-skill/--skill cannot be used with other arguments.")
        install_skill()
        sys.exit(0)

    if not args.db_alias:
        parser.print_help()
        sys.exit(1)

    if args.compare and args.plan:
        parser.error("--compare cannot be combined with --plan.")
    if (args.query2 or args.query_file2) and not args.compare:
        parser.error("--query2/--query-file2 require --compare.")
    if args.describe and (args.query or args.query_file or args.stdin or args.plan or args.query2 or args.query_file2):
        parser.error("--describe cannot be combined with a query, --plan, or --query2/--query-file2.")
    if args.describe2 and not (args.describe and args.compare):
        parser.error("--describe2 requires --describe and --compare.")

    raw_query = None
    if args.describe:
        pass
    elif args.stdin:
        raw_query = sys.stdin.read()
    elif args.query_file:
        try:
            raw_query = pathlib.Path(args.query_file).read_text(encoding="utf-8")
        except OSError as e:
            print(f"Error: could not read query file '{args.query_file}': {e}")
            sys.exit(1)
    else:
        raw_query = args.query

    if not raw_query and not args.describe:
        parser.print_help()
        sys.exit(1)

    raw_query2 = None
    if args.query_file2:
        try:
            raw_query2 = pathlib.Path(args.query_file2).read_text(encoding="utf-8")
        except OSError as e:
            print(f"Error: could not read query file '{args.query_file2}': {e}")
            sys.exit(1)
    elif args.query2:
        raw_query2 = args.query2

    # Production gating covers every alias involved.
    involved = [args.db_alias] + ([args.compare] if args.compare else [])
    for alias in involved:
        env = DB_ENVIRONMENTS[alias]
        is_production = bool(env.get("production")) or "prod" in alias.lower()
        if is_production:
            if not args.allow_prod:
                print(
                    f"Error: '{alias}' is a production database. "
                    "Re-run with --allow-prod to proceed."
                )
                sys.exit(1)
            print(f"\n!!! Running against PRODUCTION ({alias}). !!!\n", file=sys.stderr)

    query_body = ""
    query_body2 = ""
    if not args.describe:
        query_body = sanitize_query(raw_query)
        enforce_query_safety(query_body, engine_for_alias(args.db_alias))

        if args.compare:
            query_body2 = sanitize_query(raw_query2) if raw_query2 else query_body
            enforce_query_safety(query_body2, engine_for_alias(args.compare))

        if args.show_query:
            print("\n-- Sanitized SQL to execute --")
            print(query_body)
            if args.compare:
                print(f"-- against {args.compare} --")
                print(query_body2)
            print("-- end SQL --\n")

    if args.describe:
        info = run_describe(args)
        query_body = f"<describe {args.describe}>"
        if args.compare:
            query_body2 = f"<describe {args.describe2 or args.describe}>"
    elif args.compare:
        info = run_compare(
            args.db_alias,
            query_body,
            args.compare,
            query_body2,
            max_rows=args.max_rows,
            fmt=args.format,
            timeout_seconds=args.timeout,
        )
    else:
        info = execute_query(
            args.db_alias,
            query_body,
            max_rows=args.max_rows,
            fmt=args.format,
            timeout_seconds=args.timeout,
            plan=args.plan,
        )

    if not args.no_audit:
        write_audit(
            {
                "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "alias": args.db_alias,
                "query": query_body,
                **({"query2": query_body2} if args.compare else {}),
                **info,
            }
        )

    if info.get("status") == "error":
        sys.exit(1)
    sys.exit(info.get("exit_code", 0))


if __name__ == "__main__":
    main()
