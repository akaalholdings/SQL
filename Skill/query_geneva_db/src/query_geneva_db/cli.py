import argparse
import importlib.resources
import json
import os
import pathlib
import re
import shutil
import struct
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    import pyodbc
except ImportError:  # pragma: no cover - exercised in environments without pyodbc
    pyodbc = None  # type: ignore[assignment]

try:
    from azure.identity import AzureCliCredential
except ImportError:  # pragma: no cover - exercised in environments without azure-identity
    AzureCliCredential = None  # type: ignore[assignment]

DB_TYPES = ("SQL Server", "Synapse")
SQL_COPT_SS_ACCESS_TOKEN = 1256
DEFAULT_PREVIEW_ROWS = 50
DEFAULT_CANDIDATE_COUNT = 5
DEFAULT_MAX_OUTPUT_COLUMNS = 10

_CODE_FENCE_RE = re.compile(
    r"```(?:sql|tsql|t-sql)?\s*(.*?)\s*```",
    flags=re.IGNORECASE | re.DOTALL,
)
_TILDE_FENCE_RE = re.compile(
    r"~~~(?:sql|tsql|t-sql)?\s*(.*?)\s*~~~",
    flags=re.IGNORECASE | re.DOTALL,
)
_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]+")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "get",
    "give",
    "i",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "show",
    "that",
    "the",
    "to",
    "want",
    "where",
    "with",
}

COMPARISON_PATTERNS = (
    re.compile(
        r"(?P<lhs>[A-Za-z_][A-Za-z0-9_ ]{1,50})\s*(?P<op>>=|<=|>|<|=)\s*(?P<rhs>\d+(?:\.\d+)?)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"(?P<lhs>[A-Za-z_][A-Za-z0-9_ ]{1,50})\s+"
        r"(?P<op>greater than|more than|over|above|less than|under|below|at least|at most)\s+"
        r"(?P<rhs>\d+(?:\.\d+)?)",
        flags=re.IGNORECASE,
    ),
)

OPERATOR_MAP = {
    "greater than": ">",
    "more than": ">",
    "over": ">",
    "above": ">",
    "less than": "<",
    "under": "<",
    "below": "<",
    "at least": ">=",
    "at most": "<=",
}


@dataclass
class CatalogEntry:
    dataset_name: str
    technical_name: str
    purpose: str
    column_name: str
    column_desc: str
    ref_technical_name: str
    filter_usage: str


@dataclass
class DatasetProfile:
    dataset_name: str
    technical_name: str
    resolved_table_name: str
    purpose: str
    columns: set[str] = field(default_factory=set)
    descriptions: list[str] = field(default_factory=list)
    references: set[str] = field(default_factory=set)
    filter_columns: set[str] = field(default_factory=set)


@dataclass
class CandidateScore:
    profile: DatasetProfile
    score: int
    matched_columns: dict[str, int]


@dataclass
class NLQueryPlan:
    request: str
    selected_profile: DatasetProfile
    generated_sql: str
    candidates: list[CandidateScore]
    selected_columns: list[str]


def ensure_runtime_dependencies() -> None:
    if pyodbc is None:
        raise RuntimeError("Missing dependency 'pyodbc'. Install project dependencies first.")
    if AzureCliCredential is None:
        raise RuntimeError("Missing dependency 'azure-identity'. Install project dependencies first.")


def decode_datetimeoffset(raw: bytes) -> str:
    """Decode SQL Server DATETIMEOFFSET binary payload from ODBC type -155."""
    if not raw:
        return ""
    try:
        year, month, day, hour, minute, second, fraction, tz_hour, tz_min = struct.unpack("<6hI2h", raw)
    except struct.error:
        return repr(raw)

    sign = "-" if (tz_hour < 0 or tz_min < 0) else "+"
    tz_hour_abs = abs(tz_hour)
    tz_min_abs = abs(tz_min)

    return (
        f"{year:04d}-{month:02d}-{day:02d} "
        f"{hour:02d}:{minute:02d}:{second:02d}.{fraction:07d} "
        f"{sign}{tz_hour_abs:02d}:{tz_min_abs:02d}"
    )


def normalize_header(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def tokenize(text: str) -> set[str]:
    tokens = set()
    for token in _TOKEN_RE.findall(text.lower()):
        candidates = [token]
        if "_" in token:
            candidates.extend(part for part in token.split("_") if part)

        for candidate in candidates:
            if candidate in STOPWORDS:
                continue
            if len(candidate) <= 1:
                continue
            tokens.add(candidate)
    return tokens


def strip_brackets(identifier: str) -> str:
    cleaned = identifier.strip()
    if cleaned.startswith("[") and cleaned.endswith("]") and len(cleaned) >= 2:
        return cleaned[1:-1]
    return cleaned


def quote_identifier(identifier: str) -> str:
    cleaned = strip_brackets(identifier)
    escaped = cleaned.replace("]", "]]")
    return f"[{escaped}]"


def quote_object_name(object_name: str) -> str:
    parts = [strip_brackets(part.strip()) for part in object_name.split(".") if part.strip()]
    if not parts:
        raise ValueError("Object name cannot be empty.")
    return ".".join(quote_identifier(part) for part in parts)


def load_db_environments(config_path: Optional[str]) -> dict[str, dict[str, Any]]:
    candidates: list[pathlib.Path] = []
    if config_path:
        candidates.append(pathlib.Path(config_path).expanduser())
    env_config = os.getenv("QUERY_GENEVA_DB_CONFIG")
    if env_config:
        candidates.append(pathlib.Path(env_config).expanduser())
    candidates.append(pathlib.Path.cwd() / "db_environments.json")

    selected_path: Optional[pathlib.Path] = None
    for path in candidates:
        if path.exists():
            selected_path = path
            break

    if selected_path is None:
        if config_path:
            raise FileNotFoundError(f"DB config file not found: {config_path}")
        return {}

    try:
        data = json.loads(selected_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in DB config file {selected_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"DB config must be a JSON object in {selected_path}.")

    normalized: dict[str, dict[str, Any]] = {}
    for alias, info in data.items():
        if not isinstance(info, dict):
            continue
        normalized[str(alias)] = info

    return normalized


def get_best_driver() -> str:
    ensure_runtime_dependencies()
    assert pyodbc is not None

    drivers = pyodbc.drivers()
    ms_drivers = [driver for driver in drivers if "ODBC Driver" in driver and "for SQL Server" in driver]

    modern_drivers: list[str] = []
    for driver in ms_drivers:
        match = re.search(r"ODBC Driver (\d+) for SQL Server", driver)
        if match and int(match.group(1)) >= 17:
            modern_drivers.append(driver)

    if not modern_drivers:
        raise RuntimeError(
            f"No modern MS ODBC Driver (v17+) found in: {drivers}. "
            "Install ODBC Driver 17 or 18 for SQL Server."
        )

    return max(modern_drivers)


def get_db_connection(db_alias: str, environments: dict[str, dict[str, Any]]) -> Any:
    ensure_runtime_dependencies()
    assert pyodbc is not None
    assert AzureCliCredential is not None

    if db_alias not in environments:
        available = ", ".join(sorted(environments)) or "<none configured>"
        raise ValueError(f"Unknown database alias '{db_alias}'. Available aliases: {available}")

    env = environments[db_alias]
    missing = [key for key in ("server", "database") if not env.get(key)]
    if missing:
        raise ValueError(f"DB alias '{db_alias}' missing required keys: {', '.join(missing)}")

    credential = AzureCliCredential()
    database_token = credential.get_token("https://database.windows.net/.default")

    token_bytes = bytes(database_token.token, "utf-8")
    expanded_token = b"".join(bytes([b, 0]) for b in token_bytes)
    token_struct = struct.pack("=i", len(expanded_token)) + expanded_token

    driver = get_best_driver()
    conn_string = (
        f"Driver={{{driver}}};"
        f"SERVER={env['server']};"
        f"DATABASE={env['database']};"
        "ApplicationIntent=ReadOnly;"
    )
    if "ODBC Driver 18" in driver:
        conn_string += "TrustServerCertificate=yes;"

    try:
        login_timeout = int(os.getenv("QUERY_GENEVA_DB_LOGIN_TIMEOUT", "60"))
    except ValueError:
        login_timeout = 60

    conn = pyodbc.connect(
        conn_string,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
        timeout=max(login_timeout, 5),
    )
    conn.add_output_converter(-155, decode_datetimeoffset)  # DATETIMEOFFSET
    return conn


def _find_skill_file(search_root: pathlib.Path) -> Optional[pathlib.Path]:
    for parent in (search_root, *search_root.parents):
        for name in ("SKILL.md", "skill.md", "skills.md"):
            candidate = parent / name
            if candidate.exists():
                return candidate
    return None


def install_skill() -> None:
    source_path = _find_skill_file(pathlib.Path(__file__).resolve())
    if source_path is None:
        print("Error: SKILL.md not found near the package. Place SKILL.md at project root.")
        sys.exit(1)

    destination_dir = pathlib.Path.home() / ".copilot" / "skills" / "query_geneva_db"
    destination_path = destination_dir / "SKILL.md"

    try:
        destination_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
        print(f"Successfully installed skill to: {destination_path}")
    except Exception as exc:
        print(f"Error installing skill: {exc}")
        sys.exit(1)


def check_skill_status() -> str:
    source_path = _find_skill_file(pathlib.Path(__file__).resolve())
    destination_path = pathlib.Path.home() / ".copilot" / "skills" / "query_geneva_db" / "SKILL.md"

    if source_path is None:
        return ""

    if not destination_path.exists():
        return "\nNOTE: Copilot skill is not installed. Run with --skill or --install-skill."

    if source_path.stat().st_mtime > destination_path.stat().st_mtime:
        return "\nWARNING: A newer SKILL.md is available. Run with --skill or --install-skill to update."

    return ""


def format_alias_descriptions(environments: dict[str, dict[str, Any]]) -> str:
    grouped_aliases: dict[str, list[str]] = {db_type: [] for db_type in DB_TYPES}

    for alias, info in sorted(environments.items()):
        db_type = str(info.get("type", "SQL Server"))
        description = str(info.get("description", ""))
        grouped_aliases.setdefault(db_type, []).append(f"      {alias}: {description}")

    sections: list[str] = []
    for db_type, items in grouped_aliases.items():
        if not items:
            continue
        sections.append(f"  {db_type}:")
        sections.extend(items)

    if not sections:
        sections.append("  <No aliases configured. Add db_environments.json in this folder.>")

    return "\n".join(sections)


def build_arg_parser(environments: dict[str, dict[str, Any]]) -> argparse.ArgumentParser:
    skill_status = check_skill_status()
    parser = argparse.ArgumentParser(
        description="Run read-only SQL against Geneva DB or generate SQL from natural language.",
        epilog=skill_status,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--install-skill",
        "--skill",
        action="store_true",
        help="Install SKILL.md to ~/.copilot/skills/query_geneva_db/SKILL.md.",
    )
    parser.add_argument(
        "--db-config",
        type=str,
        help="Path to DB alias config JSON (default: ./db_environments.json).",
    )
    parser.add_argument(
        "--query-file",
        "-f",
        type=str,
        help="Read SQL or NL request from a file path.",
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read SQL or NL request from STDIN.",
    )
    parser.add_argument(
        "--mode",
        choices=("auto", "sql", "nl"),
        default="auto",
        help="Input mode: auto-detect, force SQL, or force natural language.",
    )
    parser.add_argument(
        "--reference-query-file",
        type=str,
        help="Override built-in dataset reference query file used for NL2SQL mapping.",
    )
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=DEFAULT_PREVIEW_ROWS,
        help=f"Rows to display from result set (default: {DEFAULT_PREVIEW_ROWS}).",
    )
    parser.add_argument(
        "--candidate-count",
        type=int,
        default=DEFAULT_CANDIDATE_COUNT,
        help=f"Number of scored NL2SQL table candidates to display (default: {DEFAULT_CANDIDATE_COUNT}).",
    )
    parser.add_argument(
        "--show-query",
        action="store_true",
        help="Print sanitized/generated SQL before execution.",
    )
    parser.add_argument(
        "--save-query",
        type=str,
        help="Write sanitized/generated SQL to a file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate/validate SQL only. Do not execute the query.",
    )

    alias_descriptions = format_alias_descriptions(environments)
    parser.add_argument(
        "db_alias",
        nargs="?",
        help=f"Database alias from config:\n{alias_descriptions}\n",
    )
    parser.add_argument(
        "query_or_request",
        nargs="?",
        help="SQL query or natural-language request (omit if using --query-file or --stdin).",
    )
    return parser


def extract_code_block_if_present(text: str) -> str:
    match = _CODE_FENCE_RE.search(text)
    if match:
        return match.group(1).strip()
    match = _TILDE_FENCE_RE.search(text)
    if match:
        return match.group(1).strip()
    return text


def strip_go_lines(text: str) -> str:
    return re.sub(r"(?im)^\s*GO\s*;?\s*$", "", text)


def strip_sql_comments(sql: str) -> str:
    out: list[str] = []
    index = 0
    length = len(sql)

    in_single = False
    in_double = False
    in_bracket = False
    in_line_comment = False
    in_block_comment = False

    while index < length:
        char = sql[index]
        nxt = sql[index + 1] if index + 1 < length else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                out.append(char)
            index += 1
            continue

        if in_block_comment:
            if char == "*" and nxt == "/":
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue

        if in_single:
            out.append(char)
            if char == "'":
                if nxt == "'":
                    out.append(nxt)
                    index += 2
                    continue
                in_single = False
            index += 1
            continue

        if in_double:
            out.append(char)
            if char == '"':
                if nxt == '"':
                    out.append(nxt)
                    index += 2
                    continue
                in_double = False
            index += 1
            continue

        if in_bracket:
            out.append(char)
            if char == "]":
                if nxt == "]":
                    out.append(nxt)
                    index += 2
                    continue
                in_bracket = False
            index += 1
            continue

        if char == "'":
            in_single = True
            out.append(char)
            index += 1
            continue
        if char == '"':
            in_double = True
            out.append(char)
            index += 1
            continue
        if char == "[":
            in_bracket = True
            out.append(char)
            index += 1
            continue

        if char == "-" and nxt == "-":
            in_line_comment = True
            index += 2
            continue
        if char == "/" and nxt == "*":
            in_block_comment = True
            index += 2
            continue

        out.append(char)
        index += 1

    return "".join(out)


def extract_sql_block(text: str) -> str:
    upper = text.upper()

    for match in re.finditer(r"\bSELECT\b", upper):
        if re.search(r"\bFROM\b", upper[match.end() :]):
            return text[match.start() :].strip()

    with_match = re.search(r"(?im)^\s*;?\s*WITH\b", text)
    if with_match:
        return text[with_match.start() :].strip()

    return text.strip()


def sanitize_query(raw: str) -> str:
    query = (raw or "").lstrip("\ufeff").strip()
    query = extract_code_block_if_present(query)
    query = strip_go_lines(query)
    query = extract_sql_block(query)
    query = strip_sql_comments(query)
    query = strip_go_lines(query)
    return query.strip()


def enforce_query_safety(query_body: str) -> None:
    normalized = query_body.lstrip()
    normalized_for_start = re.sub(r"^;+\s*", "", normalized)
    upper_query = normalized_for_start.upper()

    if not (upper_query.startswith("SELECT") or upper_query.startswith("WITH")):
        print(
            "Error: Only SELECT queries or CTEs starting with WITH are allowed.\n"
            "If you entered natural language, rerun with --mode nl (or --mode auto)."
        )
        sys.exit(1)

    pattern = r"('[^']*(?:''[^']*)*('|$))|(\[[^\]]*(\]|$))|(\"[^\"]*(\"|$))"
    check_query = re.sub(pattern, " ", query_body)

    check_query = re.sub(r"^\s*;+\s*", "", check_query)
    stripped_check = re.sub(r"[;\s]+$", "", check_query)
    if ";" in stripped_check:
        print("Error: Multiple SQL statements detected. Only a single SELECT statement is allowed.")
        sys.exit(1)

    forbidden_keywords = r"\b(DELETE|UPDATE|INSERT|DROP|ALTER|TRUNCATE|MERGE|GRANT|REVOKE|EXEC|EXECUTE|CREATE|INTO)\b"
    match = re.search(forbidden_keywords, check_query.upper())
    if match:
        print(f"Error: Forbidden keyword '{match.group(1)}' detected.")
        sys.exit(1)

    wildcard_match = re.search(r"\bSELECT\s+\*", check_query, flags=re.IGNORECASE)
    if wildcard_match:
        print("Error: SELECT * is blocked. Use explicit column names.")
        sys.exit(1)


def looks_like_sql(raw_input: str) -> bool:
    cleaned = sanitize_query(raw_input)
    if not cleaned:
        return False
    normalized = re.sub(r"^;+\s*", "", cleaned.lstrip(), flags=re.IGNORECASE)
    upper = normalized.upper()
    if upper.startswith("SELECT"):
        return bool(re.search(r"\bFROM\b", upper))
    if upper.startswith("WITH"):
        return bool(re.search(r"\bSELECT\b", upper) and re.search(r"\bFROM\b", upper))
    return False


def read_reference_query(reference_query_file: Optional[str]) -> str:
    if reference_query_file:
        return pathlib.Path(reference_query_file).read_text(encoding="utf-8")

    resource_file = importlib.resources.files("query_geneva_db.resources").joinpath("reference_catalog.sql")
    return resource_file.read_text(encoding="utf-8")


def choose_schema_for_table(schemas: list[str]) -> str:
    def schema_rank(schema: str) -> tuple[int, str]:
        lowered = schema.lower()
        if lowered.startswith("cns_"):
            return (0, lowered)
        if lowered.startswith("trf_"):
            return (1, lowered)
        if lowered.startswith("raw_"):
            return (2, lowered)
        if lowered.startswith("dbo"):
            return (3, lowered)
        return (4, lowered)

    return sorted(schemas, key=schema_rank)[0]


def load_table_inventory(conn: Any) -> dict[str, list[str]]:
    inventory: dict[str, list[str]] = defaultdict(list)
    query = """
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        for schema, table in cursor.fetchall():
            schema_name = str(schema or "").strip()
            table_name = str(table or "").strip()
            if schema_name and table_name:
                inventory[table_name.lower()].append(schema_name)
    return inventory


def resolve_technical_name(technical_name: str, inventory: dict[str, list[str]]) -> str:
    cleaned = technical_name.strip()
    if not cleaned:
        raise ValueError("Empty technical name in catalog.")

    if "." in cleaned:
        return quote_object_name(cleaned)

    table_name = strip_brackets(cleaned)
    schemas = inventory.get(table_name.lower(), [])
    if schemas:
        chosen_schema = choose_schema_for_table(schemas)
        return f"{quote_identifier(chosen_schema)}.{quote_identifier(table_name)}"
    return quote_identifier(table_name)


def load_catalog_entries(conn: Any, reference_query: str) -> list[CatalogEntry]:
    with conn.cursor() as cursor:
        cursor.execute(reference_query)
        if not cursor.description:
            return []
        headers = [normalize_header(column[0]) for column in cursor.description]
        rows = cursor.fetchall()

    entries: list[CatalogEntry] = []
    for row in rows:
        row_map = {headers[index]: row[index] for index in range(len(headers))}

        technical_name = str(row_map.get("technicalname") or "").strip()
        column_name = str(row_map.get("columnname") or "").strip()
        if not technical_name or not column_name:
            continue

        entries.append(
            CatalogEntry(
                dataset_name=str(row_map.get("datasetname") or "").strip(),
                technical_name=technical_name,
                purpose=str(row_map.get("purpose") or "").strip(),
                column_name=column_name,
                column_desc=str(row_map.get("description") or "").strip(),
                ref_technical_name=str(row_map.get("reftechnicalname") or "").strip(),
                filter_usage=str(row_map.get("filter") or "").strip(),
            )
        )

    return entries


def build_dataset_profiles(
    entries: list[CatalogEntry],
    inventory: dict[str, list[str]],
) -> list[DatasetProfile]:
    profiles: dict[str, DatasetProfile] = {}

    for entry in entries:
        key = entry.technical_name.lower()
        profile = profiles.get(key)
        if profile is None:
            profile = DatasetProfile(
                dataset_name=entry.dataset_name or entry.technical_name,
                technical_name=entry.technical_name,
                resolved_table_name=resolve_technical_name(entry.technical_name, inventory),
                purpose=entry.purpose,
            )
            profiles[key] = profile

        profile.columns.add(entry.column_name)
        if entry.column_desc:
            profile.descriptions.append(entry.column_desc)
        if entry.ref_technical_name:
            profile.references.add(entry.ref_technical_name)
        if entry.filter_usage.upper() in {"Y", "YES", "1", "TRUE"}:
            profile.filter_columns.add(entry.column_name)

    return list(profiles.values())


def score_profile(profile: DatasetProfile, request_tokens: set[str]) -> CandidateScore:
    if not request_tokens:
        return CandidateScore(profile=profile, score=0, matched_columns={})

    dataset_tokens = tokenize(f"{profile.dataset_name} {profile.technical_name} {profile.purpose}")
    reference_tokens = tokenize(" ".join(profile.references))
    matched_columns: dict[str, int] = defaultdict(int)

    score = 0
    for token in request_tokens:
        if token in dataset_tokens:
            score += 8
        if token in reference_tokens:
            score += 3

    for column in profile.columns:
        column_tokens = tokenize(column)
        overlap = request_tokens & column_tokens
        if overlap:
            value = 5 * len(overlap)
            score += value
            matched_columns[column] += value

    for desc in profile.descriptions:
        overlap = request_tokens & tokenize(desc)
        if overlap:
            score += 2 * len(overlap)

    if profile.filter_columns:
        for column in profile.filter_columns:
            overlap = request_tokens & tokenize(column)
            if overlap:
                matched_columns[column] += 2 * len(overlap)
                score += 2 * len(overlap)

    return CandidateScore(profile=profile, score=score, matched_columns=dict(matched_columns))


def detect_date_column(columns: set[str]) -> Optional[str]:
    ranked: list[tuple[int, str]] = []
    for column in columns:
        name = column.lower()
        rank = 100
        if "dttm" in name or "datetime" in name:
            rank = 0
        elif "date" in name or name.endswith("_dt"):
            rank = 1
        elif "time" in name or name.endswith("_ts"):
            rank = 2
        if rank < 100:
            ranked.append((rank, column))

    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1].lower()))
    return ranked[0][1]


def pick_output_columns(
    profile: DatasetProfile,
    matched_columns: dict[str, int],
    request_tokens: set[str],
    max_columns: int = DEFAULT_MAX_OUTPUT_COLUMNS,
) -> list[str]:
    selected: list[str] = []

    sorted_matched = sorted(matched_columns.items(), key=lambda item: (-item[1], item[0].lower()))
    for column, _ in sorted_matched:
        selected.append(column)
        if len(selected) >= max_columns:
            return selected

    fallback_patterns = (
        r"(_key|_id)$",
        r"(_nm|_name)$",
        r"(_cd|_code)$",
        r"(date|dttm|time|_dt|_ts)",
    )
    available_columns = sorted(profile.columns, key=str.lower)
    for pattern in fallback_patterns:
        for column in available_columns:
            if column in selected:
                continue
            if re.search(pattern, column, flags=re.IGNORECASE):
                selected.append(column)
            if len(selected) >= max_columns:
                return selected

    if not selected:
        for column in available_columns:
            selected.append(column)
            if len(selected) >= min(max_columns, 6):
                break

    if request_tokens and len(selected) < max_columns:
        for column in available_columns:
            if column in selected:
                continue
            if tokenize(column) & request_tokens:
                selected.append(column)
            if len(selected) >= max_columns:
                break

    return selected


def find_best_column_match(phrase: str, columns: set[str]) -> Optional[str]:
    phrase_tokens = tokenize(phrase)
    if not phrase_tokens:
        return None

    best_column: Optional[str] = None
    best_score = 0
    for column in columns:
        score = len(phrase_tokens & tokenize(column))
        if score > best_score:
            best_score = score
            best_column = column

    if best_score == 0:
        return None
    return best_column


def extract_numeric_predicates(request: str, columns: set[str], alias: str) -> list[str]:
    predicates: list[str] = []
    for pattern in COMPARISON_PATTERNS:
        for match in pattern.finditer(request):
            lhs = (match.group("lhs") or "").strip()
            op = (match.group("op") or "").strip().lower()
            rhs = (match.group("rhs") or "").strip()

            mapped_op = OPERATOR_MAP.get(op, op)
            if mapped_op not in {">", "<", ">=", "<=", "="}:
                continue

            column = find_best_column_match(lhs, columns)
            if column is None:
                continue

            predicates.append(f"{alias}.{quote_identifier(column)} {mapped_op} {rhs}")
    return predicates


def build_where_clauses(request: str, profile: DatasetProfile, alias: str) -> tuple[list[str], Optional[str]]:
    clauses: list[str] = []
    date_column = detect_date_column(profile.columns)
    lowered = request.lower()

    if date_column and "today" in lowered:
        clauses.append(f"CAST({alias}.{quote_identifier(date_column)} AS date) = CAST(GETDATE() AS date)")
    elif date_column and "yesterday" in lowered:
        clauses.append(f"CAST({alias}.{quote_identifier(date_column)} AS date) = CAST(DATEADD(day, -1, GETDATE()) AS date)")

    numeric_clauses = extract_numeric_predicates(request, profile.columns, alias)
    for clause in numeric_clauses:
        if clause not in clauses:
            clauses.append(clause)

    return clauses, date_column


def generate_sql_from_nl(
    profile: DatasetProfile,
    selected_columns: list[str],
    request: str,
    preview_rows: int,
) -> str:
    alias = "src"
    lines: list[str] = [f"SELECT TOP ({preview_rows})"]

    select_lines = [f"    {alias}.{quote_identifier(column)}" for column in selected_columns]
    lines.append(",\n".join(select_lines))
    lines.append(f"FROM {profile.resolved_table_name} AS {alias}")

    where_clauses, date_column = build_where_clauses(request, profile, alias)
    if where_clauses:
        lines.append("WHERE")
        lines.append("    " + "\n    AND ".join(where_clauses))

    if date_column:
        lines.append(f"ORDER BY {alias}.{quote_identifier(date_column)} DESC")
    elif selected_columns:
        first_column = selected_columns[0]
        if _IDENTIFIER_RE.match(strip_brackets(first_column)):
            lines.append(f"ORDER BY {alias}.{quote_identifier(first_column)}")

    if lines[-1].endswith(";"):
        return "\n".join(lines)
    lines[-1] = lines[-1] + ";"
    return "\n".join(lines)


def build_nl_query_plan(
    conn: Any,
    request: str,
    reference_query: str,
    preview_rows: int,
    candidate_count: int,
) -> NLQueryPlan:
    entries = load_catalog_entries(conn, reference_query)
    if not entries:
        raise RuntimeError("Reference catalog query returned no rows; cannot map natural language to SQL.")

    inventory = load_table_inventory(conn)
    profiles = build_dataset_profiles(entries, inventory)
    if not profiles:
        raise RuntimeError("Catalog rows could not be converted to dataset profiles.")

    request_tokens = tokenize(request)
    scored = [score_profile(profile, request_tokens) for profile in profiles]
    scored.sort(
        key=lambda item: (
            item.score,
            len(item.profile.columns),
            item.profile.technical_name.lower(),
        ),
        reverse=True,
    )

    selected = scored[0]
    selected_columns = pick_output_columns(selected.profile, selected.matched_columns, request_tokens)
    if not selected_columns:
        raise RuntimeError("Unable to select output columns from mapped dataset.")

    sql = generate_sql_from_nl(selected.profile, selected_columns, request, preview_rows)
    return NLQueryPlan(
        request=request,
        selected_profile=selected.profile,
        generated_sql=sql,
        candidates=scored[: max(candidate_count, 1)],
        selected_columns=selected_columns,
    )


def print_nl_plan(plan: NLQueryPlan) -> None:
    print("\n-- NL2SQL mapping --")
    print(f"Request: {plan.request.strip()}")
    print(
        "Selected dataset: "
        f"{plan.selected_profile.dataset_name} ({plan.selected_profile.technical_name}) "
        f"-> {plan.selected_profile.resolved_table_name}"
    )
    print("Candidate datasets:")
    for index, candidate in enumerate(plan.candidates, start=1):
        profile = candidate.profile
        print(
            f"  {index}. score={candidate.score:>4} | "
            f"{profile.dataset_name} ({profile.technical_name})"
        )


def format_cell(value: Any) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    return text.replace("\t", " ").replace("\n", " ").replace("\r", " ")


def execute_query(
    db_alias: str,
    environments: dict[str, dict[str, Any]],
    query: str,
    preview_rows: int,
) -> None:
    try:
        with get_db_connection(db_alias, environments) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

                if not cursor.description:
                    print("Query executed successfully, but returned no result set.")
                    return

                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchmany(preview_rows + 1)
                is_truncated = len(rows) > preview_rows
                rows = rows[:preview_rows]

                print("\t".join(str(column) for column in columns))
                for row in rows:
                    print("\t".join(format_cell(item) for item in row))

                if not rows:
                    print("(0 rows)")
                if is_truncated:
                    print(
                        f"\nResult truncated to first {preview_rows} rows. "
                        "Add stronger filters or increase --preview-rows."
                    )
    except Exception as exc:
        print(f"Error executing query: {exc}")
        sys.exit(1)


def validate_input_sources(args: argparse.Namespace, parser: argparse.ArgumentParser) -> str:
    sources = [
        bool(args.query_or_request),
        bool(args.query_file),
        bool(args.stdin),
    ]
    if sum(sources) != 1:
        parser.error("Provide exactly one input source: positional query/request, --query-file, or --stdin.")

    if args.stdin:
        data = sys.stdin.read()
        if not data.strip():
            parser.error("No input received from STDIN.")
        return data

    if args.query_file:
        return pathlib.Path(args.query_file).read_text(encoding="utf-8")

    return str(args.query_or_request or "")


def preparse_db_config(argv: list[str]) -> Optional[str]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db-config", type=str)
    args, _ = parser.parse_known_args(argv)
    return args.db_config


def main() -> None:
    try:
        pre_db_config = preparse_db_config(sys.argv[1:])
        environments = load_db_environments(pre_db_config)
        parser = build_arg_parser(environments)
        args = parser.parse_args()

        if args.install_skill:
            if args.db_alias or args.query_or_request or args.query_file or args.stdin:
                parser.error("--install-skill/--skill cannot be used with query execution arguments.")
            install_skill()
            return

        environments = load_db_environments(args.db_config)
        if not environments:
            parser.error(
                "No DB aliases configured. Add ./db_environments.json or pass --db-config path.\n"
                "Use db_environments.example.json as a template."
            )

        if not args.db_alias:
            parser.print_help()
            sys.exit(1)

        if args.db_alias not in environments:
            available = ", ".join(sorted(environments))
            parser.error(f"Unknown db_alias '{args.db_alias}'. Available aliases: {available}")

        if args.preview_rows <= 0:
            parser.error("--preview-rows must be greater than 0.")
        if args.candidate_count <= 0:
            parser.error("--candidate-count must be greater than 0.")

        raw_input = validate_input_sources(args, parser)
        mode = args.mode
        if mode == "auto":
            mode = "sql" if looks_like_sql(raw_input) else "nl"

        if args.db_alias == "mid_prod":
            print("\n!!! YOU ARE CONNECTING TO MID_PROD (production). PROCEED WITH CAUTION. !!!\n")

        query_body: str
        if mode == "sql":
            query_body = sanitize_query(raw_input)
            enforce_query_safety(query_body)
        else:
            reference_query = read_reference_query(args.reference_query_file)
            try:
                with get_db_connection(args.db_alias, environments) as conn:
                    plan = build_nl_query_plan(
                        conn=conn,
                        request=raw_input,
                        reference_query=reference_query,
                        preview_rows=args.preview_rows,
                        candidate_count=args.candidate_count,
                    )
            except Exception as exc:
                print(f"Error generating SQL from natural language: {exc}")
                sys.exit(1)

            print_nl_plan(plan)
            query_body = plan.generated_sql
            enforce_query_safety(query_body)

        if args.show_query or mode == "nl":
            print("\n-- SQL to execute --")
            print(query_body)
            print("-- end SQL --\n")

        if args.save_query:
            output_path = pathlib.Path(args.save_query)
            output_path.write_text(query_body + "\n", encoding="utf-8")
            print(f"Saved SQL to: {output_path}")

        if args.dry_run:
            return

        execute_query(
            db_alias=args.db_alias,
            environments=environments,
            query=query_body,
            preview_rows=args.preview_rows,
        )
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    except ValueError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    except RuntimeError as exc:
        print(f"Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
