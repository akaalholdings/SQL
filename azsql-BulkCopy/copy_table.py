#!/usr/bin/env python3
"""Copy one Azure SQL table -> Parquet parts in Blob Storage -> Azure SQL, in parallel.

Extract: range-partition the table on an integer column, N workers stream
         cursor.arrow_batch() into Parquet and upload one blob per part,
         then write <run_id>/manifest.json.
Load:    TRUNCATE dest, N workers download parts and cursor.bulkcopy_arrow(),
         then verify dest COUNT_BIG(*) == manifest total.

Usage:
  python copy_table.py --table dbo.Orders --partition-column OrderId --parts 8 --workers 4
  python copy_table.py --phase extract --table dbo.Orders --partition-column OrderId --run-id 20260902T120000Z
  python copy_table.py --phase load --run-id 20260902T120000Z [--dest-table stg.Orders]

Env (or .env): SOURCE_CONNECTION_STRING, DEST_CONNECTION_STRING,
               STORAGE_ACCOUNT_URL, STORAGE_CONTAINER
Auth: Entra ID. One DefaultAzureCredential is used for Blob and passed to SQL as
      token_provider. If a connection string already contains Authentication=,
      it is used as-is (token_provider is mutually exclusive with it).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import mssql_python
import pyarrow as pa
import pyarrow.parquet as pq
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

log = logging.getLogger("copy_table")

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INT_TYPES = {"tinyint", "smallint", "int", "bigint"}
_UNSUPPORTED = {"sql_variant", "geography", "geometry", "hierarchyid"}
_SERVER_GENERATED = {"timestamp", "rowversion"}  # dest generates these; never copied
BLOB_CONCURRENCY = 4
CONNECT_TIMEOUT = 60  # also inherited by bulkcopy's internal connection (>=1.12)


# ── identifiers ──────────────────────────────────────────────────────────────

def ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return f"[{name}]"


def parse_table(name: str) -> tuple[str, str]:
    parts = name.split(".")
    if len(parts) == 1:
        return "dbo", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Expected schema.table, got {name!r}")


def qualified(name: str) -> str:
    schema, table = parse_table(name)
    return f"{ident(schema)}.{ident(table)}"


def split_ranges(lo: int, hi: int, parts: int) -> list[tuple[int, int]]:
    """Equal-width closed ranges [lo, hi] covering every integer exactly once."""
    if hi < lo:
        raise ValueError(f"hi ({hi}) < lo ({lo})")
    span = hi - lo + 1
    parts = max(1, min(parts, span))
    width = -(-span // parts)  # ceil
    out, start = [], lo
    while start <= hi:
        end = min(start + width - 1, hi)
        out.append((start, end))
        start = end + 1
    return out


def select_sql(table: str, columns: list[str], key: str) -> str:
    cols = ", ".join(ident(c) for c in columns)
    k = ident(key)
    return f"SELECT {cols} FROM {qualified(table)} WHERE {k} >= ? AND {k} <= ?"


# ── manifest ─────────────────────────────────────────────────────────────────

@dataclass
class Part:
    name: str
    lo: int
    hi: int
    blob: str | None = None
    rows: int = 0


@dataclass
class Manifest:
    run_id: str
    source_table: str
    partition_column: str
    columns: list[str]
    source_count: int
    created_utc: str
    parts: list[Part] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(p.rows for p in self.parts)

    def to_json(self) -> str:
        d = asdict(self)
        d["total_rows"] = self.total_rows
        return json.dumps(d, indent=2)

    @classmethod
    def from_json(cls, text: str) -> "Manifest":
        d = json.loads(text)
        d.pop("total_rows", None)
        d["parts"] = [Part(**p) for p in d["parts"]]
        return cls(**d)


def manifest_blob(run_id: str) -> str:
    return f"{run_id}/manifest.json"


def part_blob(run_id: str, idx: int) -> str:
    return f"{run_id}/part-{idx:04d}.parquet"


# ── config / connections ─────────────────────────────────────────────────────

@dataclass
class Config:
    src: str
    dst: str
    account_url: str
    container: str
    batch_size: int
    workers: int
    bulk_timeout: int
    table_lock: bool
    check_constraints: bool
    row_mode: bool


def env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"Missing required env var {name}")
    return val


def connect(conn_str: str, cred):
    if "authentication=" in conn_str.lower():
        return mssql_python.connect(conn_str, timeout=CONNECT_TIMEOUT)
    return mssql_python.connect(conn_str, token_provider=cred, timeout=CONNECT_TIMEOUT)


# ── extract ──────────────────────────────────────────────────────────────────

def source_columns(cur, table: str, key: str) -> list[str]:
    """Copyable column names in ordinal order; validates the partition column."""
    # TYPE_NAME(system_type_id) resolves alias types to their base type and is
    # NULL for CLR types (geography etc.), where the user type name is correct.
    cur.execute(
        "SELECT c.name, COALESCE(TYPE_NAME(c.system_type_id), t.name), "
        "c.is_computed, c.is_nullable "
        "FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id "
        "WHERE c.object_id = OBJECT_ID(?) ORDER BY c.column_id",
        (qualified(table),),
    )
    rows = cur.fetchall()
    if not rows:
        raise SystemExit(f"Table not found or no columns: {table}")

    cols, key_seen = [], False
    for name, base_type, is_computed, is_nullable in rows:
        base_type = base_type.lower()
        if name == key:
            key_seen = True
            if base_type not in _INT_TYPES or is_nullable or is_computed:
                raise SystemExit(
                    f"Partition column {key} must be a NOT NULL, non-computed integer "
                    f"(found {base_type}, nullable={bool(is_nullable)})"
                )
        if base_type in _UNSUPPORTED:
            raise SystemExit(f"Column {name} has unsupported type {base_type}")
        if is_computed or base_type in _SERVER_GENERATED:
            log.info("Skipping server-generated/computed column %s (%s)", name, base_type)
            continue
        cols.append(name)
    if not key_seen:
        raise SystemExit(f"Partition column {key} not found on {table}")
    return cols


def extract_part(cfg: Config, cred, container: ContainerClient, sql: str,
                 part: Part, tmpdir: Path) -> Part:
    t0 = time.perf_counter()
    path = tmpdir / f"{part.name}.parquet"
    rows, writer = 0, None
    conn = connect(cfg.src, cred)
    try:
        cur = conn.cursor()
        cur.execute(sql, (part.lo, part.hi))
        try:
            while True:
                batch = cur.arrow_batch(cfg.batch_size)
                if batch.num_rows == 0:
                    break
                if writer is None:
                    writer = pq.ParquetWriter(path, batch.schema, compression="zstd")
                writer.write_batch(batch)
                rows += batch.num_rows
        finally:
            if writer is not None:
                writer.close()
    finally:
        conn.close()

    if rows:
        with path.open("rb") as fh:
            container.upload_blob(part.blob, fh, overwrite=True,
                                  max_concurrency=BLOB_CONCURRENCY)
        path.unlink()
    part.rows = rows
    log.info("%s [%d..%d]: %s rows in %.1fs", part.name, part.lo, part.hi,
             f"{rows:,}", time.perf_counter() - t0)
    return part


def extract(cfg: Config, cred, container: ContainerClient, run_id: str,
            table: str, key: str, parts: int) -> Manifest:
    conn = connect(cfg.src, cred)
    try:
        cur = conn.cursor()
        columns = source_columns(cur, table, key)
        k = ident(key)
        cur.execute(f"SELECT MIN({k}), MAX({k}), COUNT_BIG(*) FROM {qualified(table)}")
        lo, hi, count = cur.fetchone()
    finally:
        conn.close()

    manifest = Manifest(run_id=run_id, source_table=table, partition_column=key,
                        columns=columns, source_count=count,
                        created_utc=datetime.now(timezone.utc).isoformat())
    if lo is None:
        log.warning("%s is empty; nothing to extract", table)
    else:
        manifest.parts = [
            Part(name=f"part-{i:04d}", lo=a, hi=b, blob=part_blob(run_id, i))
            for i, (a, b) in enumerate(split_ranges(lo, hi, parts))
        ]
    log.info("Extract %s: %s rows, key range [%s..%s], %d parts, %d workers",
             table, f"{count:,}", lo, hi, len(manifest.parts), cfg.workers)

    sql = select_sql(table, columns, key)
    with tempfile.TemporaryDirectory(prefix="copy_table_") as tmp:
        run_parallel(cfg.workers, [
            (extract_part, (cfg, cred, container, sql, p, Path(tmp))) for p in manifest.parts
        ])

    for p in manifest.parts:
        if p.rows == 0:
            p.blob = None
    container.upload_blob(manifest_blob(run_id), manifest.to_json(), overwrite=True)

    if manifest.total_rows != count:
        # Concurrent writes on the source between COUNT and the range scans.
        log.warning("Extracted %s rows but source COUNT_BIG(*) was %s at start",
                    f"{manifest.total_rows:,}", f"{count:,}")
    log.info("Extract done: %s rows -> %s/%s", f"{manifest.total_rows:,}",
             cfg.container, run_id)
    return manifest


# ── load ─────────────────────────────────────────────────────────────────────

def _row_tuples(pf: pq.ParquetFile, batch_size: int):
    for batch in pf.iter_batches(batch_size=batch_size):
        yield from zip(*(col.to_pylist() for col in batch.columns))


def load_part(cfg: Config, cred, container: ContainerClient, dest: str,
              manifest: Manifest, part: Part, tmpdir: Path) -> int:
    if not part.rows:
        return 0
    t0 = time.perf_counter()
    path = tmpdir / f"{part.name}.parquet"
    with path.open("wb") as fh:
        container.download_blob(part.blob, max_concurrency=BLOB_CONCURRENCY).readinto(fh)

    opts = dict(batch_size=cfg.batch_size, timeout=cfg.bulk_timeout,
                column_mappings=manifest.columns, keep_identity=True, keep_nulls=True,
                check_constraints=cfg.check_constraints, table_lock=cfg.table_lock,
                use_internal_transaction=True)
    conn = connect(cfg.dst, cred)
    try:
        cur = conn.cursor()
        pf = pq.ParquetFile(path)
        if cfg.row_mode:
            result = cur.bulkcopy(dest, _row_tuples(pf, cfg.batch_size), **opts)
        else:
            reader = pa.RecordBatchReader.from_batches(
                pf.schema_arrow, pf.iter_batches(batch_size=cfg.batch_size))
            result = cur.bulkcopy_arrow(dest, reader, **opts)
    finally:
        conn.close()
        path.unlink(missing_ok=True)

    copied = result["rows_copied"]
    if copied != part.rows:
        raise RuntimeError(f"{part.name}: copied {copied:,} rows, manifest says {part.rows:,}")
    log.info("%s: %s rows loaded in %.1fs", part.name, f"{copied:,}", time.perf_counter() - t0)
    return copied


def load(cfg: Config, cred, container: ContainerClient, manifest: Manifest, dest: str) -> None:
    dest_q = qualified(dest)  # validates; bracketed form for T-SQL
    dest = ".".join(parse_table(dest))  # bulkcopy takes plain 'schema.table'
    log.info("Load %s -> %s: %s rows, %d parts, %d workers, %s",
             manifest.source_table, dest, f"{manifest.total_rows:,}", len(manifest.parts),
             cfg.workers, "bulkcopy (row mode)" if cfg.row_mode else "bulkcopy_arrow")

    conn = connect(cfg.dst, cred)
    try:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {dest_q}")
        conn.commit()
        log.info("Truncated %s", dest)

        with tempfile.TemporaryDirectory(prefix="copy_table_") as tmp:
            run_parallel(cfg.workers, [
                (load_part, (cfg, cred, container, dest, manifest, p, Path(tmp)))
                for p in manifest.parts
            ])

        cur.execute(f"SELECT COUNT_BIG(*) FROM {dest_q}")
        (dest_count,) = cur.fetchone()
    finally:
        conn.close()

    if dest_count != manifest.total_rows:
        raise RuntimeError(
            f"Row count mismatch: dest {dest_count:,} vs manifest {manifest.total_rows:,}")
    log.info("Load done: %s rows verified in %s", f"{dest_count:,}", dest)


def read_manifest(container: ContainerClient, run_id: str) -> Manifest:
    text = container.download_blob(manifest_blob(run_id)).readall().decode("utf-8")
    return Manifest.from_json(text)


# ── orchestration ────────────────────────────────────────────────────────────

def run_parallel(workers: int, jobs: list[tuple]) -> None:
    """Run (fn, args) jobs; wait for all, then raise the first failure."""
    if not jobs:
        return
    failures = []
    with ThreadPoolExecutor(max_workers=min(workers, len(jobs))) as ex:
        futures = [ex.submit(fn, *args) for fn, args in jobs]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:  # noqa: BLE001 - collected and re-raised below
                log.error("Worker failed: %s", e)
                failures.append(e)
    if failures:
        raise RuntimeError(f"{len(failures)}/{len(jobs)} parts failed") from failures[0]


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--phase", choices=["all", "extract", "load"], default="all")
    p.add_argument("--table", help="schema.table on source (required for extract)")
    p.add_argument("--dest-table", help="schema.table on dest (default: same as source)")
    p.add_argument("--partition-column", help="NOT NULL integer column to range-split on (required for extract)")
    p.add_argument("--parts", type=int, default=8, help="number of blobs to split into")
    p.add_argument("--workers", type=int, default=4, help="parallel connections per phase")
    p.add_argument("--batch-size", type=int, default=64_000, help="rows per Arrow batch / row group / bulk batch")
    p.add_argument("--bulk-timeout", type=int, default=3600, help="bulkcopy timeout per part (s)")
    p.add_argument("--run-id", help="blob folder; default UTC timestamp. Required for --phase load")
    p.add_argument("--table-lock", action="store_true",
                   help="TABLOCK during load. Only use with --workers 1, or with >1 if dest is a heap "
                        "(TABLOCK on a clustered table serialises parallel loads)")
    p.add_argument("--no-check-constraints", action="store_true",
                   help="skip CHECK constraints during load (leaves them untrusted)")
    p.add_argument("--row-mode", action="store_true",
                   help="use classic bulkcopy() with Python tuples instead of bulkcopy_arrow(). "
                        "Slower; use if bulkcopy_arrow rejects a column type")
    args = p.parse_args(argv)

    if args.phase in ("all", "extract") and not (args.table and args.partition_column):
        p.error("--table and --partition-column are required for extract")
    if args.phase == "load" and not args.run_id:
        p.error("--run-id is required for --phase load")
    for name in ("parts", "workers", "batch_size"):
        if getattr(args, name) < 1:
            p.error(f"--{name.replace('_', '-')} must be >= 1")
    return args


def main(argv=None) -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)

    args = parse_args(argv)
    cfg = Config(
        src=env("SOURCE_CONNECTION_STRING"), dst=env("DEST_CONNECTION_STRING"),
        account_url=env("STORAGE_ACCOUNT_URL"), container=env("STORAGE_CONTAINER"),
        batch_size=args.batch_size, workers=args.workers, bulk_timeout=args.bulk_timeout,
        table_lock=args.table_lock, check_constraints=not args.no_check_constraints,
        row_mode=args.row_mode,
    )
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cred = DefaultAzureCredential()
    container = ContainerClient(cfg.account_url, cfg.container, credential=cred)

    t0 = time.perf_counter()
    try:
        manifest = None
        if args.phase in ("all", "extract"):
            manifest = extract(cfg, cred, container, run_id, args.table,
                               args.partition_column, args.parts)
        if args.phase in ("all", "load"):
            if manifest is None:
                manifest = read_manifest(container, run_id)
            load(cfg, cred, container, manifest,
                 args.dest_table or args.table or manifest.source_table)
    except Exception as e:  # noqa: BLE001 - top-level: report and fail the process
        log.error("FAILED (run_id=%s): %s", run_id, e)
        return 1
    log.info("Total %.1fs (run_id=%s)", time.perf_counter() - t0, run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
