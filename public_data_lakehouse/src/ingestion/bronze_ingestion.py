"""Bronze layer: download public source files into a UC volume and load them into raw Delta tables.

Load strategy (the public source has no CDC / change tracking):
  * Each downloaded file is fingerprinted with SHA-256. One distinct file = one *snapshot*.
  * snapshot mode: rows are written with replaceWhere on `_snapshot_id`, so re-running the same
    file replaces exactly that snapshot (no duplicates). An unchanged file is skipped.
  * full_refresh mode: the table is overwritten with the current snapshot only.
Malformed CSV rows are never dropped: they stay in bronze with `_corrupt_record` populated and
are copied to the quarantine table with a reason.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import ProjectConfig, SourceConfig
from common.logging_utils import get_logger, log
from common.spark_session import is_databricks_runtime

logger = get_logger("bronze")

CORRUPT_COL = "_corrupt_record"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

INGESTION_LOG_DDL = (
    "run_id STRING, source_name STRING, table_name STRING, source_url STRING, "
    "landing_path STRING, file_sha256 STRING, file_bytes BIGINT, snapshot_id STRING, "
    "load_mode STRING, status STRING, row_count BIGINT, corrupt_record_count BIGINT, "
    "downloaded_at TIMESTAMP, started_at TIMESTAMP, duration_s DOUBLE, error_message STRING"
)
QUARANTINE_DDL = (
    "run_id STRING, source_name STRING, snapshot_id STRING, source_file STRING, reason STRING, "
    "raw_record STRING, quarantined_at TIMESTAMP"
)


class SourceValidationError(RuntimeError):
    pass


@dataclass
class DownloadedFile:
    local_path: str
    sha256: str
    size_bytes: int
    downloaded_at: datetime

    @property
    def snapshot_id(self) -> str:
        return self.sha256[:16]


# ---- pure helpers (unit tested) ------------------------------------------------------------

def parse_header(first_line: str) -> list[str]:
    """Parse a CSV header line, tolerating a UTF-8 BOM and quoted names."""
    line = first_line.lstrip("﻿").strip("\r\n")
    return [c.strip().strip('"') for c in line.split(",")]


def header_mismatches(actual: list[str], expected: list[str]) -> list[str]:
    problems = []
    if len(actual) != len(expected):
        problems.append(f"expected {len(expected)} columns, found {len(actual)}")
    for i, (a, e) in enumerate(zip(actual, expected)):
        if a != e:
            problems.append(f"column {i}: expected '{e}', found '{a}'")
    return problems


def sha256_of(path: str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def add_ingestion_metadata(df: DataFrame, source_name: str, snapshot_id: str, file_sha256: str,
                           ingested_at: datetime) -> DataFrame:
    return (df
            .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
            .withColumn("_ingestion_date", F.to_date(F.col("_ingested_at")))
            .withColumn("_source_name", F.lit(source_name))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_snapshot_id", F.lit(snapshot_id))
            .withColumn("_source_file_sha256", F.lit(file_sha256)))


# ---- download & landing -------------------------------------------------------------------

def download(src: SourceConfig, retries: int = 3, timeout: int = 120) -> DownloadedFile:
    tmp_dir = tempfile.mkdtemp(prefix=f"lakehouse_{src.name}_")
    local_path = os.path.join(tmp_dir, src.file)
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(src.url, headers={"User-Agent": "public_data_lakehouse/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp, open(local_path, "wb") as out:
                if resp.status != 200:
                    raise SourceValidationError(f"HTTP {resp.status} for {src.url}")
                shutil.copyfileobj(resp, out)
            break
        except Exception as exc:  # network errors are retried, then surfaced
            last_err = exc
            log(logger, "download attempt failed", source=src.name, attempt=attempt, error=str(exc))
            time.sleep(2 * attempt)
    else:
        raise SourceValidationError(f"Download failed for {src.url}: {last_err}")

    size = os.path.getsize(local_path)
    if size == 0:
        raise SourceValidationError(f"Empty source file downloaded from {src.url}")
    with open(local_path, encoding="utf-8-sig", errors="replace") as fh:
        problems = header_mismatches(parse_header(fh.readline()), src.columns)
    if problems:
        raise SourceValidationError(f"Source schema drift in {src.file}: {problems}")
    return DownloadedFile(local_path, sha256_of(local_path), size,
                          datetime.now(timezone.utc).replace(tzinfo=None))


def land_file(cfg: ProjectConfig, src: SourceConfig, f: DownloadedFile) -> str:
    """Copy into /Volumes/<catalog>/<bronze>/<volume>/<subdir>/<source>/<snapshot>/<file>."""
    target_dir = f"{cfg.landing_path}/{src.name}/{f.snapshot_id}"
    target = f"{target_dir}/{src.file}"
    if is_databricks_runtime():
        if not os.path.exists(target):
            os.makedirs(target_dir, exist_ok=True)
            shutil.copyfile(f.local_path, target)
    else:  # local run via Databricks Connect: upload through the Files API
        from databricks.sdk import WorkspaceClient

        with open(f.local_path, "rb") as fh:
            WorkspaceClient().files.upload(target, fh, overwrite=True)
    return target


# ---- read & write -------------------------------------------------------------------------

def read_source_csv(spark: SparkSession, path: str, src: SourceConfig) -> DataFrame:
    return (spark.read.format("csv")
            .schema(f"{src.ddl_schema}, `{CORRUPT_COL}` STRING")
            .option("header", "true")
            .option("multiLine", "true")          # review comments contain embedded newlines
            .option("quote", '"')
            .option("escape", '"')
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")         # keep malformed rows, flag them
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .option("timestampFormat", TIMESTAMP_FORMAT)
            .option("enforceSchema", "true")      # header verified separately in download()
            .load(path))


def _table_exists(spark: SparkSession, fqn: str) -> bool:
    return spark.catalog.tableExists(fqn.replace("`", ""))


def write_bronze(spark: SparkSession, df: DataFrame, table: str, snapshot_id: str,
                 load_mode: str) -> None:
    writer = df.write.format("delta")
    if not _table_exists(spark, table):
        writer.mode("append").saveAsTable(table)  # first load creates the table
    elif load_mode == "full_refresh":
        writer.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    else:
        writer.mode("overwrite").option("replaceWhere", f"_snapshot_id = '{snapshot_id}'").saveAsTable(table)


def last_loaded_hash(spark: SparkSession, log_table: str, source: str) -> str | None:
    if not _table_exists(spark, log_table):
        return None
    row = (spark.table(log_table)
           .filter((F.col("source_name") == source) & F.col("status").isin("LOADED", "SKIPPED_UNCHANGED"))
           .orderBy(F.col("started_at").desc()).select("file_sha256").first())
    return row["file_sha256"] if row else None


def quarantine_corrupt_rows(spark: SparkSession, cfg: ProjectConfig, table: str, src: SourceConfig,
                            snapshot_id: str, run_id: str) -> int:
    q_table = cfg.bronze(cfg.tables["quarantine"])
    spark.sql(f"CREATE TABLE IF NOT EXISTS {q_table} ({QUARANTINE_DDL}) "
              "COMMENT 'Rows that failed CSV parsing or type conversion during bronze ingestion'")
    spark.sql(f"DELETE FROM {q_table} WHERE source_name = '{src.name}' AND snapshot_id = '{snapshot_id}'")
    bad = (spark.table(table)
           .filter((F.col("_snapshot_id") == snapshot_id) & F.col(CORRUPT_COL).isNotNull())
           .select(F.lit(run_id).alias("run_id"), F.lit(src.name).alias("source_name"),
                   F.lit(snapshot_id).alias("snapshot_id"), F.col("_source_file").alias("source_file"),
                   F.lit("CSV parse or type-conversion failure (PERMISSIVE mode)").alias("reason"),
                   F.col(CORRUPT_COL).alias("raw_record"),
                   F.current_timestamp().alias("quarantined_at")))
    bad.write.mode("append").saveAsTable(q_table)
    return spark.table(q_table).filter(
        (F.col("source_name") == src.name) & (F.col("snapshot_id") == snapshot_id)).count()


# ---- orchestration ------------------------------------------------------------------------

def ingest_source(spark: SparkSession, cfg: ProjectConfig, src: SourceConfig, run_id: str,
                  force_reload: bool = False) -> dict:
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    t0 = time.monotonic()
    table = cfg.bronze(src.name)
    entry = dict(run_id=run_id, source_name=src.name, table_name=table, source_url=src.url,
                 landing_path=None, file_sha256=None, file_bytes=None, snapshot_id=None,
                 load_mode=cfg.load_mode, status="FAILED", row_count=None, corrupt_record_count=None,
                 downloaded_at=None, started_at=started, duration_s=None, error_message=None)
    try:
        f = download(src)
        entry.update(file_sha256=f.sha256, file_bytes=f.size_bytes, snapshot_id=f.snapshot_id,
                     downloaded_at=f.downloaded_at)
        landed = land_file(cfg, src, f)
        entry["landing_path"] = landed

        unchanged = (cfg.load_mode == "snapshot" and not force_reload and _table_exists(spark, table)
                     and last_loaded_hash(spark, cfg.bronze(cfg.tables["ingestion_log"]), src.name) == f.sha256)
        if unchanged:
            entry["status"] = "SKIPPED_UNCHANGED"
        else:
            df = add_ingestion_metadata(read_source_csv(spark, landed, src), src.name,
                                        f.snapshot_id, f.sha256, started)
            write_bronze(spark, df, table, f.snapshot_id, cfg.load_mode)
            entry["status"] = "LOADED"

        snap = spark.table(table).filter(F.col("_snapshot_id") == f.snapshot_id)
        entry["row_count"] = snap.count()
        entry["corrupt_record_count"] = quarantine_corrupt_rows(spark, cfg, table, src, f.snapshot_id, run_id)
        if entry["row_count"] == 0:
            raise SourceValidationError(f"{src.file} produced 0 rows")
    except Exception as exc:
        entry["status"] = "FAILED"
        entry["error_message"] = f"{type(exc).__name__}: {exc}"[:2000]
    entry["duration_s"] = round(time.monotonic() - t0, 2)
    log(logger, "source ingested", **{k: entry[k] for k in
        ("source_name", "status", "row_count", "corrupt_record_count", "snapshot_id", "duration_s", "error_message")})
    return entry


def run_bronze_ingestion(spark: SparkSession, cfg: ProjectConfig, run_id: str,
                         force_reload: bool = False) -> list[dict]:
    log_table = cfg.bronze(cfg.tables["ingestion_log"])
    spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} ({INGESTION_LOG_DDL}) "
              "COMMENT 'One row per source file per pipeline run: counts, hashes, durations, failures'")
    entries = [ingest_source(spark, cfg, src, run_id, force_reload) for src in cfg.sources.values()]
    spark.createDataFrame([tuple(e[k] for k in _log_columns()) for e in entries], INGESTION_LOG_DDL) \
        .write.mode("append").saveAsTable(log_table)

    for e in entries:
        if e["status"] != "FAILED":
            src = cfg.sources[e["source_name"]]
            spark.sql(f"COMMENT ON TABLE {e['table_name']} IS 'Raw {src.file} from {cfg.source_name} "
                      f"({src.url}). Source columns preserved; underscore-prefixed columns are "
                      "ingestion metadata.'")

    failed = [e for e in entries if e["status"] == "FAILED"]
    if failed:
        raise RuntimeError("Bronze ingestion failed for: " +
                           "; ".join(f"{e['source_name']}: {e['error_message']}" for e in failed))
    return entries


def _log_columns() -> list[str]:
    return [c.split()[0] for c in INGESTION_LOG_DDL.split(", ")]
