"""Pipeline entry point.

Used by every Python task of the Databricks jobs (one --step per task) and for local runs through
Databricks Connect (`--step all`). Steps:

  validate -> bronze -> bronze_dq -> silver -> silver_dq -> gold -> gold_dq -> summary
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path


def _bootstrap_path(project_root: str | None) -> None:
    """Make `src/` importable when run as a Databricks python file task or locally."""
    candidates = []
    if project_root:
        candidates.append(Path(project_root) / "src")
    try:
        candidates.append(Path(__file__).resolve().parents[1])
    except NameError:  # __file__ is not always defined on Databricks
        pass
    for c in candidates:
        if (c / "common").is_dir() and str(c) not in sys.path:
            sys.path.insert(0, str(c))
            return
    raise RuntimeError(f"Could not locate project src/ directory (tried {candidates})")


STEPS = ["validate", "bronze", "bronze_dq", "silver", "silver_dq", "gold", "gold_dq", "summary"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="public_data_lakehouse pipeline")
    p.add_argument("--step", required=True, choices=STEPS + ["all"])
    p.add_argument("--catalog", default=None, help="overrides config / LAKEHOUSE_CATALOG")
    p.add_argument("--run-id", default=None, help="defaults to a generated id")
    p.add_argument("--project-root", default=None)
    p.add_argument("--force-reload", action="store_true", help="reload bronze even if files are unchanged")
    p.add_argument("--gold-object-type", choices=["materialized_view", "table"], default=None)
    for layer in ("bronze", "silver", "gold"):
        p.add_argument(f"--{layer}-schema", default=None, help=f"overrides schemas.{layer} in the config")
    return p.parse_args([a for a in (argv if argv is not None else sys.argv[1:]) if a != ""])


def validate_environment(spark, cfg) -> dict:
    """Fail fast if the catalog is not usable; create schemas and the landing volume (idempotent)."""
    user = spark.sql("SELECT current_user() AS u").first()["u"]
    catalogs = {r[0] for r in spark.sql("SHOW CATALOGS").collect()}
    if cfg.catalog not in catalogs:
        raise RuntimeError(f"Catalog '{cfg.catalog}' not found or not visible to {user}. "
                           "Set LAKEHOUSE_CATALOG / --catalog to a catalog you can use.")
    for layer in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fqn(layer)} "
                  f"COMMENT '{cfg.project_name}: {layer} layer (Olist Brazilian e-commerce)'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema_fqn('bronze')}.`{cfg.landing_volume}` "
              "COMMENT 'Landing zone for downloaded public source files'")
    return {"user": user, "catalog": cfg.catalog, "schemas": cfg.schemas}


def summarize(spark, cfg, run_id: str) -> None:
    from pyspark.sql import functions as F

    print("=" * 80)
    print(f"public_data_lakehouse summary | catalog={cfg.catalog} | run_id={run_id}")
    for layer in ("bronze", "silver", "gold"):
        rows = spark.sql(f"SHOW TABLES IN {cfg.schema_fqn(layer)}").collect()
        print(f"\n[{layer}]")
        for row in sorted(rows, key=lambda x: x["tableName"]):
            n = spark.table(cfg.table_fqn(layer, row["tableName"])).count()
            print(f"  {row['tableName']:<35} {n:>12,} rows")
    dq_tbl = cfg.gold(cfg.tables["dq_results"])
    if spark.catalog.tableExists(dq_tbl.replace("`", "")):
        latest = spark.table(dq_tbl).agg(F.max("executed_at")).first()[0]
        print("\n[data quality: latest results per layer]")
        w_rows = (spark.table(dq_tbl).groupBy("layer", "status").count().orderBy("layer", "status").collect())
        for row in w_rows:
            print(f"  {row['layer']:<8} {row['status']:<5} {row['count']:>6}")
        print(f"  latest check at {latest}")
    print("=" * 80)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    _bootstrap_path(args.project_root)

    from common.config import load_config
    from common.logging_utils import get_logger, log, timed

    logger = get_logger("pipeline")
    from dataclasses import replace

    cfg = load_config(catalog=args.catalog)
    if args.gold_object_type:
        cfg = replace(cfg, gold_object_type=args.gold_object_type)
    schema_overrides = {l: getattr(args, f"{l}_schema") for l in ("bronze", "silver", "gold")}
    cfg = replace(cfg, schemas={l: schema_overrides[l] or cfg.schemas[l] for l in cfg.schemas})
    run_id = args.run_id or f"local-{uuid.uuid4().hex[:12]}"
    steps = STEPS if args.step == "all" else [args.step]
    log(logger, "pipeline start", run_id=run_id, steps=steps, catalog=cfg.catalog,
        load_mode=cfg.load_mode, gold_object_type=cfg.gold_object_type)

    # Gold MVs run on a SQL warehouse; don't hold a Spark session for that step (Free Edition limits
    # concurrent serverless compute, see gold_transformations).
    spark = None
    if any(s != "gold" for s in steps) or cfg.gold_object_type == "table":
        from common.spark_session import get_spark
        spark = get_spark()

    for step in steps:
        with timed(logger, step, run_id=run_id) as res:
            if step == "validate":
                res.update(validate_environment(spark, cfg))
            elif step == "bronze":
                from ingestion.bronze_ingestion import run_bronze_ingestion
                entries = run_bronze_ingestion(spark, cfg, run_id, args.force_reload)
                res["sources"] = {e["source_name"]: f"{e['status']}:{e['row_count']}" for e in entries}
            elif step == "silver":
                from transformations.silver_transformations import build_silver
                res["tables"] = len(build_silver(spark, cfg, run_id))
            elif step == "gold":
                from transformations import gold_transformations as gt
                if cfg.gold_object_type == "table":
                    res["objects"] = gt.run_gold_tables_fallback(spark, cfg)
                else:
                    res["objects"] = gt.create_on_warehouse(cfg)
            elif step.endswith("_dq"):
                from quality import dq_suites
                res["dq"] = getattr(dq_suites, f"run_{step}")(spark, cfg, run_id)
            elif step == "summary":
                summarize(spark, cfg, run_id)
    log(logger, "pipeline finished", run_id=run_id)


if __name__ == "__main__":
    main()
