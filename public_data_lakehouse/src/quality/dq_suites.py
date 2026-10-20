"""Data-quality suites per layer, built from the reusable checks in common.data_quality.

Results are appended to gold.data_quality_results. That table is written only by these DQ steps and
is never read by Gold objects, so there is no circular dependency.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import data_quality as dq
from common.config import ProjectConfig
from transformations.gold_transformations import discover_gold_objects
from transformations.silver_transformations import latest_snapshot

CRIT, WARN = dq.CRITICAL, dq.WARNING


def _runner(spark: SparkSession, cfg: ProjectConfig, run_id: str, layer: str) -> dq.DataQualityRunner:
    return dq.DataQualityRunner(spark, cfg.gold(cfg.tables["dq_results"]), run_id, layer)


def run_bronze_dq(spark: SparkSession, cfg: ProjectConfig, run_id: str) -> dict:
    r = _runner(spark, cfg, run_id, "bronze")
    th = cfg.data_quality
    log = spark.table(cfg.bronze(cfg.tables["ingestion_log"])).filter(F.col("run_id") == run_id)
    r.add(dq.violations(log, "bronze", "_ingestion_log", "ingestion_status_not_failed",
                        F.col("status") == "FAILED"))
    r.add(dq.make_result("bronze", "_ingestion_log", "all_sources_logged", log.count(), "==",
                         len(cfg.sources), CRIT, "one ingestion log row per configured source this run"))
    for name, src in cfg.sources.items():
        df = latest_snapshot(spark.table(cfg.bronze(name)))
        r.add(dq.row_count_at_least(df, "bronze", name, th["min_bronze_rows"].get(name, 1)))
        r.add(dq.violations(df, "bronze", name, "corrupt_record_ratio", F.col("_corrupt_record").isNotNull(),
                            CRIT, max_ratio=th["max_corrupt_record_ratio"]))
        r.add(dq.not_null(df, "bronze", name, ["_ingested_at", "_source_file", "_snapshot_id"]))
        if src.primary_key:
            # Raw layer: duplicates are reported here and resolved in silver.
            r.add(dq.unique_key(df, "bronze", name, src.primary_key, WARN))
            r.add(dq.not_null(df, "bronze", name, src.primary_key, WARN))
    return r.finalize()


def run_silver_dq(spark: SparkSession, cfg: ProjectConfig, run_id: str) -> dict:
    r = _runner(spark, cfg, run_id, "silver")
    rules, th = cfg.business_rules, cfg.data_quality
    t = {n: spark.table(cfg.silver(n)) for n in (
        "orders", "order_items", "payments", "reviews", "customers", "sellers", "products",
        "geolocation_zip", "fact_order_details", "dim_customer", "dim_product", "dim_seller", "dim_date")}

    pks = {"orders": ["order_id"], "order_items": ["order_id", "order_item_id"],
           "payments": ["order_id", "payment_sequential"], "reviews": ["review_id", "order_id"],
           "customers": ["customer_id"], "sellers": ["seller_id"], "products": ["product_id"],
           "geolocation_zip": ["zip_code_prefix"], "fact_order_details": ["order_id", "order_item_id"],
           "dim_customer": ["customer_unique_id"], "dim_product": ["product_id"],
           "dim_seller": ["seller_id"], "dim_date": ["date"]}
    for name, keys in pks.items():
        r.add(dq.unique_key(t[name], "silver", name, keys))
        r.add(dq.not_null(t[name], "silver", name, keys))

    # referential integrity
    fks = [("order_items", "orders", ["order_id"]), ("order_items", "products", ["product_id"]),
           ("order_items", "sellers", ["seller_id"]), ("payments", "orders", ["order_id"]),
           ("reviews", "orders", ["order_id"]), ("orders", "customers", ["customer_id"])]
    for child, parent, keys in fks:
        r.add(dq.referential_integrity(t[child], t[parent], "silver", child, keys, parent))

    # domain / validity checks
    r.add(dq.accepted_values(t["orders"], "silver", "orders", "order_status", rules["known_order_statuses"]))
    r.add(dq.accepted_values(t["payments"], "silver", "payments", "payment_type", rules["known_payment_types"]))
    r.add(dq.non_negative(t["order_items"], "silver", "order_items", ["price", "freight_value"]))
    r.add(dq.non_negative(t["payments"], "silver", "payments", ["payment_value", "payment_installments"]))
    r.add(dq.violations(t["orders"], "silver", "orders", "inconsistent_timestamps",
                        F.col("has_inconsistent_timestamps"), WARN, max_ratio=0.01))
    r.add(dq.violations(t["orders"], "silver", "orders", "delivered_without_delivery_date",
                        F.col("is_delivered_without_date"), WARN, max_ratio=0.001))
    r.add(dq.violations(t["products"], "silver", "products", "category_not_translated",
                        F.col("category_resolution") != "translated", WARN, max_ratio=0.05))
    r.add(dq.violations(t["payments"], "silver", "payments", "zero_installments",
                        F.col("has_zero_installments"), WARN, max_ratio=0.001))

    # row reconciliation: bronze = corrupt + rejected + duplicates_removed + silver, nothing lost silently
    recon = spark.table(cfg.silver("_reconciliation"))
    r.add(dq.violations(recon, "silver", "_reconciliation", "rows_accounted_for",
                        F.col("bronze_rows") != F.col("corrupt_rows") + F.col("rejected_rows") +
                        F.col("duplicates_removed") + F.col("silver_rows")))
    r.add(dq.violations(recon, "silver", "_reconciliation", "duplicates_removed_non_negative",
                        F.col("duplicates_removed") < 0))

    # join multiplication / fact integrity
    fact, items = t["fact_order_details"], t["order_items"]
    r.add(dq.values_equal("silver", "fact_order_details", "fact_rows_equal_order_items",
                          fact.count(), items.count()))
    r.add(dq.values_equal("silver", "fact_order_details", "fact_price_equals_items_price",
                          dq.scalar(fact, F.sum("price")), dq.scalar(items, F.sum("price")), 0.01))
    paid = (t["payments"].join(items.select("order_id").distinct(), "order_id")
            .agg(F.sum("payment_value").alias("v")).first()["v"] or 0)
    r.add(dq.values_equal("silver", "fact_order_details", "allocated_payments_equal_payments",
                          dq.scalar(fact, F.sum("item_payment_allocated")), float(paid), 0.05))

    # order items vs payments
    tol = rules["payment_reconciliation_tolerance"]
    order_level = fact.groupBy("order_id").agg(F.max("order_items_total").alias("items"),
                                               F.max("order_payment_total").alias("paid"))
    r.add(dq.violations(order_level.filter(F.col("paid").isNotNull()), "silver", "fact_order_details",
                        "order_items_vs_payments_mismatch", F.abs(F.col("paid") - F.col("items")) > tol,
                        WARN, max_ratio=th["max_payment_mismatch_ratio"],
                        detail=f"tolerance {tol} BRL; differences come from installment interest/vouchers"))
    r.add(dq.violations(order_level, "silver", "fact_order_details", "orders_with_items_missing_payment",
                        F.col("paid").isNull(), WARN, max_ratio=0.001))
    return r.finalize()


def run_gold_dq(spark: SparkSession, cfg: ProjectConfig, run_id: str) -> dict:
    r = _runner(spark, cfg, run_id, "gold")
    expected_type = "MATERIALIZED_VIEW" if cfg.gold_object_type == "materialized_view" else "MANAGED"
    objs = {o.base_name: cfg.gold_object_name(o.base_name) for o in discover_gold_objects()}
    info = (spark.table(f"`{cfg.catalog}`.information_schema.tables")
            .filter(F.col("table_schema") == cfg.schemas["gold"]).select("table_name", "table_type"))
    types = {row["table_name"]: row["table_type"] for row in info.collect()}  # tiny metadata result
    for base, name in objs.items():
        r.add(dq.CheckResult("gold", name, "object_exists_with_expected_type", CRIT,
                             types.get(name) == expected_type, None, f"table_type = {expected_type}",
                             f"found table_type={types.get(name)}"))
        if name in types:
            r.add(dq.row_count_at_least(spark.table(cfg.gold(name)), "gold", name, 1))

    g = lambda base: spark.table(cfg.gold(objs[base]))  # noqa: E731
    fact = spark.table(cfg.silver("fact_order_details"))
    sale = fact.filter("is_sale_eligible")
    sale_gmv = dq.scalar(sale, F.sum("price"))
    sale_orders = sale.select("order_id").distinct().count()

    r.add(dq.values_equal("gold", objs["daily_sales"], "gmv_reconciles_to_fact",
                          dq.scalar(g("daily_sales"), F.sum("gmv")), sale_gmv, 0.01))
    r.add(dq.values_equal("gold", objs["monthly_revenue"], "gmv_reconciles_to_fact",
                          dq.scalar(g("monthly_revenue"), F.sum("gmv")), sale_gmv, 0.01))
    r.add(dq.values_equal("gold", objs["monthly_revenue"], "order_count_reconciles_to_fact",
                          dq.scalar(g("monthly_revenue"), F.sum("order_count")), sale_orders))
    r.add(dq.values_equal("gold", objs["product_performance"], "revenue_share_sums_to_100",
                          dq.scalar(g("product_performance"), F.sum("revenue_share_pct")), 100.0, 0.01))
    r.add(dq.values_equal("gold", objs["seller_performance"], "gmv_reconciles_to_fact",
                          dq.scalar(g("seller_performance"), F.sum("gmv")), sale_gmv, 0.01))
    r.add(dq.values_equal("gold", objs["customer_analytics"], "customers_reconcile_to_fact",
                          g("customer_analytics").count(), sale.select("customer_unique_id").distinct().count()))
    r.add(dq.values_equal("gold", objs["customer_state_summary"], "customers_reconcile_to_customer_mv",
                          dq.scalar(g("customer_state_summary"), F.sum("customer_count")),
                          g("customer_analytics").count()))
    r.add(dq.values_equal("gold", objs["order_fulfillment"], "orders_reconcile_to_silver",
                          dq.scalar(g("order_fulfillment"), F.sum("order_count")),
                          spark.table(cfg.silver("orders")).count()))
    r.add(dq.values_equal("gold", objs["payment_analytics"], "payments_reconcile_to_silver",
                          dq.scalar(g("payment_analytics"), F.sum("payment_value")),
                          dq.scalar(spark.table(cfg.silver("payments")), F.sum("payment_value")), 0.01))
    r.add(dq.violations(g("monthly_revenue"), "gold", objs["monthly_revenue"], "non_negative_gmv",
                        F.col("gmv") < 0))
    return r.finalize()
