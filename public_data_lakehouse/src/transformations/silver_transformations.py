"""Silver layer: clean, standardise, validate, deduplicate and integrate bronze data.

All transformation functions are pure (DataFrame in -> DataFrame out) so they can be unit tested;
`build_silver` wires them to the bronze/silver tables. Silver is rebuilt deterministically from the
latest bronze snapshot on every run (idempotent).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.config import ProjectConfig
from common.logging_utils import get_logger, log

logger = get_logger("silver")

CORRUPT_COL = "_corrupt_record"
REJECTED_COLUMNS = ["table_name", "business_key", "reason", "record_json", "_silver_processed_at"]


# ---- generic reusable helpers -------------------------------------------------------------

def clean_str(col: str | Column) -> Column:
    """Trim, collapse internal whitespace, and turn empty strings into NULL."""
    c = F.regexp_replace(F.trim(F.col(col) if isinstance(col, str) else col), r"\s+", " ")
    return F.when(c == "", None).otherwise(c)


def normalize_zip_prefix(col: str) -> Column:
    """Brazilian CEP prefixes are 5 digits; some source files dropped the leading zero."""
    c = F.trim(F.col(col))
    return F.when(c.rlike(r"^\d{1,5}$"), F.lpad(c, 5, "0")).otherwise(None)


def latest_snapshot(df: DataFrame) -> DataFrame:
    """Keep only rows of the most recently ingested bronze snapshot."""
    row = (df.groupBy("_snapshot_id").agg(F.max("_ingested_at").alias("ts"))
           .orderBy(F.col("ts").desc(), F.col("_snapshot_id").desc()).first())
    if row is None:
        return df
    return df.filter(F.col("_snapshot_id") == row["_snapshot_id"])


def to_rejected(df: DataFrame, table: str, keys: list[str], reason: str | Column,
                exclude: tuple[str, ...] = ()) -> DataFrame:
    reason_col = F.lit(reason) if isinstance(reason, str) else reason
    key_col = F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("<null>")) for k in keys]) \
        if keys else F.lit(None).cast("string")
    payload = [F.col(c) for c in df.columns if c not in exclude]
    return df.select(
        F.lit(table).alias("table_name"), key_col.alias("business_key"), reason_col.alias("reason"),
        F.to_json(F.struct(*payload)).alias("record_json"),
        F.current_timestamp().alias("_silver_processed_at"))


def split_valid(df: DataFrame, table: str, keys: list[str],
                rules: list[tuple[str, Column]]) -> tuple[DataFrame, DataFrame]:
    """Apply ordered rejection rules [(reason, violation_condition)].

    Returns (valid_rows, rejected_rows); each rejected row carries the first rule it violated.
    """
    reason: Column = F.lit(None).cast("string")
    for name, cond in reversed(rules):
        reason = F.when(F.coalesce(cond, F.lit(False)), F.lit(name)).otherwise(reason)
    tagged = df.withColumn("_reject_reason", reason)
    valid = tagged.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    bad = tagged.filter(F.col("_reject_reason").isNotNull())
    return valid, to_rejected(bad, table, keys, F.col("_reject_reason"), exclude=("_reject_reason",))


def deduplicate(df: DataFrame, keys: list[str], order_by: list[Column] | None = None) -> DataFrame:
    """Keep one row per business key, chosen deterministically.

    Default order: most recent ingestion first, then a hash of the full row as a stable tiebreak,
    so reruns always pick the same survivor.
    """
    order_by = order_by or [F.col("_ingested_at").desc()]
    row_hash = F.xxhash64(*[F.col(c) for c in df.columns])
    w = Window.partitionBy(*keys).orderBy(*order_by, row_hash.desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def null_key_rules(keys: list[str]) -> list[tuple[str, Column]]:
    return [(f"null business key: {k}", F.col(k).isNull()) for k in keys]


def with_audit(df: DataFrame) -> DataFrame:
    meta = [c for c in df.columns if c.startswith("_") and c not in ("_snapshot_id", "_ingested_at")]
    return (df.drop(*meta)
            .withColumnRenamed("_snapshot_id", "_bronze_snapshot_id")
            .withColumnRenamed("_ingested_at", "_bronze_ingested_at")
            .withColumn("_silver_processed_at", F.current_timestamp()))


# ---- entity cleaning functions ------------------------------------------------------------

@dataclass
class Cleaned:
    df: DataFrame
    rejected: DataFrame
    keys: list[str]


def clean_orders(df: DataFrame, rules: dict[str, Any]) -> Cleaned:
    keys = ["order_id"]
    known = rules["known_order_statuses"]
    non_sale = rules["non_sale_statuses"]
    out = (df
           .withColumn("order_id", clean_str("order_id"))
           .withColumn("customer_id", clean_str("customer_id"))
           .withColumn("order_status", F.lower(clean_str("order_status")))
           .withColumn("is_known_status", F.col("order_status").isin(known))
           .withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
           .withColumn("delivery_days", F.round(
               (F.unix_timestamp("order_delivered_customer_date") -
                F.unix_timestamp("order_purchase_timestamp")) / 86400.0, 2))
           .withColumn("is_late_delivery", F.when(
               F.col("order_delivered_customer_date").isNotNull() &
               F.col("order_estimated_delivery_date").isNotNull(),
               F.to_date("order_delivered_customer_date") > F.to_date("order_estimated_delivery_date")))
           .withColumn("is_delivered_without_date",
                       (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull())
           .withColumn("has_inconsistent_timestamps",
                       F.coalesce(F.col("order_approved_at") < F.col("order_purchase_timestamp"), F.lit(False)) |
                       F.coalesce(F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"), F.lit(False)))
           # Unknown statuses are NOT sale-eligible and stay visible through is_known_status.
           .withColumn("is_sale_eligible", F.col("is_known_status") & ~F.col("order_status").isin(non_sale)))
    valid, rejected = split_valid(out, "orders", keys, null_key_rules(keys) + [
        ("null customer_id", F.col("customer_id").isNull()),
        ("null or unparseable order_purchase_timestamp", F.col("order_purchase_timestamp").isNull()),
    ])
    return Cleaned(deduplicate(valid, keys), rejected, keys)


def clean_order_items(df: DataFrame) -> Cleaned:
    keys = ["order_id", "order_item_id"]
    out = (df.withColumn("order_id", clean_str("order_id"))
           .withColumn("product_id", clean_str("product_id"))
           .withColumn("seller_id", clean_str("seller_id"))
           .withColumn("item_total", (F.col("price") + F.col("freight_value")).cast("decimal(12,2)")))
    valid, rejected = split_valid(out, "order_items", keys, null_key_rules(keys) + [
        ("null product_id", F.col("product_id").isNull()),
        ("null seller_id", F.col("seller_id").isNull()),
        ("null or negative price", F.col("price").isNull() | (F.col("price") < 0)),
        ("null or negative freight_value", F.col("freight_value").isNull() | (F.col("freight_value") < 0)),
    ])
    return Cleaned(deduplicate(valid, keys), rejected, keys)


def clean_payments(df: DataFrame, rules: dict[str, Any]) -> Cleaned:
    keys = ["order_id", "payment_sequential"]
    out = (df.withColumn("order_id", clean_str("order_id"))
           .withColumn("payment_type", F.lower(clean_str("payment_type")))
           .withColumn("is_known_payment_type", F.col("payment_type").isin(rules["known_payment_types"]))
           # 0 installments occurs in the source; kept and flagged rather than "fixed".
           .withColumn("has_zero_installments", F.col("payment_installments") == 0))
    valid, rejected = split_valid(out, "order_payments", keys, null_key_rules(keys) + [
        ("null or negative payment_value", F.col("payment_value").isNull() | (F.col("payment_value") < 0)),
    ])
    return Cleaned(deduplicate(valid, keys), rejected, keys)


def clean_reviews(df: DataFrame) -> Cleaned:
    keys = ["review_id", "order_id"]
    out = (df.withColumn("review_id", clean_str("review_id"))
           .withColumn("order_id", clean_str("order_id"))
           .withColumn("review_comment_title", clean_str("review_comment_title"))
           .withColumn("review_comment_message", clean_str("review_comment_message"))
           .withColumn("has_comment", F.col("review_comment_message").isNotNull()))
    valid, rejected = split_valid(out, "order_reviews", keys, null_key_rules(keys) + [
        ("review_score outside 1..5", ~F.coalesce(F.col("review_score").between(1, 5), F.lit(False))),
    ])
    return Cleaned(deduplicate(valid, keys), rejected, keys)


def clean_customers(df: DataFrame) -> Cleaned:
    keys = ["customer_id"]
    out = (df.withColumn("customer_id", clean_str("customer_id"))
           .withColumn("customer_unique_id", clean_str("customer_unique_id"))
           .withColumn("customer_zip_code_prefix", normalize_zip_prefix("customer_zip_code_prefix"))
           .withColumn("customer_city", F.lower(clean_str("customer_city")))
           .withColumn("customer_state", F.upper(clean_str("customer_state"))))
    valid, rejected = split_valid(out, "customers", keys, null_key_rules(keys) + [
        ("null customer_unique_id", F.col("customer_unique_id").isNull()),
    ])
    return Cleaned(deduplicate(valid, keys), rejected, keys)


def clean_sellers(df: DataFrame) -> Cleaned:
    keys = ["seller_id"]
    out = (df.withColumn("seller_id", clean_str("seller_id"))
           .withColumn("seller_zip_code_prefix", normalize_zip_prefix("seller_zip_code_prefix"))
           .withColumn("seller_city", F.lower(clean_str("seller_city")))
           .withColumn("seller_state", F.upper(clean_str("seller_state"))))
    valid, rejected = split_valid(out, "sellers", keys, null_key_rules(keys))
    return Cleaned(deduplicate(valid, keys), rejected, keys)


def clean_category_translation(df: DataFrame) -> Cleaned:
    keys = ["product_category_name"]
    out = (df.withColumn("product_category_name", F.lower(clean_str("product_category_name")))
           .withColumn("product_category_name_english", F.lower(clean_str("product_category_name_english"))))
    valid, rejected = split_valid(out, "category_translation", keys, null_key_rules(keys))
    return Cleaned(deduplicate(valid, keys), rejected, keys)


def clean_products(df: DataFrame, translation: DataFrame) -> Cleaned:
    keys = ["product_id"]
    out = (df.withColumn("product_id", clean_str("product_id"))
           .withColumn("product_category_name", F.lower(clean_str("product_category_name")))
           # fix the source's misspelled column names in silver
           .withColumnRenamed("product_name_lenght", "product_name_length")
           .withColumnRenamed("product_description_lenght", "product_description_length"))
    # translation has ~71 rows -> broadcast is demonstrably safe
    tr = F.broadcast(translation.select("product_category_name", "product_category_name_english"))
    out = (out.join(tr, "product_category_name", "left")
           .withColumn("category_resolution",
                       F.when(F.col("product_category_name").isNull(), "missing_in_source")
                       .when(F.col("product_category_name_english").isNull(), "untranslated")
                       .otherwise("translated"))
           .withColumn("product_volume_cm3",
                       F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")))
    valid, rejected = split_valid(out, "products", keys, null_key_rules(keys) + [
        ("negative product dimension or weight",
         (F.col("product_weight_g") < 0) | (F.col("product_length_cm") < 0) |
         (F.col("product_height_cm") < 0) | (F.col("product_width_cm") < 0)),
    ])
    return Cleaned(deduplicate(valid, keys), rejected, keys)


def build_geolocation_zip(df: DataFrame, bbox: dict[str, float]) -> Cleaned:
    """Collapse ~1M raw points to one row per zip prefix (median lat/lng, modal city/state).

    Points outside Brazil's bounding box are rejected (known outliers in the source).
    """
    keys = ["zip_code_prefix"]
    pts = (df.withColumn("zip_code_prefix", normalize_zip_prefix("geolocation_zip_code_prefix"))
           .withColumn("city", F.lower(clean_str("geolocation_city")))
           .withColumn("state", F.upper(clean_str("geolocation_state"))))
    valid, rejected = split_valid(pts, "geolocation", keys, null_key_rules(keys) + [
        ("lat/lng missing or outside Brazil bounding box",
         ~(F.col("geolocation_lat").between(bbox["lat_min"], bbox["lat_max"]) &
           F.col("geolocation_lng").between(bbox["lng_min"], bbox["lng_max"]))),
    ])
    coords = valid.groupBy("zip_code_prefix").agg(
        F.percentile_approx("geolocation_lat", 0.5).alias("latitude"),
        F.percentile_approx("geolocation_lng", 0.5).alias("longitude"),
        F.count(F.lit(1)).alias("source_point_count"),
        F.max("_snapshot_id").alias("_bronze_snapshot_id"))
    w = Window.partitionBy("zip_code_prefix").orderBy(F.col("n").desc(), "city", "state")
    modal = (valid.groupBy("zip_code_prefix", "city", "state").agg(F.count(F.lit(1)).alias("n"))
             .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").select("zip_code_prefix", "city", "state"))
    out = coords.join(modal, "zip_code_prefix").withColumn("_silver_processed_at", F.current_timestamp())
    return Cleaned(out, rejected, keys)


def reject_orphans(child: Cleaned, parent: DataFrame, table: str, fk: list[str],
                   parent_name: str) -> Cleaned:
    """Move child rows whose foreign key has no parent into the rejected set."""
    parent_keys = parent.select(*fk).distinct().withColumn("_parent_found", F.lit(True))
    tagged = child.df.join(parent_keys, fk, "left")
    orphans = tagged.filter(F.col("_parent_found").isNull()).drop("_parent_found")
    kept = tagged.filter(F.col("_parent_found")).drop("_parent_found")
    rej = to_rejected(orphans, table, child.keys, f"orphan: {','.join(fk)} not found in {parent_name}")
    return Cleaned(kept, child.rejected.unionByName(rej), child.keys)


# ---- integration: fact and dimensions -----------------------------------------------------

def aggregate_payments_per_order(payments: DataFrame) -> DataFrame:
    """Grain: one row per order. Must be aggregated BEFORE joining to item grain."""
    w = Window.partitionBy("order_id").orderBy(F.col("payment_value").desc(), F.col("payment_sequential"))
    primary = (payments.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
               .select("order_id", F.col("payment_type").alias("primary_payment_type")))
    agg = payments.groupBy("order_id").agg(
        F.sum("payment_value").cast("decimal(14,2)").alias("order_payment_total"),
        F.count(F.lit(1)).alias("payment_count"),
        F.max("payment_installments").alias("max_installments"),
        F.sort_array(F.collect_set("payment_type")).alias("payment_types"))
    return agg.join(primary, "order_id")


def aggregate_reviews_per_order(reviews: DataFrame) -> DataFrame:
    return reviews.groupBy("order_id").agg(
        F.round(F.avg("review_score"), 2).alias("review_score_avg"),
        F.count(F.lit(1)).alias("review_count"))


def build_fact_order_details(orders: DataFrame, items: DataFrame, customers: DataFrame,
                             products: DataFrame, sellers: DataFrame, payments: DataFrame,
                             reviews: DataFrame) -> DataFrame:
    """Grain: one row per order item (order_id, order_item_id).

    Join plan (all validated N:1 so no row multiplication):
      items N:1 orders (inner; orphan items were already rejected)
      orders N:1 customers (left)  | items N:1 products (left) | items N:1 sellers (left)
      order-level payment and review aggregates 1:1 per order (left)
    Order-level money is carried as order_* columns; item_payment_allocated distributes the order's
    payment total across its items pro-rata to item_total so it can be summed at any grain.
    """
    o = orders.select("order_id", "customer_id", "order_status", "is_known_status", "is_sale_eligible",
                      "order_purchase_timestamp", "purchase_date", "order_approved_at",
                      "order_delivered_carrier_date", "order_delivered_customer_date",
                      "order_estimated_delivery_date", "delivery_days", "is_late_delivery")
    i = items.select("order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date",
                     "price", "freight_value", "item_total")
    c = customers.select("customer_id", "customer_unique_id", "customer_zip_code_prefix",
                         "customer_city", "customer_state")
    p = products.select("product_id", "product_category_name", "product_category_name_english",
                        "category_resolution")
    s = sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    order_w = Window.partitionBy("order_id")

    fact = (i.join(o, "order_id", "inner")
            .join(c, "customer_id", "left")
            .join(p, "product_id", "left")
            .join(s, "seller_id", "left")
            .join(aggregate_payments_per_order(payments), "order_id", "left")
            .join(aggregate_reviews_per_order(reviews), "order_id", "left")
            .withColumn("order_item_count", F.count(F.lit(1)).over(order_w))
            .withColumn("order_merchandise_total", F.sum("price").over(order_w).cast("decimal(14,2)"))
            .withColumn("order_freight_total", F.sum("freight_value").over(order_w).cast("decimal(14,2)"))
            .withColumn("order_items_total", F.sum("item_total").over(order_w).cast("decimal(14,2)"))
            .withColumn("item_payment_allocated", F.when(
                F.col("order_items_total") > 0,
                (F.col("order_payment_total") * F.col("item_total") / F.col("order_items_total"))
            ).cast("decimal(18,6)"))
            .withColumn("is_seller_late_to_carrier",
                        F.col("order_delivered_carrier_date") > F.col("shipping_limit_date"))
            .withColumn("_silver_processed_at", F.current_timestamp()))
    first = ["order_id", "order_item_id", "customer_id", "customer_unique_id", "product_id", "seller_id"]
    return fact.select(*first, *[col for col in fact.columns if col not in first])


def build_dim_customer(customers: DataFrame, orders: DataFrame, geo: DataFrame) -> DataFrame:
    """Grain: one row per customer_unique_id (a person). customer_id is a per-order key in Olist."""
    joined = customers.join(orders.select("customer_id", "order_id", "order_purchase_timestamp"),
                            "customer_id", "left")
    w = Window.partitionBy("customer_unique_id").orderBy(
        F.col("order_purchase_timestamp").desc_nulls_last(), F.col("customer_id"))
    latest = (joined.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
              .select("customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"))
    stats = joined.groupBy("customer_unique_id").agg(
        F.countDistinct("customer_id").alias("customer_id_count"),
        F.countDistinct("order_id").alias("order_count"),
        F.min("order_purchase_timestamp").alias("first_order_at"),
        F.max("order_purchase_timestamp").alias("last_order_at"))
    g = geo.select(F.col("zip_code_prefix").alias("customer_zip_code_prefix"), "latitude", "longitude")
    return (stats.join(latest, "customer_unique_id").join(g, "customer_zip_code_prefix", "left")
            .withColumn("_silver_processed_at", F.current_timestamp()))


def build_dim_seller(sellers: DataFrame, geo: DataFrame) -> DataFrame:
    g = geo.select(F.col("zip_code_prefix").alias("seller_zip_code_prefix"), "latitude", "longitude")
    return (sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
            .join(g, "seller_zip_code_prefix", "left")
            .withColumn("_silver_processed_at", F.current_timestamp()))


def build_dim_product(products: DataFrame) -> DataFrame:
    return (products.select(
        "product_id", "product_category_name", "product_category_name_english", "category_resolution",
        "product_name_length", "product_description_length", "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm", "product_volume_cm3")
        .withColumn("_silver_processed_at", F.current_timestamp()))


def build_dim_date(spark: SparkSession, orders: DataFrame) -> DataFrame:
    """Calendar spine covering purchase through estimated-delivery dates (gap-free time series)."""
    bounds = orders.agg(F.min("purchase_date").alias("lo"),
                        F.max(F.to_date("order_estimated_delivery_date")).alias("hi"))
    return (bounds.select(F.explode(F.sequence("lo", "hi", F.expr("INTERVAL 1 DAY"))).alias("date"))
            .select("date",
                    F.year("date").alias("year"), F.quarter("date").alias("quarter"),
                    F.month("date").alias("month"), F.date_format("date", "MMMM").alias("month_name"),
                    F.date_format("date", "yyyy-MM").alias("year_month"),
                    F.dayofmonth("date").alias("day_of_month"), F.dayofweek("date").alias("day_of_week"),
                    F.date_format("date", "EEEE").alias("day_name"),
                    F.dayofweek("date").isin(1, 7).alias("is_weekend")))


# ---- build orchestration ------------------------------------------------------------------

def _read_bronze(spark: SparkSession, cfg: ProjectConfig, name: str) -> tuple[DataFrame, DataFrame, int]:
    """Latest bronze snapshot split into (parsable rows, corrupt rows as rejected, bronze row count)."""
    df = latest_snapshot(spark.table(cfg.bronze(name)))
    n = df.count()
    corrupt = to_rejected(df.filter(F.col(CORRUPT_COL).isNotNull()), name, cfg.sources[name].primary_key,
                          "corrupt CSV record (see bronze._quarantine)")
    return df.filter(F.col(CORRUPT_COL).isNull()), corrupt, n


def build_silver(spark: SparkSession, cfg: ProjectConfig, run_id: str) -> list[dict]:
    rules = cfg.business_rules
    raw, corrupt, bronze_rows = {}, {}, {}
    for name in cfg.sources:
        raw[name], corrupt[name], bronze_rows[name] = _read_bronze(spark, cfg, name)

    tr = clean_category_translation(raw["category_translation"])
    cleaned = {
        "orders": clean_orders(raw["orders"], rules),
        "order_items": clean_order_items(raw["order_items"]),
        "order_payments": clean_payments(raw["order_payments"], rules),
        "order_reviews": clean_reviews(raw["order_reviews"]),
        "customers": clean_customers(raw["customers"]),
        "sellers": clean_sellers(raw["sellers"]),
        "products": clean_products(raw["products"], tr.df),
        "category_translation": tr,
        "geolocation": build_geolocation_zip(raw["geolocation"], rules["brazil_bbox"]),
    }
    # Referential integrity: child rows without a parent cannot be attributed -> rejected.
    for child in ("order_items", "order_payments", "order_reviews"):
        cleaned[child] = reject_orphans(cleaned[child], cleaned["orders"].df, child, ["order_id"], "orders")

    targets = {  # source name -> silver table name
        "orders": "orders", "order_items": "order_items", "order_payments": "payments",
        "order_reviews": "reviews", "customers": "customers", "sellers": "sellers",
        "products": "products", "category_translation": "category_translation",
        "geolocation": "geolocation_zip",
    }
    recon = []
    for name, target in targets.items():
        c = cleaned[name]
        df = c.df if name == "geolocation" else with_audit(c.df)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.silver(target))
        rejected_n = c.rejected.count()
        silver_n = spark.table(cfg.silver(target)).count()
        corrupt_n = corrupt[name].count()
        recon.append(dict(run_id=run_id, source_name=name, silver_table=target,
                          bronze_rows=bronze_rows[name], corrupt_rows=corrupt_n, rejected_rows=rejected_n,
                          # geolocation is an aggregation, so "duplicates" there are collapsed points
                          duplicates_removed=bronze_rows[name] - corrupt_n - rejected_n - silver_n,
                          silver_rows=silver_n, is_aggregated=(name == "geolocation")))
        log(logger, "silver table written", **recon[-1])

    s = {t: spark.table(cfg.silver(t)) for t in targets.values()}
    fact = build_fact_order_details(s["orders"], s["order_items"], s["customers"], s["products"],
                                    s["sellers"], s["payments"], s["reviews"])
    (fact.write.mode("overwrite").option("overwriteSchema", "true").clusterBy("purchase_date")
     .saveAsTable(cfg.silver("fact_order_details")))
    build_dim_customer(s["customers"], s["orders"], s["geolocation_zip"]) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.silver("dim_customer"))
    build_dim_product(s["products"]) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.silver("dim_product"))
    build_dim_seller(s["sellers"], s["geolocation_zip"]) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.silver("dim_seller"))
    build_dim_date(spark, s["orders"]) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.silver("dim_date"))

    rejected_all = None
    for name in targets:
        part = corrupt[name].unionByName(cleaned[name].rejected)
        rejected_all = part if rejected_all is None else rejected_all.unionByName(part)
    (rejected_all.withColumn("run_id", F.lit(run_id)).write.mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(cfg.silver(cfg.tables["rejected"])))

    spark.createDataFrame(recon).write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(cfg.silver("_reconciliation"))
    return recon
