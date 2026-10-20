"""Generate docs/data_dictionary.md from Unity Catalog metadata + business definitions.

Column names and data types come from <catalog>.information_schema (so the document always matches
what is deployed); descriptions, grain and lineage come from the dictionaries below.
Runs a read-only query on the configured SQL warehouse:

    DATABRICKS_CONFIG_PROFILE=<profile> .venv/bin/python scripts/generate_data_dictionary.py
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from databricks.sdk import WorkspaceClient  # noqa: E402

from common.config import load_config  # noqa: E402

COLUMN_DOCS = {
    # ingestion / audit metadata
    "_ingested_at": "Timestamp (UTC) when the row was loaded into bronze",
    "_ingestion_date": "Date part of _ingested_at",
    "_source_name": "Source key from config (e.g. orders)",
    "_source_file": "Full path of the landed file in the UC volume",
    "_snapshot_id": "First 16 hex chars of the file SHA-256; one value per distinct source file",
    "_source_file_sha256": "SHA-256 of the downloaded source file",
    "_corrupt_record": "Raw CSV line when the row failed parsing/typing (NULL for valid rows)",
    "_bronze_snapshot_id": "Bronze snapshot the silver row was built from",
    "_bronze_ingested_at": "Bronze ingestion timestamp of the surviving row",
    "_silver_processed_at": "Timestamp when the silver row was produced",
    # keys
    "order_id": "Order identifier (Olist hash)",
    "order_item_id": "Sequential item number within an order (1..n)",
    "customer_id": "Customer key **per order** (each order gets its own customer_id)",
    "customer_unique_id": "Identifier of the actual person across orders",
    "product_id": "Product identifier", "seller_id": "Seller identifier",
    "review_id": "Review identifier (not unique alone: one review can cover several orders)",
    "payment_sequential": "Sequence of the payment within the order (an order can use several methods)",
    # orders
    "order_status": "Order status as published (lower-cased in silver)",
    "order_purchase_timestamp": "Purchase timestamp (local Brazil time as published)",
    "order_approved_at": "Payment approval timestamp",
    "order_delivered_carrier_date": "Hand-over to the logistics carrier",
    "order_delivered_customer_date": "Delivery to the customer",
    "order_estimated_delivery_date": "Delivery date promised to the customer at purchase",
    "is_known_status": "Status is one of the 8 statuses documented by Olist",
    "is_sale_eligible": "Known status and not canceled/unavailable; used for all sales measures",
    "purchase_date": "Date of order_purchase_timestamp",
    "delivery_days": "Days from purchase to customer delivery (fractional)",
    "is_late_delivery": "Delivered on a date after the estimated delivery date (NULL if not delivered)",
    "is_delivered_without_date": "Status delivered but no customer delivery date in source",
    "has_inconsistent_timestamps": "Approval or delivery earlier than purchase",
    # items / payments / reviews
    "shipping_limit_date": "Deadline for the seller to hand the item to the carrier",
    "price": "Item price (BRL) - merchandise value",
    "freight_value": "Freight charged for the item (BRL)",
    "item_total": "price + freight_value",
    "payment_type": "Payment method (credit_card, boleto, voucher, debit_card, not_defined)",
    "payment_installments": "Number of installments chosen",
    "payment_value": "Amount paid with this payment record (BRL)",
    "is_known_payment_type": "payment_type in documented list",
    "has_zero_installments": "Source shows 0 installments (kept, flagged)",
    "review_score": "Customer satisfaction score 1-5",
    "review_comment_title": "Review title (free text, Portuguese)",
    "review_comment_message": "Review text (free text, Portuguese)",
    "review_creation_date": "Date the satisfaction survey was sent",
    "review_answer_timestamp": "Timestamp the survey was answered",
    "has_comment": "Review has a non-empty message",
    # customers / sellers / geo
    "customer_zip_code_prefix": "First 5 digits of the customer CEP (left-padded in silver)",
    "customer_city": "Customer city", "customer_state": "Customer state (UF)",
    "seller_zip_code_prefix": "First 5 digits of the seller CEP (left-padded in silver)",
    "seller_city": "Seller city", "seller_state": "Seller state (UF)",
    "geolocation_zip_code_prefix": "Zip prefix of the geolocation point",
    "geolocation_lat": "Latitude", "geolocation_lng": "Longitude",
    "geolocation_city": "City of the point", "geolocation_state": "State of the point",
    "zip_code_prefix": "5-digit zip prefix", "latitude": "Median latitude of the zip prefix",
    "longitude": "Median longitude of the zip prefix", "city": "Most frequent city name for the zip prefix",
    "state": "Most frequent state for the zip prefix", "source_point_count": "Raw points aggregated",
    # products
    "product_category_name": "Category (Portuguese, as published)",
    "product_category_name_english": "Category in English from the translation file (NULL if none)",
    "category_resolution": "translated | untranslated | missing_in_source",
    "product_name_lenght": "Characters in product name (source spelling)",
    "product_description_lenght": "Characters in product description (source spelling)",
    "product_name_length": "Characters in product name", "product_description_length": "Characters in description",
    "product_photos_qty": "Number of product photos", "product_weight_g": "Weight in grams",
    "product_length_cm": "Length cm", "product_height_cm": "Height cm", "product_width_cm": "Width cm",
    "product_volume_cm3": "length x height x width",
    # fact
    "order_item_count": "Items in the order", "order_merchandise_total": "SUM(price) of the order",
    "order_freight_total": "SUM(freight) of the order", "order_items_total": "SUM(price + freight) of the order",
    "order_payment_total": "SUM(payment_value) of the order (NULL if no payment)",
    "payment_count": "Payment records of the order", "max_installments": "Max installments of the order",
    "payment_types": "Distinct payment methods of the order", "primary_payment_type": "Method with the largest amount",
    "item_payment_allocated": "order_payment_total allocated pro-rata to item_total (sums to order payment)",
    "review_score_avg": "Average review score of the order", "review_count": "Reviews of the order",
    "is_seller_late_to_carrier": "Carrier pickup after shipping_limit_date",
    # dims
    "customer_id_count": "Per-order customer_ids belonging to the person", "order_count": "Number of orders",
    "first_order_at": "First purchase", "last_order_at": "Most recent purchase",
    "date": "Calendar date", "year": "Year", "quarter": "Quarter", "month": "Month number",
    "month_name": "Month name", "year_month": "yyyy-MM", "day_of_month": "Day of month",
    "day_of_week": "1=Sunday..7=Saturday", "day_name": "Weekday name", "is_weekend": "Saturday or Sunday",
    # gold
    "order_date": "Purchase date", "month_start": "First day of the purchase month",
    "item_count": "Order items", "gmv": "Merchandise value SUM(price) (BRL)",
    "gross_order_value": "SUM(price + freight) (BRL)", "payment_total": "SUM(payment_value) (BRL)",
    "avg_order_value": "gross_order_value / order_count", "active_days": "Days in the month with orders",
    "prev_month_gmv": "GMV of the previous month", "gmv_mom_change_pct": "Month-over-month GMV change %",
    "product_category": "English category, else Portuguese, else '(missing category)'",
    "items_sold": "Items sold", "seller_count": "Distinct sellers", "avg_item_price": "Average item price",
    "revenue_share_pct": "Share of total GMV %", "revenue_rank": "Rank by GMV (1 = highest)",
    "category_revenue_rank": "Rank by GMV within category", "total_spend": "Amount paid (allocated payments)",
    "avg_spend_per_order": "total_spend / order_count", "first_purchase_at": "First purchase",
    "last_purchase_at": "Last purchase", "customer_tenure_days": "Days between first and last purchase",
    "is_repeat_customer": "More than one sale-eligible order", "customer_count": "Customers",
    "repeat_customer_count": "Customers with >1 order", "repeat_customer_pct": "Repeat customers %",
    "orders_per_customer": "Average orders per customer", "avg_spend_per_customer": "Average spend per customer",
    "avg_review_score": "Average order review score", "delivered_orders": "Orders with a delivery date",
    "avg_delivery_days": "Average purchase-to-delivery days", "late_delivery_pct": "Late deliveries %",
    "late_to_carrier_pct": "Orders handed to carrier after the limit %", "gmv_rank": "Rank by GMV",
    "delivered_count": "Orders delivered", "late_delivery_count": "Orders delivered late",
    "avg_promised_days": "Average promised lead time (days)", "avg_days_vs_promise": "Delivered minus promised date (days)",
    "delivered_without_date_count": "Delivered orders missing delivery date", "non_sale_payment_value": "Payments on canceled/unavailable orders",
    "avg_payment_value": "Average payment record value", "avg_installments": "Average installments",
    "share_of_month_value_pct": "Share of the month's payment value %", "orders_with_items": "Orders having items",
    "orders_with_payment": "Orders having payments", "orders_missing_payment": "Orders with items but no payment",
    "items_total": "SUM(price + freight)", "payment_minus_items": "payment_total - items_total",
    "mismatched_orders": "Orders where |payment - items| > 1.00 BRL", "mismatched_orders_pct": "Mismatched orders %",
    # operational tables
    "run_id": "Pipeline run identifier (Databricks job run id)", "source_name": "Source key",
    "table_name": "Target table / table the record belongs to", "source_url": "Download URL",
    "landing_path": "Landed file path", "file_sha256": "SHA-256 of the file", "file_bytes": "File size",
    "snapshot_id": "Snapshot id (hash prefix)", "load_mode": "snapshot | full_refresh",
    "status": "Outcome (LOADED, SKIPPED_UNCHANGED, FAILED / PASS, WARN, FAIL)", "row_count": "Rows in bronze for the snapshot",
    "corrupt_record_count": "Rows quarantined", "downloaded_at": "Download timestamp (UTC)",
    "started_at": "Step start (UTC)", "duration_s": "Duration seconds", "error_message": "Error detail",
    "source_file": "Landed file path", "reason": "Why the row was quarantined/rejected", "raw_record": "Raw CSV text",
    "quarantined_at": "Quarantine timestamp", "business_key": "Business key of the rejected row ('|' separated)",
    "record_json": "Full rejected row as JSON", "silver_table": "Silver table name",
    "bronze_rows": "Rows in latest bronze snapshot", "corrupt_rows": "Corrupt rows",
    "rejected_rows": "Rejected rows", "duplicates_removed": "Rows removed by dedupe (or aggregated, geolocation)",
    "silver_rows": "Rows written to silver", "is_aggregated": "Silver table is an aggregation of the source",
    "layer": "bronze | silver | gold", "dataset": "Checked table/object", "check_name": "Check identifier",
    "severity": "critical (fails pipeline) | warning", "observed_value": "Measured value",
    "expected_condition": "Condition the value must meet", "message": "Diagnostic message", "executed_at": "Check time (UTC)",
}

TABLE_DOCS = {
    "bronze": ("Raw source rows, original columns + ingestion metadata",
               "one row per source CSV record per snapshot", "Olist CSV → UC volume → bronze"),
    "silver.orders": ("Cleaned orders", "order_id", "bronze.orders"),
    "silver.order_items": ("Cleaned order items", "(order_id, order_item_id)", "bronze.order_items"),
    "silver.payments": ("Cleaned payments", "(order_id, payment_sequential)", "bronze.order_payments"),
    "silver.reviews": ("Cleaned reviews", "(review_id, order_id)", "bronze.order_reviews"),
    "silver.customers": ("Cleaned customers (per-order customer key)", "customer_id", "bronze.customers"),
    "silver.sellers": ("Cleaned sellers", "seller_id", "bronze.sellers"),
    "silver.products": ("Cleaned products with English category", "product_id", "bronze.products + bronze.category_translation"),
    "silver.category_translation": ("Category translation", "product_category_name", "bronze.category_translation"),
    "silver.geolocation_zip": ("One coordinate per zip prefix", "zip_code_prefix", "bronze.geolocation (aggregated)"),
    "silver.fact_order_details": ("Consolidated order-item fact", "(order_id, order_item_id)", "items ⋈ orders ⋈ customers ⋈ products ⋈ sellers ⋈ payments(agg) ⋈ reviews(agg)"),
    "silver.dim_customer": ("Customer (person) dimension", "customer_unique_id", "customers + orders + geolocation_zip"),
    "silver.dim_product": ("Product dimension", "product_id", "silver.products"),
    "silver.dim_seller": ("Seller dimension with coordinates", "seller_id", "sellers + geolocation_zip"),
    "silver.dim_date": ("Calendar spine", "date", "orders date range"),
    "silver._rejected_records": ("Rows rejected by silver validation", "one row per rejected record", "bronze"),
    "silver._reconciliation": ("Row-count reconciliation per source", "source_name", "bronze/silver counts"),
    "bronze._ingestion_log": ("Ingestion audit log", "(run_id, source_name)", "bronze ingestion"),
    "bronze._quarantine": ("Malformed CSV rows", "one row per corrupt record", "bronze tables"),
    "gold.data_quality_results": ("DQ results, append-only", "(run_id, layer, dataset, check_name)", "DQ steps"),
    "gold.mv_daily_sales": ("Daily sales KPIs", "order_date", "silver.fact_order_details"),
    "gold.mv_monthly_revenue": ("Monthly GMV and MoM change", "month_start", "silver.fact_order_details"),
    "gold.mv_product_performance": ("Product sales and ranking", "product_id", "silver.fact_order_details"),
    "gold.mv_customer_analytics": ("Customer purchasing", "customer_unique_id", "fact + dim_customer"),
    "gold.mv_customer_state_summary": ("Customers by state", "customer_state", "fact + dim_customer"),
    "gold.mv_seller_performance": ("Seller performance", "seller_id", "fact + dim_seller"),
    "gold.mv_order_fulfillment": ("Fulfillment by date and status", "(purchase_date, order_status)", "silver.orders"),
    "gold.mv_payment_analytics": ("Payment method mix", "(month_start, payment_type)", "silver.payments + silver.orders"),
    "gold.mv_payment_reconciliation": ("Items vs payments reconciliation", "month_start", "silver.fact_order_details"),
}


def main() -> None:
    cfg = load_config()
    w = WorkspaceClient()
    wh = next(x for x in w.warehouses.list() if x.name == cfg.sql_warehouse_name)
    schemas = ", ".join(f"'{s}'" for s in cfg.schemas.values())
    sql = (f"SELECT c.table_schema, c.table_name, t.table_type, c.column_name, c.full_data_type, c.ordinal_position "
           f"FROM `{cfg.catalog}`.information_schema.columns c JOIN `{cfg.catalog}`.information_schema.tables t "
           f"USING (table_catalog, table_schema, table_name) WHERE c.table_schema IN ({schemas}) "
           f"ORDER BY c.table_schema, c.table_name, c.ordinal_position")
    res = w.statement_execution.execute_statement(warehouse_id=wh.id, statement=sql, wait_timeout="50s")
    if res.status.state.value != "SUCCEEDED":
        raise RuntimeError(res.status.error)
    rows = res.result.data_array or []
    tables: dict[tuple, list] = defaultdict(list)
    for schema, table, ttype, col, dtype, _ in rows:
        if table.startswith(("__materialization_", "event_log_")):
            continue  # Databricks-managed backing tables of materialized views
        tables[(schema, table, ttype)].append((col, dtype))

    order = {cfg.schemas["bronze"]: 0, cfg.schemas["silver"]: 1, cfg.schemas["gold"]: 2}
    out = ["# Data dictionary", "",
           f"Generated by `scripts/generate_data_dictionary.py` from `{cfg.catalog}.information_schema` "
           "(types are the deployed types). Money is BRL. Business definitions: see "
           "[business_metrics.md](business_metrics.md); lineage: [source_lineage.md](source_lineage.md).", ""]
    missing = set()
    for (schema, table, ttype) in sorted(tables, key=lambda k: (order.get(k[0], 9), k[1])):
        layer = {v: k for k, v in cfg.schemas.items()}.get(schema, schema)
        key = f"{layer}.{table}"
        desc, grain, lineage = TABLE_DOCS.get(key) or (TABLE_DOCS["bronze"] if layer == "bronze" else ("", "", ""))
        if layer == "bronze" and key not in TABLE_DOCS:
            lineage = f"{cfg.sources[table].url}" if table in cfg.sources else lineage
        out += [f"## `{schema}.{table}`", "",
                f"* **Type:** {ttype}  ", f"* **Description:** {desc}  ",
                f"* **Grain / primary key:** {grain}  ", f"* **Lineage:** {lineage}", "",
                "| Column | Type | Definition |", "|---|---|---|"]
        for col, dtype in tables[(schema, table, ttype)]:
            d = COLUMN_DOCS.get(col)
            if d is None:
                missing.add(col)
                d = ""
            out.append(f"| `{col}` | {dtype} | {d} |")
        out.append("")
    (ROOT / "docs" / "data_dictionary.md").write_text("\n".join(out), encoding="utf-8")
    print(f"wrote docs/data_dictionary.md: {len(tables)} tables, {len(rows)} columns")
    if missing:
        print("columns without a definition:", sorted(missing))


if __name__ == "__main__":
    main()
