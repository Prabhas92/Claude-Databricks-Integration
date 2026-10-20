# Architecture

## Data flow

```
 Olist GitHub (HTTPS, 9 CSVs)
          │  urllib download, SHA-256, header/schema-drift check
          ▼
 UC volume  <catalog>.bronze.landing  /olist/<source>/<snapshot_id>/<file>.csv
          │  spark.read.csv(explicit schema, PERMISSIVE, multiLine)  + ingestion metadata
          ▼
 BRONZE  (Delta, 1 table per source, snapshot-idempotent)        bronze._ingestion_log
   orders, order_items, order_payments, order_reviews,           bronze._quarantine
   customers, products, sellers, geolocation, category_translation
          │  latest snapshot → clean / type / validate / dedupe / orphan-reject
          ▼
 SILVER  (Delta, full deterministic rebuild)                      silver._rejected_records
   orders, order_items, payments, reviews, customers, sellers,    silver._reconciliation
   products, category_translation, geolocation_zip
   fact_order_details (grain: order item)
   dim_customer, dim_product, dim_seller, dim_date
          │  SQL on SQL warehouse (CREATE OR REPLACE MATERIALIZED VIEW)
          ▼
 GOLD   (Unity Catalog materialized views)                         gold.data_quality_results
   mv_daily_sales, mv_monthly_revenue, mv_product_performance,
   mv_customer_analytics, mv_customer_state_summary, mv_seller_performance,
   mv_order_fulfillment, mv_payment_analytics, mv_payment_reconciliation
```

## Orchestration (Lakeflow Jobs, deployed with a Declarative Automation Bundle)

```
lakehouse_end_to_end  (paused weekly schedule)
 ├─ run_job_task → lakehouse_bronze_silver   [serverless jobs compute, env v4]
 │     validate_environment → bronze_ingest → bronze_dq → silver_build → silver_dq
 └─ run_job_task → lakehouse_gold            (starts only if the first job succeeded)
       mv_daily_sales → mv_monthly_revenue → … → mv_payment_reconciliation   [SQL warehouse]
       → gold_dq → summary                                                    [serverless]
```

* Each Python task calls `src/orchestration/run_pipeline.py --step <step>` with catalog/schema
  parameters from bundle variables and `{{job.run_id}}` as the run id. The same entry point runs
  locally through Databricks Connect.
* Failure propagation: a failing task (including a critical DQ failure, which raises
  `DataQualityError`) fails its job, which fails the `run_job_task` and skips the rest.
* **Why two jobs:** on this Databricks Free Edition workspace, MV refresh compute can't start while
  serverless job compute is allocated. This was verified: an MV created from inside a running job
  fails with `RESOURCE_EXHAUSTED`, even from a later SQL task in the same job. Splitting Gold into its
  own job releases the Python compute first. Gold tasks retry every 2 minutes in case release is slow.
  On a paid workspace the two jobs could be merged.

## Key design decisions

| Decision | Reason |
|---|---|
| Download inside Databricks to a UC managed volume | Reproducible and auditable (hash, size, timestamp). No local machine needed. Serverless egress to GitHub was verified. |
| Explicit schemas + `PERMISSIVE` + `_corrupt_record` | Types are enforced, but malformed rows are never dropped. They stay in bronze and are copied to `_quarantine`. |
| Snapshot idempotency via SHA-256 + `replaceWhere` | Source has no CDC. A true incremental claim would be false. Reruns of the same file are no-ops or exact replacements. |
| Silver = full deterministic rebuild from the latest snapshot | ~1.5M rows total. Simpler and provably idempotent. Deterministic dedupe (latest ingestion, then row hash). |
| Payments/reviews aggregated to order grain before the join | Prevents row multiplication on multi-item, multi-payment orders |
| `item_payment_allocated` | Lets payment totals be summed at item grain without double counting |
| Liquid clustering on `fact_order_details(purchase_date)` | Most Gold queries filter or aggregate by purchase date |
| Broadcast only for `category_translation` (71 rows) | Demonstrably small. Everything else is left to AQE. |
| No caching / repartitioning | Data is small, and serverless manages caching. Explicit caching isn't needed. |
| Gold as genuine materialized views | Precomputed, governed serving objects with UC lineage. A table fallback exists (`gold_object_type: table` → `agg_*` names) but isn't used. |
| DQ results in an append-only Delta table | Auditable history per run_id. Critical failures stop the pipeline after results are persisted. |

## Compute and cost

| Workload | Compute | Notes |
|---|---|---|
| Python tasks | Serverless jobs compute (environment version 4, Python 3.12, Spark 4.x) | No clusters created |
| Materialized views | SQL warehouse "Serverless Starter Warehouse" (looked up by name) + MV serverless pipeline | Warehouse auto-stops when idle |
| Local tests | Databricks Connect → serverless | No local JVM needed |
