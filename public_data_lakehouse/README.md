# public_data_lakehouse

A production-style **Databricks Medallion Lakehouse** built on real public data: the
[Brazilian E-Commerce Public Dataset by Olist](https://github.com/olist/work-at-olist-data), about 100k
real orders from 2016–2018 across 9 related tables. It ingests raw files into Bronze Delta tables,
cleans and integrates them in Silver, and serves analytics through Unity Catalog **materialized
views** in Gold. Data-quality checks run at every layer, and everything deploys as a Databricks
Asset Bundle (Lakeflow Jobs).

## Business problem

A marketplace wants trustworthy answers to: *How are sales trending? Which products and sellers
drive revenue? Who are our repeat customers and where are they? Are we delivering on our promises?
How do customers pay, and do payments reconcile with what was sold?* These answers need data that is
reproducible, auditable, de-duplicated and free of join double counting. This project is built to
provide that.

## Architecture

```
Olist GitHub CSVs ──► UC volume (landing) ──► BRONZE (raw Delta, 9 tables)
                                                 │  clean · type · validate · dedupe · reject orphans
                                                 ▼
                                              SILVER (9 clean tables + fact_order_details + 4 dims)
                                                 │  SQL on SQL warehouse
                                                 ▼
                                              GOLD (9 materialized views)
             data-quality checks after every layer ──► gold.data_quality_results
```

Details: [docs/architecture.md](docs/architecture.md) · lineage: [docs/source_lineage.md](docs/source_lineage.md) ·
metrics: [docs/business_metrics.md](docs/business_metrics.md) · columns: [docs/data_dictionary.md](docs/data_dictionary.md) ·
troubleshooting: [docs/troubleshooting.md](docs/troubleshooting.md)

## Dataset and license

| | |
|---|---|
| Source | `https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/` (Olist's official repo) |
| License | Repository: MIT License © 2019 Olist. Kaggle copy: CC BY-NC-SA 4.0. Used non-commercially, with attribution and without redistribution. |
| Access | Unauthenticated HTTPS. Downloaded at run time by the pipeline. Never committed. |
| Files | orders, order_items, order_payments, order_reviews, customers, products, sellers, geolocation, category translation |

See [data_sources/README.md](data_sources/README.md) for record counts, keys and the source quirks
found during profiling.

## Prerequisites

* Databricks workspace with **Unity Catalog** and serverless compute (developed on Free Edition, AWS).
* A catalog you can create schemas/volumes in (default `claudedatabricksintegration`).
* A SQL warehouse (default name `Serverless Starter Warehouse`) for materialized views.
* Databricks CLI ≥ 0.250 / 1.x (developed with v1.21.0), authenticated with a profile.
* Python 3.10+ locally for tests (3.12 used). No local Java is needed: Spark tests use Databricks Connect.

## Local setup

```bash
cd public_data_lakehouse
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"          # pyyaml, pytest, databricks-connect
cp .env.example .env                       # set profile / catalog (no secrets in this file)
```

## Databricks CLI authentication

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile>
databricks auth profiles                    # verify "Valid: YES"
export DATABRICKS_CONFIG_PROFILE=<profile>  # used by the CLI, bundle, SDK and Databricks Connect
```

Tokens are kept in the CLI's secure storage. Nothing in this repo contains credentials.

## Configuration

Everything is in [`config/project_config.yaml`](config/project_config.yaml): catalog, schema names,
landing volume, load mode, Gold object type, warehouse name, business rules, DQ thresholds, and
source URLs with explicit schemas and primary keys.

| Setting | Default | Override |
|---|---|---|
| catalog | `claudedatabricksintegration` | `LAKEHOUSE_CATALOG` env, `--catalog`, bundle `--var catalog=...` |
| schemas | `bronze` / `silver` / `gold` | bundle vars `bronze_schema`, `silver_schema`, `gold_schema` (passed to every task) |
| load_mode | `snapshot` | `full_refresh` |
| gold_object_type | `materialized_view` | `table` (fallback, objects named `agg_*`) |

## Deployment

```bash
./scripts/validate_environment.sh                 # read-only checks + bundle validate + unit tests
databricks bundle validate
databricks bundle deploy                          # target dev (default); add --var="catalog=<c>" if needed
databricks bundle summary                         # job URLs
```

Deployed jobs (dev target prefixes names with `[dev <user>]`):

| Job | Tasks |
|---|---|
| `lakehouse_bronze_silver` | validate_environment → bronze_ingest → bronze_dq → silver_build → silver_dq (serverless) |
| `lakehouse_gold` | 9 × materialized-view SQL tasks (SQL warehouse) → gold_dq → summary |
| `lakehouse_end_to_end` | run_job_task(bronze_silver) → run_job_task(gold). Weekly schedule, **paused**. |

## Running the pipeline

```bash
./scripts/run_pipeline.sh                         # deploy + end-to-end
./scripts/run_pipeline.sh --no-deploy --job gold  # only rebuild Gold
./scripts/run_pipeline.sh --force-reload          # reload bronze even if source files are unchanged

# or directly
databricks bundle run lakehouse_end_to_end
databricks bundle run lakehouse_bronze_silver --params force_reload=--force-reload

# monitoring
databricks jobs list-runs --limit 5
databricks jobs get-run <run_id> -o json
databricks jobs get-run-output <task_run_id> -o json
```

Local execution (Databricks Connect on serverless), e.g. one step:

```bash
.venv/bin/python src/orchestration/run_pipeline.py --step silver_dq --run-id manual-check
```

## Tables

| Layer | Objects |
|---|---|
| bronze | `orders, order_items, order_payments, order_reviews, customers, products, sellers, geolocation, category_translation` + `_ingestion_log`, `_quarantine`. Volume `landing`. |
| silver | `orders, order_items, payments, reviews, customers, sellers, products, category_translation, geolocation_zip, fact_order_details, dim_customer, dim_product, dim_seller, dim_date` + `_rejected_records`, `_reconciliation` |
| gold | `mv_daily_sales, mv_monthly_revenue, mv_product_performance, mv_customer_analytics, mv_customer_state_summary, mv_seller_performance, mv_order_fulfillment, mv_payment_analytics, mv_payment_reconciliation` (materialized views) + `data_quality_results` (Delta table) |

`silver.fact_order_details` has a grain of **one row per order item**. Payments and reviews are
aggregated to order level *before* the join, and `item_payment_allocated` spreads each order's
payment across its items so sums never double count.

## Sample queries

All in [`sql/queries/sample_queries.sql`](sql/queries/sample_queries.sql). They use `:named`
parameters, which become widgets in the SQL editor.

```sql
-- Monthly sales trend
SELECT year_month, order_count, gmv, avg_order_value, gmv_mom_change_pct
FROM claudedatabricksintegration.gold.mv_monthly_revenue
WHERE month_start BETWEEN :start_date AND :end_date ORDER BY month_start;

-- Best-performing products
SELECT revenue_rank, product_id, product_category, gmv, revenue_share_pct
FROM claudedatabricksintegration.gold.mv_product_performance ORDER BY revenue_rank LIMIT :top_n;

-- Top customers by spending
SELECT customer_unique_id, customer_state, order_count, total_spend
FROM claudedatabricksintegration.gold.mv_customer_analytics ORDER BY total_spend DESC LIMIT :top_n;

-- Seller performance
SELECT seller_id, order_count, gmv, avg_review_score, late_delivery_pct
FROM claudedatabricksintegration.gold.mv_seller_performance WHERE order_count >= 30 ORDER BY gmv_rank LIMIT :top_n;

-- Late deliveries
SELECT DATE_TRUNC('MONTH', purchase_date) m, SUM(late_delivery_count) late, SUM(delivered_count) delivered
FROM claudedatabricksintegration.gold.mv_order_fulfillment GROUP BY 1 ORDER BY 1;

-- Payment method distribution
SELECT payment_type, SUM(payment_value) value FROM claudedatabricksintegration.gold.mv_payment_analytics
GROUP BY payment_type ORDER BY value DESC;
```

## Materialized-view behaviour

* The Gold objects are genuine Unity Catalog **materialized views** (`table_type = MATERIALIZED_VIEW`,
  checked by Gold DQ). Each is created with `CREATE OR REPLACE MATERIALIZED VIEW` on the SQL
  warehouse. Databricks then provisions a managed serverless pipeline that computes and stores the
  result.
* **Refresh:** every run of `lakehouse_gold` recreates them, so they always reflect the current
  Silver data and the SQL in the repo. Silver is rebuilt by overwrite each run, so an incremental MV
  refresh wouldn't add value here. Manual refresh: `REFRESH MATERIALIZED VIEW <name>` on a SQL warehouse.
* No `SCHEDULE` / `TRIGGER ON UPDATE` clause is used. Refresh is driven by the job, so it always runs
  after Silver DQ has passed.
* **Free Edition constraint (verified):** MV compute can't run while serverless job compute is
  active. That's why Gold is its own job. See [troubleshooting](docs/troubleshooting.md).
* Fallback: `gold_object_type: table` builds the same SQL as Delta tables named `agg_*` with Spark.
  It's tested at the SQL-rewrite level but not used in this deployment.

## Data-quality rules

Reusable checks live in `src/common/data_quality.py`, and the per-layer suites are in
`src/quality/dq_suites.py`. Every result is appended to `gold.data_quality_results`. A **critical**
failure raises `DataQualityError` after the results are persisted, which fails the job and stops
downstream layers. A **warning** is only recorded.

| Layer | Critical | Warning |
|---|---|---|
| Bronze | every source logged and not FAILED; row count ≥ configured minimum; corrupt-record ratio ≤ 0.1%; metadata not null | duplicate / null business keys (resolved in silver) |
| Silver | PK unique + not null for all 13 tables; 6 FK checks; non-negative money; row reconciliation `bronze = corrupt + rejected + duplicates + silver`; fact rows = item rows; fact price = item price; allocated payments = payments | unknown statuses / payment types; inconsistent timestamps; delivered without date; untranslated categories; zero installments; items vs payments mismatch > 1%; orders missing payment |
| Gold | every object exists with the expected type and has rows; GMV/orders/customers/payments reconcile to Silver; revenue shares sum to 100% | – |

## Testing

```bash
LAKEHOUSE_SKIP_SPARK_TESTS=1 .venv/bin/python -m pytest -q   # pure Python (config, DQ logic, SQL rewrite)
.venv/bin/python -m pytest -q                                # + Spark tests on serverless via Databricks Connect
```

Fixtures in `tests/fixtures/` and inline DataFrames are **synthetic, test-only** data.

## Cost and cleanup

* Compute: serverless jobs + the 2X-Small serverless SQL warehouse (auto-stop). No clusters, cloud
  storage, external locations or service principals were created.
* The weekly schedule is **paused**. Unpause it only if you want recurring runs.
* Storage: about 125 MB of landed CSVs plus the Delta tables (a few hundred MB including history).
* Cleanup:

```bash
databricks bundle destroy                      # removes the 3 jobs and the deployed files
# data objects (irreversible):
#   DROP SCHEMA claudedatabricksintegration.gold   CASCADE;
#   DROP SCHEMA claudedatabricksintegration.silver CASCADE;
#   DROP SCHEMA claudedatabricksintegration.bronze CASCADE;   -- also drops the landing volume
```

## Verified run (2026-10-20, Databricks Free Edition)

| Stage | Result |
|---|---|
| Unit tests | 29 passed, 1 skipped (CSV-parsing test needs the landing volume; it existed only after the first deploy) |
| `lakehouse_bronze_silver` job | **SUCCESS**. All 9 sources downloaded, loaded and quarantined (0 corrupt rows). Silver built. |
| Bronze DQ / Silver DQ | **66 / 53 checks, all PASS** |
| Row reconciliation | Every source balances. Example: geolocation 1,000,163 = 42 rejected + 981,111 collapsed + 19,010 zip rows. |
| Fact grain | `fact_order_details` = 112,650 rows = `order_items` rows (no join multiplication) |
| `lakehouse_gold` job | **Partial**. 7 of 9 materialized views created (`table_type = MATERIALIZED_VIEW`). `mv_payment_analytics` and `mv_payment_reconciliation` were still waiting for free-tier compute when the run was stopped. Gold DQ has not run yet. |

Sample results from the Gold MVs: Nov-2017 GMV +52% MoM (Black Friday), top category
`health_beauty` (9.3% of GMV), and a repeat-customer rate of 3.04% (2,887 of 94,983 customers).

Issues found and fixed during the run:
- `mode("errorifexists")` isn't supported on serverless.
- `IDENTIFIER()` over a concatenation is rejected in MV DDL.
- Free Edition serverless concurrency limit. Gold now runs as a separate job, with retries.

Not yet done: the rerun idempotency check, and finishing the last 2 MVs plus Gold DQ. Run
`databricks bundle run lakehouse_gold` to complete them.
