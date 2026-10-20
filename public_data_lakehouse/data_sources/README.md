# Data sources

## Selected dataset: Brazilian E-Commerce Public Dataset by Olist

| Item | Value |
|---|---|
| Publisher | Olist (Brazilian marketplace integrator) |
| Download location used | Olist's official GitHub repository `olist/work-at-olist-data`, folder `datasets/` |
| Base URL | `https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/` |
| Repository page | https://github.com/olist/work-at-olist-data |
| Same data on Kaggle | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| Access | Plain HTTPS GET, no login, API key or paid service |
| License | The GitHub repository is published under the **MIT License** (Copyright (c) 2019 @olist). The Kaggle copy of the dataset is published under **CC BY-NC-SA 4.0**. This project is non-commercial and educational, attributes Olist, and does not redistribute the data (files are downloaded at run time and are gitignored), which is consistent with both. |
| Content | ~100k real, anonymised orders placed on Olist between 2016-09 and 2018-10. Company and partner names in reviews were replaced by names of Game of Thrones houses by the publisher. |
| Change tracking | None. The files are static snapshots: no CDC, no `updated_at` columns. |
| Download timestamp | Recorded per file and run in `bronze._ingestion_log.downloaded_at` (with SHA-256 and size). |

### Why this source

* Nine **related** tables with documented keys (orders, items, payments, reviews, customers,
  products, sellers, geolocation, category translation), so it supports real joins.
* Direct, stable, unauthenticated download from the publisher's own repository.
* Kaggle was not used because its download requires an authenticated account/API key, which is not
  configured here.
* The UCI Online Retail dataset was evaluated, but it is a single flat table and can't demonstrate
  multi-entity integration.

### Files (verified 2026-10-20, sizes from the GitHub API)

| Source key | File | Size | Records* | Business key |
|---|---|---:|---:|---|
| orders | olist_orders_dataset.csv | 17.7 MB | 99,441 | order_id |
| order_items | olist_order_items_dataset.csv | 15.0 MB | 112,650 | (order_id, order_item_id) |
| order_payments | olist_order_payments_dataset.csv | 5.6 MB | 103,886 | (order_id, payment_sequential) |
| order_reviews | olist_order_reviews_dataset.csv | 14.5 MB | 99,224 | (review_id, order_id). review_id alone has 814 duplicates |
| customers | olist_customers_dataset.csv | 8.6 MB | 99,441 | customer_id (per order). customer_unique_id = person |
| products | olist_products_dataset.csv | 2.4 MB | 32,951 | product_id |
| sellers | olist_sellers_dataset.csv | 0.2 MB | 3,095 | seller_id |
| geolocation | olist_geolocation_dataset.csv | 61.3 MB | 1,000,163 | none (19,015 zip prefixes) |
| category_translation | product_category_name_translation.csv | 2.6 KB | 71 | product_category_name |

\*Records counted with a multi-line-aware CSV parser during source profiling. `wc -l` overcounts
reviews because comments contain embedded newlines.

### Source quirks found during profiling (handled in the pipeline)

| Finding | Handling |
|---|---|
| `review_comment_message` contains quoted newlines | CSV read with `multiLine=true`, `quote`/`escape` = `"` |
| `product_category_name_translation.csv` starts with a UTF-8 BOM | Explicit schema; header validated after stripping the BOM |
| Customer/seller zip prefixes lost leading zeros (`9790` vs geolocation `"09790"`) | Zip prefixes kept as STRING in bronze; left-padded to 5 digits in silver |
| Product columns misspelled `product_name_lenght`, `product_description_lenght` | Preserved in bronze, renamed to `*_length` in silver |
| 610 products have no category; 2 categories have no English translation | `category_resolution` = `missing_in_source` / `untranslated`. Never mapped to a fake category |
| 775 orders have no items; 1 order has no payment | Kept in `silver.orders` and fulfillment metrics; excluded from item-grain fact by design |
| 8 orders are `delivered` but have no delivery date | Flag `is_delivered_without_date`; DQ warning |
| 312 orders: payments differ from price+freight by > 1.00 BRL | Reported in `mv_payment_reconciliation` and DQ (warning, threshold 1%) |
| 42 geolocation points fall outside Brazil | Rejected in silver (`_rejected_records`) |
| 3 payments typed `not_defined`, 2 with 0 installments, 9 with value 0 | Kept as documented source values, flagged |

### Reproducing the profile locally

```bash
mkdir -p data/raw && cd data/raw
for f in customers geolocation order_items order_payments order_reviews orders products sellers; do
  curl -sSfLO "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_${f}_dataset.csv"
done
curl -sSfLO https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/product_category_name_translation.csv
```
