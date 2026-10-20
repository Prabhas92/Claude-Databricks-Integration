# Source model and lineage

## Source relational model (as verified by profiling)

```
customers (customer_id PK) 1 ────< 1 orders (order_id PK)        customer_id is one per order;
     │ customer_unique_id = person                                 a person has many customer_ids
     │
orders 1 ────< N order_items (order_id, order_item_id)  >──── 1 products (product_id)
   │                         │                                           │
   │                         └──── N:1 sellers (seller_id)               └─ N:1 category_translation
   ├────< N order_payments (order_id, payment_sequential)
   └────< N order_reviews (review_id, order_id)

geolocation: many lat/lng points per zip prefix → joins to customers/sellers on zip prefix after
aggregation to one row per prefix.
```

Verified cardinalities on the real files:

| Relationship | Type | Orphans found |
|---|---|---|
| order_items.order_id → orders | N:1 | 0 |
| order_items.product_id → products | N:1 | 0 |
| order_items.seller_id → sellers | N:1 | 0 |
| order_payments.order_id → orders | N:1 | 0 |
| order_reviews.order_id → orders | N:1 | 0 |
| orders.customer_id → customers | 1:1 | 0 |
| orders without items | | 775 |
| orders without payments | | 1 |

## Layer lineage

| Silver object | Built from | Grain | Join type / key |
|---|---|---|---|
| silver.orders | bronze.orders | order_id | – |
| silver.order_items | bronze.order_items | (order_id, order_item_id) | orphan check vs silver.orders |
| silver.payments | bronze.order_payments | (order_id, payment_sequential) | orphan check vs silver.orders |
| silver.reviews | bronze.order_reviews | (review_id, order_id) | orphan check vs silver.orders |
| silver.customers | bronze.customers | customer_id | – |
| silver.sellers | bronze.sellers | seller_id | – |
| silver.products | bronze.products + bronze.category_translation | product_id | LEFT, broadcast (71 rows) on product_category_name |
| silver.category_translation | bronze.category_translation | product_category_name | – |
| silver.geolocation_zip | bronze.geolocation | zip_code_prefix | aggregation (median lat/lng, modal city/state) |
| silver.fact_order_details | order_items ⋈ orders ⋈ customers ⋈ products ⋈ sellers ⋈ payments(agg) ⋈ reviews(agg) | (order_id, order_item_id) | see below |
| silver.dim_customer | customers + orders + geolocation_zip | customer_unique_id | LEFT |
| silver.dim_product | products | product_id | – |
| silver.dim_seller | sellers + geolocation_zip | seller_id | LEFT on zip prefix |
| silver.dim_date | orders (date range) | date | generated spine |

### fact_order_details join plan

| Step | Right side | Key | Join | Expected cardinality | Why it cannot multiply rows |
|---|---|---|---|---|---|
| 1 | silver.orders | order_id | INNER | N:1 | order_id unique in silver.orders (DQ critical); orphans were already rejected |
| 2 | silver.customers | customer_id | LEFT | N:1 | customer_id unique |
| 3 | silver.products | product_id | LEFT | N:1 | product_id unique |
| 4 | silver.sellers | seller_id | LEFT | N:1 | seller_id unique |
| 5 | payments aggregated per order | order_id | LEFT | N:1 | aggregated to 1 row/order **before** the join |
| 6 | reviews aggregated per order | order_id | LEFT | N:1 | aggregated to 1 row/order before the join |

Verified by DQ: `fact rows == silver.order_items rows`, `SUM(fact.price) == SUM(items.price)` and
`SUM(item_payment_allocated) == SUM(payments of orders with items)`.

Rejected records: every row removed between bronze and silver is in `silver._rejected_records` (with
reason and the full JSON record) or counted as a duplicate in `silver._reconciliation`, which
enforces `bronze = corrupt + rejected + duplicates_removed + silver` for every source.

## Gold lineage

| Gold object | Reads |
|---|---|
| mv_daily_sales, mv_monthly_revenue, mv_product_performance, mv_payment_reconciliation | silver.fact_order_details |
| mv_customer_analytics, mv_customer_state_summary | silver.fact_order_details, silver.dim_customer |
| mv_seller_performance | silver.fact_order_details, silver.dim_seller |
| mv_order_fulfillment | silver.orders |
| mv_payment_analytics | silver.payments, silver.orders |
| gold.data_quality_results | written by DQ steps only; read by nothing in the pipeline (no cycle) |

Unity Catalog also captures this lineage automatically. See *Catalog Explorer → table → Lineage*.
