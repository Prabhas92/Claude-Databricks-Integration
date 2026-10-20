-- Gold: mv_customer_analytics
-- Grain: one row per customer_unique_id (a person; customer_id is a per-order key in Olist).
-- total_spend = SUM(item_payment_allocated) = what the customer actually paid for sale-eligible orders.
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Customer-level purchasing: orders, items, GMV, amount paid, first/last purchase, repeat flag, location'
AS
WITH cust AS (
  SELECT customer_unique_id,
         COUNT(DISTINCT order_id)        AS order_count,
         COUNT(*)                        AS item_count,
         SUM(price)                      AS gmv,
         SUM(item_payment_allocated)     AS total_spend,
         MIN(order_purchase_timestamp)   AS first_purchase_at,
         MAX(order_purchase_timestamp)   AS last_purchase_at
  FROM IDENTIFIER(:fact_order_details)
  WHERE is_sale_eligible
  GROUP BY customer_unique_id
)
SELECT c.customer_unique_id,
       d.customer_state,
       d.customer_city,
       d.customer_zip_code_prefix,
       c.order_count,
       c.item_count,
       CAST(c.gmv AS DECIMAL(18,2))                          AS gmv,
       CAST(c.total_spend AS DECIMAL(18,2))                  AS total_spend,
       CAST(c.total_spend / c.order_count AS DECIMAL(18,2))  AS avg_spend_per_order,
       c.first_purchase_at,
       c.last_purchase_at,
       DATEDIFF(c.last_purchase_at, c.first_purchase_at)     AS customer_tenure_days,
       c.order_count > 1                                     AS is_repeat_customer
FROM cust c
LEFT JOIN IDENTIFIER(:dim_customer) d ON c.customer_unique_id = d.customer_unique_id;
