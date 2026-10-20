-- Gold: mv_daily_sales
-- Grain: one row per order purchase date.
-- Scope: sale-eligible orders (status not canceled/unavailable) that have at least one item.
-- Measures are computed at ORDER grain first, so order-level payment totals are never double counted.
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Daily sales KPIs (GMV, freight, gross order value, payments, AOV) for sale-eligible orders by purchase date'
AS
WITH order_level AS (
  SELECT order_id,
         purchase_date,
         MAX(order_item_count)        AS item_count,
         MAX(order_merchandise_total) AS merchandise_value,
         MAX(order_freight_total)     AS freight_value,
         MAX(order_items_total)       AS gross_order_value,
         MAX(order_payment_total)     AS payment_total
  FROM IDENTIFIER(:fact_order_details)
  WHERE is_sale_eligible
  GROUP BY order_id, purchase_date
)
SELECT purchase_date                                   AS order_date,
       COUNT(*)                                        AS order_count,
       SUM(item_count)                                 AS item_count,
       CAST(SUM(merchandise_value) AS DECIMAL(18,2))   AS gmv,
       CAST(SUM(freight_value) AS DECIMAL(18,2))       AS freight_value,
       CAST(SUM(gross_order_value) AS DECIMAL(18,2))   AS gross_order_value,
       CAST(SUM(payment_total) AS DECIMAL(18,2))       AS payment_total,
       CAST(SUM(gross_order_value) / COUNT(*) AS DECIMAL(18,2)) AS avg_order_value
FROM order_level
GROUP BY purchase_date;
