-- Gold: mv_payment_analytics
-- Grain: one row per (purchase month, payment_type). Scope: ALL payment records.
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Payment method mix by month: payment count, orders, value, installments, share of monthly value'
AS
WITH p AS (
  SELECT CAST(DATE_TRUNC('MONTH', o.purchase_date) AS DATE) AS month_start,
         p.payment_type,
         p.order_id,
         p.payment_value,
         p.payment_installments,
         o.is_sale_eligible
  FROM IDENTIFIER(:payments) p
  JOIN IDENTIFIER(:orders) o ON p.order_id = o.order_id
),
agg AS (
  SELECT month_start,
         payment_type,
         COUNT(*)                                                         AS payment_count,
         COUNT(DISTINCT order_id)                                         AS order_count,
         SUM(payment_value)                                               AS payment_value,
         SUM(CASE WHEN NOT is_sale_eligible THEN payment_value ELSE 0 END) AS non_sale_payment_value,
         AVG(payment_installments)                                        AS avg_installments,
         MAX(payment_installments)                                        AS max_installments
  FROM p
  GROUP BY month_start, payment_type
)
SELECT month_start,
       payment_type,
       payment_count,
       order_count,
       CAST(payment_value AS DECIMAL(18,2))                                     AS payment_value,
       CAST(non_sale_payment_value AS DECIMAL(18,2))                            AS non_sale_payment_value,
       CAST(payment_value / payment_count AS DECIMAL(18,2))                     AS avg_payment_value,
       ROUND(avg_installments, 2)                                               AS avg_installments,
       max_installments,
       ROUND(payment_value / SUM(payment_value) OVER (PARTITION BY month_start) * 100, 2) AS share_of_month_value_pct
FROM agg;
