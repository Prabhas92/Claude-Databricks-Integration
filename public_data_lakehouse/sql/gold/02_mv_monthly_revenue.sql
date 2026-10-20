-- Gold: mv_monthly_revenue
-- Grain: one row per purchase month.
-- "Revenue" here = merchandise revenue earned by sellers (GMV = SUM(price)). Olist's own commission
-- is NOT in the dataset, so marketplace take-rate revenue cannot be computed.
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Monthly merchandise revenue (GMV), freight, payments, order count, AOV and month-over-month GMV change'
AS
WITH order_level AS (
  SELECT order_id,
         CAST(DATE_TRUNC('MONTH', purchase_date) AS DATE) AS month_start,
         purchase_date,
         MAX(order_merchandise_total) AS merchandise_value,
         MAX(order_freight_total)     AS freight_value,
         MAX(order_items_total)       AS gross_order_value,
         MAX(order_payment_total)     AS payment_total
  FROM IDENTIFIER(:fact_order_details)
  WHERE is_sale_eligible
  GROUP BY order_id, purchase_date
),
monthly AS (
  SELECT month_start,
         COUNT(*)                       AS order_count,
         COUNT(DISTINCT purchase_date)  AS active_days,
         SUM(merchandise_value)         AS gmv,
         SUM(freight_value)             AS freight_value,
         SUM(gross_order_value)         AS gross_order_value,
         SUM(payment_total)             AS payment_total
  FROM order_level
  GROUP BY month_start
)
SELECT month_start,
       DATE_FORMAT(month_start, 'yyyy-MM')                          AS year_month,
       order_count,
       active_days,
       CAST(gmv AS DECIMAL(18,2))                                   AS gmv,
       CAST(freight_value AS DECIMAL(18,2))                         AS freight_value,
       CAST(gross_order_value AS DECIMAL(18,2))                     AS gross_order_value,
       CAST(payment_total AS DECIMAL(18,2))                         AS payment_total,
       CAST(gross_order_value / order_count AS DECIMAL(18,2))       AS avg_order_value,
       CAST(LAG(gmv) OVER (ORDER BY month_start) AS DECIMAL(18,2))  AS prev_month_gmv,
       ROUND((gmv - LAG(gmv) OVER (ORDER BY month_start))
             / NULLIF(LAG(gmv) OVER (ORDER BY month_start), 0) * 100, 2) AS gmv_mom_change_pct
FROM monthly;
