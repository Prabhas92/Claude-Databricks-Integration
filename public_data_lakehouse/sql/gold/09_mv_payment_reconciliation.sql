-- Gold: mv_payment_reconciliation
-- Grain: one row per purchase month. Scope: ALL orders that have at least one item.
-- Compares item totals (price + freight) with what was paid. Differences arise in the source from
-- e.g. installment interest and vouchers; orders differing by more than 1.00 BRL are counted as
-- mismatched (same tolerance as business_rules.payment_reconciliation_tolerance in the config).
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Monthly reconciliation of order item totals (price + freight) against payments'
AS
WITH order_level AS (
  SELECT order_id,
         CAST(DATE_TRUNC('MONTH', purchase_date) AS DATE) AS month_start,
         MAX(order_items_total)   AS items_total,
         MAX(order_payment_total) AS payment_total
  FROM IDENTIFIER(:fact_order_details)
  GROUP BY order_id, purchase_date
)
SELECT month_start,
       COUNT(*)                                                            AS orders_with_items,
       COUNT(payment_total)                                                AS orders_with_payment,
       COUNT(*) - COUNT(payment_total)                                     AS orders_missing_payment,
       CAST(SUM(items_total) AS DECIMAL(18,2))                             AS items_total,
       CAST(SUM(payment_total) AS DECIMAL(18,2))                           AS payment_total,
       CAST(SUM(payment_total) - SUM(items_total) AS DECIMAL(18,2))        AS payment_minus_items,
       SUM(CASE WHEN ABS(payment_total - items_total) > 1.00 THEN 1 ELSE 0 END) AS mismatched_orders,
       ROUND(SUM(CASE WHEN ABS(payment_total - items_total) > 1.00 THEN 1 ELSE 0 END)
             / COUNT(*) * 100, 3)                                          AS mismatched_orders_pct
FROM order_level
GROUP BY month_start;
