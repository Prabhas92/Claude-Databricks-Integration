-- Gold: mv_customer_state_summary
-- Grain: one row per customer state (customer's most recent location from dim_customer).
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Geographic customer analysis: customers, repeat-customer rate, order frequency and spend by state'
AS
WITH cust AS (
  SELECT customer_unique_id,
         COUNT(DISTINCT order_id)    AS order_count,
         SUM(item_payment_allocated) AS total_spend
  FROM IDENTIFIER(:fact_order_details)
  WHERE is_sale_eligible
  GROUP BY customer_unique_id
)
SELECT COALESCE(d.customer_state, '(unknown)')                         AS customer_state,
       COUNT(*)                                                        AS customer_count,
       SUM(CASE WHEN c.order_count > 1 THEN 1 ELSE 0 END)              AS repeat_customer_count,
       ROUND(AVG(CASE WHEN c.order_count > 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS repeat_customer_pct,
       SUM(c.order_count)                                              AS order_count,
       ROUND(AVG(c.order_count), 4)                                    AS orders_per_customer,
       CAST(SUM(c.total_spend) AS DECIMAL(18,2))                       AS total_spend,
       CAST(AVG(c.total_spend) AS DECIMAL(18,2))                       AS avg_spend_per_customer
FROM cust c
LEFT JOIN IDENTIFIER(:dim_customer) d ON c.customer_unique_id = d.customer_unique_id
GROUP BY COALESCE(d.customer_state, '(unknown)');
