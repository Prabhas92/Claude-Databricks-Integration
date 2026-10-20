-- Gold: mv_order_fulfillment
-- Grain: one row per (purchase_date, order_status). Scope: ALL orders, including canceled and
-- orders without items. Late = delivered to customer on a date after order_estimated_delivery_date
-- (the delivery promise shown to the customer, present in the source for every order).
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Order fulfillment by purchase date and status: delivered, late deliveries, delivery duration vs promise'
AS
SELECT purchase_date,
       order_status,
       COUNT(*)                                                            AS order_count,
       COUNT(order_delivered_customer_date)                                AS delivered_count,
       SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END)                    AS late_delivery_count,
       ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END)
             / NULLIF(COUNT(is_late_delivery), 0) * 100, 2)                AS late_delivery_pct,
       ROUND(AVG(delivery_days), 2)                                        AS avg_delivery_days,
       ROUND(AVG(DATEDIFF(order_estimated_delivery_date, order_purchase_timestamp)), 2) AS avg_promised_days,
       ROUND(AVG(CASE WHEN order_delivered_customer_date IS NOT NULL
                      THEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) END), 2)
                                                                           AS avg_days_vs_promise,
       SUM(CASE WHEN is_delivered_without_date THEN 1 ELSE 0 END)          AS delivered_without_date_count
FROM IDENTIFIER(:orders)
GROUP BY purchase_date, order_status;
