-- Gold: mv_seller_performance
-- Grain: one row per seller_id. Scope: sale-eligible orders.
-- Order-level metrics (delivery, reviews) are first reduced to one row per (seller, order) so that
-- multi-item orders do not overweight a seller.
-- late_to_carrier: carrier pickup after the seller's shipping_limit_date (seller-side SLA).
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Seller orders, items, GMV, freight, review score, delivery time, late delivery and late-to-carrier rates'
AS
WITH seller_order AS (
  SELECT seller_id,
         order_id,
         COUNT(*)                                   AS items,
         SUM(price)                                 AS gmv,
         SUM(freight_value)                         AS freight_value,
         MAX(review_score_avg)                      AS review_score,
         MAX(delivery_days)                         AS delivery_days,
         MAX(CAST(is_late_delivery AS INT))         AS is_late_delivery,
         MAX(CAST(is_seller_late_to_carrier AS INT)) AS is_late_to_carrier
  FROM IDENTIFIER(:fact_order_details)
  WHERE is_sale_eligible
  GROUP BY seller_id, order_id
)
SELECT so.seller_id,
       s.seller_state,
       s.seller_city,
       COUNT(*)                                        AS order_count,
       SUM(so.items)                                   AS item_count,
       CAST(SUM(so.gmv) AS DECIMAL(18,2))              AS gmv,
       CAST(SUM(so.freight_value) AS DECIMAL(18,2))    AS freight_value,
       ROUND(AVG(so.review_score), 2)                  AS avg_review_score,
       COUNT(so.delivery_days)                         AS delivered_orders,
       ROUND(AVG(so.delivery_days), 2)                 AS avg_delivery_days,
       ROUND(AVG(so.is_late_delivery) * 100, 2)        AS late_delivery_pct,
       ROUND(AVG(so.is_late_to_carrier) * 100, 2)      AS late_to_carrier_pct,
       RANK() OVER (ORDER BY SUM(so.gmv) DESC)         AS gmv_rank
FROM seller_order so
LEFT JOIN IDENTIFIER(:dim_seller) s ON so.seller_id = s.seller_id
GROUP BY so.seller_id, s.seller_state, s.seller_city;
