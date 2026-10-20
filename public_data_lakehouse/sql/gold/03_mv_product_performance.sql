-- Gold: mv_product_performance
-- Grain: one row per product_id. Scope: items of sale-eligible orders.
-- Parameters (fully qualified names, passed by the job / gold_transformations.py):
--   :target = this object; source tables :fact_order_details, :dim_customer, :dim_seller, :orders, :payments
CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)
COMMENT 'Product sales: items sold, orders, GMV, revenue share and revenue rank (overall and within category)'
AS
WITH product_sales AS (
  SELECT product_id,
         COALESCE(product_category_name_english, product_category_name, '(missing category)') AS product_category,
         category_resolution,
         COUNT(*)                         AS items_sold,
         COUNT(DISTINCT order_id)         AS order_count,
         COUNT(DISTINCT seller_id)        AS seller_count,
         SUM(price)                       AS gmv,
         SUM(freight_value)               AS freight_value,
         AVG(price)                       AS avg_item_price
  FROM IDENTIFIER(:fact_order_details)
  WHERE is_sale_eligible
  GROUP BY product_id, product_category_name_english, product_category_name, category_resolution
)
SELECT product_id,
       product_category,
       category_resolution,
       items_sold,
       order_count,
       seller_count,
       CAST(gmv AS DECIMAL(18,2))                                         AS gmv,
       CAST(freight_value AS DECIMAL(18,2))                               AS freight_value,
       CAST(avg_item_price AS DECIMAL(18,2))                              AS avg_item_price,
       ROUND(gmv / SUM(gmv) OVER () * 100, 6)                             AS revenue_share_pct,
       RANK() OVER (ORDER BY gmv DESC)                                    AS revenue_rank,
       RANK() OVER (PARTITION BY product_category ORDER BY gmv DESC)      AS category_revenue_rank
FROM product_sales;
