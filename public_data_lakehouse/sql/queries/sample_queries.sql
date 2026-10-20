-- Sample consumer queries against the Gold layer.
-- Parameters use Databricks SQL named parameter markers (:name). In the SQL editor they become
-- input widgets; with the Statement Execution API pass them in `parameters`.
-- Suggested values: :start_date = '2017-01-01', :end_date = '2018-08-31', :top_n = 10, :state = 'SP'

USE CATALOG claudedatabricksintegration;

-- 1. Monthly sales trend
SELECT year_month, order_count, active_days, gmv, freight_value, payment_total, avg_order_value, gmv_mom_change_pct
FROM gold.mv_monthly_revenue
WHERE month_start BETWEEN DATE_TRUNC('MONTH', CAST(:start_date AS DATE)) AND CAST(:end_date AS DATE)
ORDER BY month_start;

-- 2. Best-performing products
SELECT revenue_rank, product_id, product_category, items_sold, order_count, gmv, revenue_share_pct
FROM gold.mv_product_performance
ORDER BY revenue_rank
LIMIT :top_n;

-- 2b. Best categories (roll-up of the product MV)
SELECT product_category, SUM(items_sold) AS items_sold, SUM(gmv) AS gmv, ROUND(SUM(revenue_share_pct), 2) AS revenue_share_pct
FROM gold.mv_product_performance
GROUP BY product_category
ORDER BY gmv DESC
LIMIT :top_n;

-- 3. Top customers by spending (optionally one state)
SELECT customer_unique_id, customer_state, customer_city, order_count, total_spend, first_purchase_at, last_purchase_at
FROM gold.mv_customer_analytics
WHERE (:state = 'ALL' OR customer_state = :state)
ORDER BY total_spend DESC
LIMIT :top_n;

-- 3b. Repeat-customer rate by state
SELECT customer_state, customer_count, repeat_customer_pct, orders_per_customer, avg_spend_per_customer
FROM gold.mv_customer_state_summary
ORDER BY customer_count DESC;

-- 4. Seller performance (sellers with meaningful volume)
SELECT gmv_rank, seller_id, seller_state, order_count, gmv, avg_review_score, avg_delivery_days,
       late_delivery_pct, late_to_carrier_pct
FROM gold.mv_seller_performance
WHERE order_count >= 30
ORDER BY gmv_rank
LIMIT :top_n;

-- 5. Late deliveries by month
SELECT DATE_TRUNC('MONTH', purchase_date) AS month,
       SUM(delivered_count) AS delivered,
       SUM(late_delivery_count) AS late,
       ROUND(SUM(late_delivery_count) / NULLIF(SUM(delivered_count), 0) * 100, 2) AS late_pct
FROM gold.mv_order_fulfillment
WHERE purchase_date BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
GROUP BY 1
ORDER BY 1;

-- 6. Payment method distribution
SELECT payment_type, SUM(payment_count) AS payments, SUM(payment_value) AS value,
       ROUND(SUM(payment_value) / SUM(SUM(payment_value)) OVER () * 100, 2) AS share_pct,
       ROUND(SUM(avg_installments * payment_count) / SUM(payment_count), 2) AS avg_installments
FROM gold.mv_payment_analytics
WHERE month_start BETWEEN DATE_TRUNC('MONTH', CAST(:start_date AS DATE)) AND CAST(:end_date AS DATE)
GROUP BY payment_type
ORDER BY value DESC;

-- 7. Daily sales for a date range
SELECT order_date, order_count, gmv, avg_order_value
FROM gold.mv_daily_sales
WHERE order_date BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
ORDER BY order_date;

-- 8. Latest data-quality results
SELECT layer, dataset, check_name, status, observed_value, expected_condition, message, executed_at
FROM gold.data_quality_results
WHERE executed_at >= (SELECT MAX(executed_at) - INTERVAL 3 HOURS FROM gold.data_quality_results)
  AND status <> 'PASS'
ORDER BY layer, status;
