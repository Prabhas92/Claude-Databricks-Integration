# Business metric definitions

All money is in BRL (Brazilian real), exactly as published. No currency conversion is applied.

## Money measures (they are deliberately different)

| Measure | Definition | Source columns | Notes |
|---|---|---|---|
| **GMV / merchandise value / merchandise revenue** | `SUM(price)` over order items | order_items.price | Value of goods sold by sellers. This is what "revenue" means in this project. |
| **Freight** | `SUM(freight_value)` over order items | order_items.freight_value | Shipping charged to the customer. One item row carries its share of freight. |
| **Gross order value / order total** | `SUM(price + freight_value)` per order | order_items | What the order should cost before payment effects. |
| **Payment total** | `SUM(payment_value)` per order | order_payments.payment_value | What the customer actually paid across all payment methods (credit card, boleto, voucher, debit card). Can differ from the gross order value. Installment interest raises it, and the source sometimes splits vouchers differently. |
| **Item payment allocated** | `order_payment_total × item_total / order_items_total` | fact_order_details | Pro-rata allocation that lets item-level rows sum to the order's payment total without double counting. |
| **Olist revenue (commission)** | **Not available** | – | The dataset has no take-rate or commission, so no "platform revenue" metric is reported. |

## Which orders count as sales

* Statuses documented by Olist: `created, approved, invoiced, processing, shipped, delivered,
  canceled, unavailable`.
* **Sale-eligible** = known status AND status not in (`canceled`, `unavailable`). Configured in
  `business_rules.non_sale_statuses`.
* An unknown status is never mapped to a valid one. It stays visible with `is_known_status = false`,
  is excluded from sales, and triggers a DQ warning.
* Sales measures need at least one order item. The 775 item-less orders (mostly
  unavailable/canceled) appear only in fulfillment metrics.
* Returns: **the dataset has no return or refund information**, so no return logic is applied.
* Date basis for sales is `order_purchase_timestamp` (local Brazil time as published; no timezone in
  the source, stored as-is in a TIMESTAMP column of a UTC session).

## Gold objects

| Object | Grain | Key measures | Filter scope |
|---|---|---|---|
| mv_daily_sales | order_date | order_count, item_count, gmv, freight_value, gross_order_value, payment_total, avg_order_value = gross_order_value / orders | sale-eligible |
| mv_monthly_revenue | month_start | as above + active_days, prev_month_gmv, gmv_mom_change_pct | sale-eligible |
| mv_product_performance | product_id | items_sold, order_count, seller_count, gmv, freight, avg_item_price, revenue_share_pct (of total GMV), revenue_rank, category_revenue_rank | sale-eligible |
| mv_customer_analytics | customer_unique_id | order_count, item_count, gmv, total_spend (= paid), avg_spend_per_order, first/last purchase, tenure, is_repeat_customer, latest state/city | sale-eligible |
| mv_customer_state_summary | customer_state | customer_count, repeat_customer_count/pct, orders_per_customer, total/avg spend | sale-eligible |
| mv_seller_performance | seller_id | order_count, item_count, gmv, freight, avg_review_score, delivered_orders, avg_delivery_days, late_delivery_pct, late_to_carrier_pct, gmv_rank | sale-eligible |
| mv_order_fulfillment | purchase_date × order_status | order_count, delivered_count, late_delivery_count/pct, avg_delivery_days, avg_promised_days, avg_days_vs_promise, delivered_without_date_count | all orders |
| mv_payment_analytics | month × payment_type | payment_count, order_count, payment_value, non_sale_payment_value, avg_payment_value, avg/max installments, share_of_month_value_pct | all payments |
| mv_payment_reconciliation | month | orders_with_items, orders_with_payment, orders_missing_payment, items_total, payment_total, payment_minus_items, mismatched_orders(_pct) | all orders with items |

### Delivery definitions

* **Delivery days** = (`order_delivered_customer_date` − `order_purchase_timestamp`) in fractional days.
* **Late delivery** = delivered to the customer on a calendar date after
  `order_estimated_delivery_date`. That date is the promise shown to the customer and is present for
  100% of orders, so this indicator is reliable. NULL when the order was not delivered.
* **Late to carrier** (seller SLA) = `order_delivered_carrier_date > shipping_limit_date`.
* **Avg days vs promise** = delivered date − estimated date (negative = early).

### Caveats

* 2016-09 to 2016-12 and 2018-09 to 2018-10 are sparse/partial months in the source. Use
  `active_days` in `mv_monthly_revenue` before reading month-over-month changes.
* Review scores are averaged per order first and then per seller, so multi-item orders don't
  overweight a seller.
* `total_spend` for a customer is NULL-safe. The single order without any payment contributes 0 to
  spend but still counts as an order.
