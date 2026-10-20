-- Silver inspection queries.
-- Row reconciliation: bronze = corrupt + rejected + duplicates_removed + silver
SELECT source_name, silver_table, bronze_rows, corrupt_rows, rejected_rows, duplicates_removed, silver_rows,
       bronze_rows = corrupt_rows + rejected_rows + duplicates_removed + silver_rows AS balanced
FROM claudedatabricksintegration.silver._reconciliation
ORDER BY source_name;

-- Why rows were rejected
SELECT table_name, reason, COUNT(*) AS rows
FROM claudedatabricksintegration.silver._rejected_records
GROUP BY table_name, reason
ORDER BY rows DESC;

-- Grain check of the fact table (must return 0 rows)
SELECT order_id, order_item_id, COUNT(*) AS n
FROM claudedatabricksintegration.silver.fact_order_details
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;
