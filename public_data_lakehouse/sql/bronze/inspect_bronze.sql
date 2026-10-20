-- Bronze inspection queries (run on a SQL warehouse; replace the catalog if you changed it).
-- Ingestion history: one row per source file per run
SELECT run_id, source_name, status, row_count, corrupt_record_count, snapshot_id,
       file_bytes, downloaded_at, duration_s, error_message
FROM claudedatabricksintegration.bronze._ingestion_log
ORDER BY started_at DESC, source_name;

-- Snapshots held per bronze table (idempotency: a rerun of the same file must not add a snapshot)
SELECT _snapshot_id, MIN(_ingested_at) AS ingested_at, COUNT(*) AS rows
FROM claudedatabricksintegration.bronze.orders
GROUP BY _snapshot_id;

-- Quarantined malformed rows
SELECT source_name, reason, COUNT(*) AS rows
FROM claudedatabricksintegration.bronze._quarantine
GROUP BY source_name, reason;
