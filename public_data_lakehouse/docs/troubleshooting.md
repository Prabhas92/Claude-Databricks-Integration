# Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `RESOURCE_EXHAUSTED: You've hit the limit for serverless compute for free usage` when creating a materialized view | Databricks **Free Edition** allows one serverless compute at a time besides the SQL warehouse. MV creation starts serverless pipeline compute, which can't start while a serverless job, notebook or Databricks Connect session is active. | Gold runs as a separate job (`lakehouse_gold`), with retries every 2 min. Close notebooks/Connect sessions (they idle out after ~10 min) and rerun `databricks bundle run lakehouse_gold`. Paid workspaces don't have this limit. |
| `[NON_LITERAL_IDENTIFIER_NOT_SUPPORTED]` creating a materialized view | `IDENTIFIER(:schema \|\| '.name')` (an expression) is not allowed in MV DDL | Pass fully qualified names as plain parameters (`IDENTIFIER(:target)`). The bundle builds them in `x-gold-sources` in `resources/jobs.yml`. |
| `PySparkValueError: errorifexists is not supported` | Spark Connect / serverless doesn't support `mode("errorifexists")` | Use `append` for the first write (already fixed in `write_bronze`) |
| `Catalog '<x>' not found or not visible` in validate step | Wrong catalog or no `USE CATALOG` privilege | `databricks catalogs list`, then deploy with `--var="catalog=<catalog>"`, or set `LAKEHOUSE_CATALOG` for local runs |
| `PERMISSION_DENIED` creating schemas/volume | Identity lacks `CREATE SCHEMA` / `CREATE VOLUME` on the catalog | Ask the catalog owner: `GRANT USE CATALOG, CREATE SCHEMA ON CATALOG <c> TO <principal>` |
| `SourceValidationError: Source schema drift` | Upstream CSV header changed | Compare with `config/project_config.yaml` → update the schema deliberately; never auto-evolve silently |
| `Download failed` / HTTP errors | GitHub unavailable or egress blocked | Check `bronze._ingestion_log.error_message`. Serverless egress to raw.githubusercontent.com was verified for this workspace. Workspaces with serverless egress control must allow that domain. |
| Bronze status `SKIPPED_UNCHANGED` | Same file hash already loaded (snapshot mode) | Expected and idempotent. To force: `scripts/run_pipeline.sh --force-reload` |
| `DataQualityError: N critical ... DQ check(s) failed` | A critical rule failed. The job stops before downstream layers. | `SELECT * FROM <catalog>.gold.data_quality_results WHERE run_id = '<run>' AND status = 'FAIL'` |
| `[CANNOT_RESOLVE_DATAFRAME_COLUMN]` in tests | Referencing a column from a DataFrame after it was dropped (Spark Connect is strict) | Use `F.col("name")` on the current DataFrame |
| Unit-test dates shifted by a day | Local timezone conversion of naive datetimes in Spark Connect | `tests/conftest.py` pins `TZ=UTC` |
| `databricks: unknown flag: --profile x` when using `$P` in zsh | zsh does not word-split variables | `export DATABRICKS_CONFIG_PROFILE=<profile>` instead |
| MV shows stale data | MVs are recomputed only by `CREATE OR REPLACE` / `REFRESH` | Rerun `lakehouse_gold` (or `REFRESH MATERIALIZED VIEW <name>` on the SQL warehouse) |

## Useful commands

```bash
export DATABRICKS_CONFIG_PROFILE=<your-profile>
databricks bundle summary                                  # deployed jobs + URLs
databricks jobs list-runs --job-id <id> --limit 5          # recent runs
databricks jobs get-run <run_id> -o json                   # task states
databricks jobs get-run-output <task_run_id> -o json       # logs / error of one task
databricks experimental aitools tools query "SELECT * FROM claudedatabricksintegration.bronze._ingestion_log ORDER BY started_at DESC LIMIT 20"
```
