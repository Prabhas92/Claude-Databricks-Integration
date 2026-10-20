#!/usr/bin/env bash
# Deploy the bundle and run the end-to-end orchestrator job (bronze/silver job -> gold job).
# Usage: scripts/run_pipeline.sh [--force-reload] [--no-deploy] [--job bronze_silver|gold|end_to_end]
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] && set -a && source .env && set +a
: "${DATABRICKS_CONFIG_PROFILE:?set DATABRICKS_CONFIG_PROFILE (see .env.example)}"
CATALOG="${LAKEHOUSE_CATALOG:-claudedatabricksintegration}"
TARGET="${BUNDLE_TARGET:-dev}"
JOB=end_to_end; DEPLOY=1; FORCE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force-reload) FORCE="--force-reload" ;;
    --no-deploy) DEPLOY=0 ;;
    --job) JOB="$2"; shift ;;
    *) echo "unknown arg $1"; exit 2 ;;
  esac; shift
done

if [[ $DEPLOY -eq 1 ]]; then
  databricks bundle deploy -t "$TARGET" --var="catalog=$CATALOG"
fi

if [[ -n "$FORCE" && "$JOB" != "gold" ]]; then
  # force_reload is a job parameter of the bronze/silver job, so run that job directly first.
  databricks bundle run lakehouse_bronze_silver -t "$TARGET" --var="catalog=$CATALOG" --params "force_reload=$FORCE"
  [[ "$JOB" == "end_to_end" ]] && JOB=gold || exit 0
fi
databricks bundle run "lakehouse_${JOB}" -t "$TARGET" --var="catalog=$CATALOG"
