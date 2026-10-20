#!/usr/bin/env bash
# Read-only checks: CLI, auth, catalog, warehouse, local tests, bundle validation.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] && set -a && source .env && set +a
: "${DATABRICKS_CONFIG_PROFILE:?set DATABRICKS_CONFIG_PROFILE (see .env.example)}"
CATALOG="${LAKEHOUSE_CATALOG:-claudedatabricksintegration}"
TARGET="${BUNDLE_TARGET:-dev}"

echo "== Databricks CLI";            databricks --version
echo "== Identity";                  databricks current-user me -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['userName'])"
echo "== Catalog '$CATALOG'";        databricks catalogs get "$CATALOG" -o json | python3 -c "import json,sys;d=json.load(sys.stdin);print('owner:',d['owner'])"
echo "== Schemas";                   databricks schemas list "$CATALOG"
echo "== SQL warehouses";            databricks warehouses list
echo "== Bundle validate";           databricks bundle validate -t "$TARGET" --var="catalog=$CATALOG"
if [[ -x .venv/bin/python ]]; then
  echo "== Unit tests (pure Python)"; LAKEHOUSE_SKIP_SPARK_TESTS=1 .venv/bin/python -m pytest -q
fi
echo "Environment OK"
