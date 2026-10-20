"""Gold layer: business-ready serving objects defined in sql/gold/NN_mv_<name>.sql.

The SQL files are the single source of truth and are executed in one of two ways:

* materialized_view (default): on a SQL warehouse. In the Databricks job each file is a `sql_task`;
  locally `create_on_warehouse` runs them through the Statement Execution API. Materialized views
  need serverless pipeline compute, which on Databricks Free Edition cannot run concurrently with
  serverless job compute - hence Gold runs as its own job after bronze/silver finish.
* table (documented fallback): the same SQL is rewritten to CREATE OR REPLACE TABLE and executed with Spark;
  :target then resolves to agg_<name> so nobody mistakes a table for an MV.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path

from common.config import PROJECT_ROOT, ProjectConfig
from common.logging_utils import get_logger, log

logger = get_logger("gold")

GOLD_SQL_DIR = PROJECT_ROOT / "sql" / "gold"
_FILE_RE = re.compile(r"^\d+_mv_(?P<name>[a-z0-9_]+)\.sql$")
_PARAM_RE = re.compile(r"(?<![:\w]):(?P<name>[A-Za-z_]\w*)")


@dataclass(frozen=True)
class GoldObject:
    base_name: str     # e.g. daily_sales
    path: Path

    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")


def discover_gold_objects(sql_dir: Path = GOLD_SQL_DIR) -> list[GoldObject]:
    objs = []
    for p in sorted(sql_dir.glob("*.sql")):
        m = _FILE_RE.match(p.name)
        if m:
            objs.append(GoldObject(m.group("name"), p))
    return objs


def split_statements(sql: str) -> list[str]:
    """Split a SQL script on statement-terminating semicolons; drop comment-only chunks."""
    no_comments = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", no_comments) if s.strip()]


GOLD_SOURCE_TABLES = ("fact_order_details", "dim_customer", "dim_seller", "orders", "payments")


def sql_parameters(cfg: ProjectConfig, base_name: str) -> dict[str, str]:
    """Fully qualified names for IDENTIFIER(:param). Plain parameters are required: a materialized
    view rejects IDENTIFIER() over a concatenation (NON_LITERAL_IDENTIFIER_NOT_SUPPORTED)."""
    params = {t: cfg.silver(t) for t in GOLD_SOURCE_TABLES}
    params["target"] = cfg.gold(cfg.gold_object_name(base_name))
    return params


def bind_parameters(statement: str, params: dict[str, str]) -> str:
    """Inline named parameter markers as SQL string literals (used by the Spark fallback)."""
    def repl(m: re.Match) -> str:
        name = m.group("name")
        if name not in params:
            raise KeyError(f"Unbound SQL parameter :{name}")
        return "'" + params[name].replace("'", "''") + "'"
    return _PARAM_RE.sub(repl, statement)


def to_table_fallback(statement: str) -> str:
    """Rewrite an MV DDL into CREATE OR REPLACE TABLE (the target name comes from :target)."""
    return re.sub(r"\bMATERIALIZED\s+VIEW\b", "TABLE", statement, flags=re.IGNORECASE)


def run_gold_tables_fallback(spark, cfg: ProjectConfig) -> list[str]:
    """gold_object_type=table: build Gold as Delta tables with Spark (idempotent CREATE OR REPLACE)."""
    built = []
    for obj in discover_gold_objects():
        for stmt in split_statements(obj.sql()):
            spark.sql(to_table_fallback(bind_parameters(stmt, sql_parameters(cfg, obj.base_name))))
        built.append(cfg.gold(cfg.gold_object_name(obj.base_name)))
        log(logger, "gold table built", object=built[-1])
    return built


def create_on_warehouse(cfg: ProjectConfig, poll_seconds: int = 5, timeout_s: int = 1800) -> list[str]:
    """gold_object_type=materialized_view, outside a job: run the SQL on the configured warehouse."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

    w = WorkspaceClient()
    wh = next((x for x in w.warehouses.list() if x.name == cfg.sql_warehouse_name), None)
    if wh is None:
        raise RuntimeError(f"SQL warehouse '{cfg.sql_warehouse_name}' not found or not accessible")
    created = []
    for obj in discover_gold_objects():
        params = [StatementParameterListItem(name=k, value=v)
                  for k, v in sql_parameters(cfg, obj.base_name).items()]
        for stmt in split_statements(obj.sql()):
            used = [p for p in params if re.search(rf":{p.name}\b", stmt)]
            resp = w.statement_execution.execute_statement(
                warehouse_id=wh.id, statement=stmt, parameters=used, wait_timeout="0s")
            deadline = time.monotonic() + timeout_s
            while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
                if time.monotonic() > deadline:
                    raise TimeoutError(f"Timed out creating {obj.base_name}")
                time.sleep(poll_seconds)
                resp = w.statement_execution.get_statement(resp.statement_id)
            if resp.status.state != StatementState.SUCCEEDED:
                raise RuntimeError(f"{obj.path.name} failed: {resp.status.error}")
        created.append(cfg.gold(cfg.gold_object_name(obj.base_name)))
        log(logger, "materialized view created", object=created[-1])
    return created
