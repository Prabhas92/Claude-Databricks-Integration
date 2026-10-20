"""Project configuration: loads config/project_config.yaml and resolves fully-qualified names."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "project_config.yaml"

VALID_LOAD_MODES = {"snapshot", "full_refresh"}
VALID_GOLD_OBJECT_TYPES = {"materialized_view", "table"}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")


class ConfigError(ValueError):
    """Raised when the project configuration is invalid."""


@dataclass(frozen=True)
class SourceConfig:
    name: str
    file: str
    url: str
    primary_key: list[str]
    schema: list[tuple[str, str]]  # (column, spark SQL type)

    @property
    def ddl_schema(self) -> str:
        return ", ".join(f"`{c}` {t}" for c, t in self.schema)

    @property
    def columns(self) -> list[str]:
        return [c for c, _ in self.schema]


@dataclass(frozen=True)
class ProjectConfig:
    project_name: str
    catalog: str
    schemas: dict[str, str]
    landing_volume: str
    landing_subdir: str
    load_mode: str
    gold_object_type: str
    gold_object_prefix: dict[str, str]
    sql_warehouse_name: str
    tables: dict[str, str]
    business_rules: dict[str, Any]
    data_quality: dict[str, Any]
    source_name: str
    sources: dict[str, SourceConfig] = field(default_factory=dict)

    # ---- name helpers -------------------------------------------------------------------
    def schema_fqn(self, layer: str) -> str:
        return f"`{self.catalog}`.`{self.schemas[layer]}`"

    def table_fqn(self, layer: str, table: str) -> str:
        return f"{self.schema_fqn(layer)}.`{table}`"

    def bronze(self, table: str) -> str:
        return self.table_fqn("bronze", table)

    def silver(self, table: str) -> str:
        return self.table_fqn("silver", table)

    def gold(self, table: str) -> str:
        return self.table_fqn("gold", table)

    def gold_object_name(self, base_name: str) -> str:
        """e.g. 'daily_sales' -> 'mv_daily_sales' (MV) or 'agg_daily_sales' (table fallback)."""
        return f"{self.gold_object_prefix[self.gold_object_type]}{base_name}"

    @property
    def landing_path(self) -> str:
        return (
            f"/Volumes/{self.catalog}/{self.schemas['bronze']}/"
            f"{self.landing_volume}/{self.landing_subdir}"
        )


def _parse_schema(entries: list[str], source: str) -> list[tuple[str, str]]:
    parsed = []
    for entry in entries:
        parts = entry.split(None, 1)
        if len(parts) != 2:
            raise ConfigError(f"Source '{source}': schema entry '{entry}' must be '<column> <TYPE>'")
        parsed.append((parts[0], parts[1].strip()))
    return parsed


def _require_identifier(value: str, what: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise ConfigError(f"Invalid {what}: {value!r}")
    return value


def load_config(path: str | Path | None = None, catalog: str | None = None) -> ProjectConfig:
    """Load and validate configuration.

    Catalog precedence: explicit argument > LAKEHOUSE_CATALOG env var > YAML value.
    """
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not cfg_path.exists():
        raise ConfigError(f"Config file not found: {cfg_path}")
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}

    resolved_catalog = catalog or os.environ.get("LAKEHOUSE_CATALOG") or raw.get("catalog")
    _require_identifier(resolved_catalog, "catalog")

    schemas = raw.get("schemas", {})
    for layer in ("bronze", "silver", "gold"):
        if layer not in schemas:
            raise ConfigError(f"Missing schema for layer '{layer}'")
        _require_identifier(schemas[layer], f"{layer} schema")

    load_mode = raw.get("load_mode", "snapshot")
    if load_mode not in VALID_LOAD_MODES:
        raise ConfigError(f"load_mode must be one of {sorted(VALID_LOAD_MODES)}, got {load_mode!r}")

    gold_type = raw.get("gold_object_type", "materialized_view")
    if gold_type not in VALID_GOLD_OBJECT_TYPES:
        raise ConfigError(
            f"gold_object_type must be one of {sorted(VALID_GOLD_OBJECT_TYPES)}, got {gold_type!r}"
        )

    base_url = raw["source_base_url"].rstrip("/")
    sources = {}
    for name, spec in (raw.get("sources") or {}).items():
        _require_identifier(name, "source name")
        schema = _parse_schema(spec["schema"], name)
        cols = {c for c, _ in schema}
        missing_pk = [k for k in spec.get("primary_key", []) if k not in cols]
        if missing_pk:
            raise ConfigError(f"Source '{name}': primary key columns {missing_pk} not in schema")
        sources[name] = SourceConfig(
            name=name,
            file=spec["file"],
            url=spec.get("url") or f"{base_url}/{spec['file']}",
            primary_key=list(spec.get("primary_key", [])),
            schema=schema,
        )
    if not sources:
        raise ConfigError("No sources configured")

    return ProjectConfig(
        project_name=raw.get("project_name", "public_data_lakehouse"),
        catalog=resolved_catalog,
        schemas={k: schemas[k] for k in ("bronze", "silver", "gold")},
        landing_volume=_require_identifier(raw.get("landing_volume", "landing"), "landing_volume"),
        landing_subdir=raw.get("landing_subdir", "olist"),
        load_mode=load_mode,
        gold_object_type=gold_type,
        gold_object_prefix=raw.get("gold_object_prefix", {"materialized_view": "mv_", "table": "agg_"}),
        sql_warehouse_name=raw.get("sql_warehouse_name", ""),
        tables=raw.get("tables", {}),
        business_rules=raw.get("business_rules", {}),
        data_quality=raw.get("data_quality", {}),
        source_name=raw.get("source_name", "public_source"),
        sources=sources,
    )
