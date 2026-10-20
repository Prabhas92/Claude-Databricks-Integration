from pathlib import Path

import pytest
import yaml

from common.config import DEFAULT_CONFIG_PATH, ConfigError, load_config


def _write(tmp_path: Path, mutate) -> Path:
    raw = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())
    mutate(raw)
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(raw))
    return p


def test_default_config_loads_all_sources():
    cfg = load_config()
    assert set(cfg.sources) == {"orders", "order_items", "order_payments", "order_reviews", "customers",
                                "products", "sellers", "geolocation", "category_translation"}
    assert cfg.sources["orders"].url.startswith("https://raw.githubusercontent.com/olist/")
    assert cfg.sources["order_reviews"].primary_key == ["review_id", "order_id"]


def test_schema_mapping_parses_types_and_strips_comments():
    cfg = load_config()
    products = dict(cfg.sources["products"].schema)
    assert products["product_name_lenght"] == "INT"          # trailing YAML comment removed
    assert dict(cfg.sources["order_items"].schema)["price"] == "DECIMAL(12,2)"
    assert "`order_id` STRING" in cfg.sources["orders"].ddl_schema


def test_catalog_precedence(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_CATALOG", "env_catalog")
    assert load_config().catalog == "env_catalog"
    assert load_config(catalog="arg_catalog").catalog == "arg_catalog"


def test_name_helpers_quote_identifiers():
    cfg = load_config(catalog="my-catalog")
    assert cfg.silver("orders") == "`my-catalog`.`silver`.`orders`"
    assert cfg.landing_path == "/Volumes/my-catalog/bronze/landing/olist"
    assert cfg.gold_object_name("daily_sales") == "mv_daily_sales"


def test_invalid_load_mode_rejected(tmp_path):
    with pytest.raises(ConfigError, match="load_mode"):
        load_config(_write(tmp_path, lambda r: r.update(load_mode="incremental")))


def test_primary_key_must_exist_in_schema(tmp_path):
    def mutate(r):
        r["sources"]["orders"]["primary_key"] = ["nope"]
    with pytest.raises(ConfigError, match="primary key"):
        load_config(_write(tmp_path, mutate))


def test_invalid_catalog_identifier_rejected():
    with pytest.raises(ConfigError, match="catalog"):
        load_config(catalog="bad name; DROP")


def test_table_fallback_prefix(tmp_path):
    cfg = load_config(_write(tmp_path, lambda r: r.update(gold_object_type="table")))
    assert cfg.gold_object_name("daily_sales") == "agg_daily_sales"
