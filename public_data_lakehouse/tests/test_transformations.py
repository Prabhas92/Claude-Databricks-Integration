"""Transformation tests. All data here is synthetic and test-only (see tests/fixtures/README.md)."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from ingestion import bronze_ingestion as bi
from transformations import gold_transformations as gt

FIXTURES = Path(__file__).parent / "fixtures"
TS = datetime(2024, 1, 1, 0, 0, 0)


# ---- pure-Python: bronze helpers ----------------------------------------------------------

def test_parse_header_handles_bom_and_quotes():
    assert bi.parse_header('﻿"a","b",c\r\n') == ["a", "b", "c"]


def test_header_drift_is_detected(cfg):
    with open(FIXTURES / "bad_header_orders.csv", encoding="utf-8-sig") as fh:
        problems = bi.header_mismatches(bi.parse_header(fh.readline()), cfg.sources["orders"].columns)
    assert problems == ["column 2: expected 'order_status', found 'status'"]


def test_header_matches_for_valid_fixture(cfg):
    with open(FIXTURES / "orders_malformed.csv", encoding="utf-8-sig") as fh:
        assert bi.header_mismatches(bi.parse_header(fh.readline()), cfg.sources["orders"].columns) == []


# ---- pure-Python: gold SQL handling -------------------------------------------------------

def test_gold_sql_discovery_and_statement_split():
    objs = gt.discover_gold_objects()
    assert [o.base_name for o in objs][:2] == ["daily_sales", "monthly_revenue"]
    assert len(objs) == 9
    stmts = gt.split_statements(objs[0].sql())
    assert len(stmts) == 1 and stmts[0].startswith("CREATE OR REPLACE MATERIALIZED VIEW IDENTIFIER(:target)")


def test_sql_parameters_are_fully_qualified(cfg):
    p = gt.sql_parameters(cfg, "daily_sales")
    assert p["target"] == f"`{cfg.catalog}`.`gold`.`mv_daily_sales`"
    assert p["fact_order_details"] == f"`{cfg.catalog}`.`silver`.`fact_order_details`"
    for obj in gt.discover_gold_objects():                    # every marker in every file is bound
        for stmt in gt.split_statements(obj.sql()):
            gt.bind_parameters(stmt, gt.sql_parameters(cfg, obj.base_name))


def test_table_fallback_rewrites_type_and_name(cfg):
    from dataclasses import replace

    tcfg = replace(cfg, gold_object_type="table")
    stmt = gt.split_statements(gt.discover_gold_objects()[0].sql())[0]
    out = gt.to_table_fallback(gt.bind_parameters(stmt, gt.sql_parameters(tcfg, "daily_sales")))
    assert out.startswith(f"CREATE OR REPLACE TABLE IDENTIFIER('`{cfg.catalog}`.`gold`.`agg_daily_sales`')")
    assert "MATERIALIZED" not in out and ":" + "target" not in out


def test_bind_parameters_rejects_unknown_and_escapes():
    assert gt.bind_parameters("SELECT :a, x::int", {"a": "it's"}) == "SELECT 'it''s', x::int"
    with pytest.raises(KeyError):
        gt.bind_parameters("SELECT :missing", {})


# ---- Spark: silver transformations --------------------------------------------------------

@pytest.fixture(scope="module")
def st():
    from transformations import silver_transformations

    return silver_transformations


ORDERS_DDL = ("order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp TIMESTAMP, "
              "order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, "
              "order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP, "
              "_ingested_at TIMESTAMP, _snapshot_id STRING")


def _orders(spark):
    d = lambda day, h=0: datetime(2018, 1, day, h)  # noqa: E731
    rows = [
        ("o1", "c1", " Delivered ", d(1), d(1, 1), d(2), d(5), d(10), TS, "s1"),   # on time
        ("o2", "c2", "delivered", d(1), d(1, 1), d(2), d(12), d(10), TS, "s1"),    # late
        ("o3", "c3", "canceled", d(2), None, None, None, d(10), TS, "s1"),         # not a sale
        ("o4", "c4", "teleported", d(2), None, None, None, d(10), TS, "s1"),       # unknown status
        ("o5", None, "delivered", d(3), None, None, None, d(10), TS, "s1"),        # rejected
        ("o1", "c1", "delivered", d(1), d(1, 1), d(2), d(5), d(10), datetime(2023, 1, 1), "s1"),  # dup, older
    ]
    return spark.createDataFrame(rows, ORDERS_DDL)


@pytest.mark.spark
def test_clean_str_and_zip_normalisation(spark, st):
    df = spark.createDataFrame([("  sao   paulo ", "1037"), ("", "01037"), ("x", "ABC")], "c STRING, z STRING")
    out = df.select(st.clean_str("c").alias("c"), st.normalize_zip_prefix("z").alias("z")).collect()
    assert [(r.c, r.z) for r in out] == [("sao paulo", "01037"), (None, "01037"), ("x", None)]


@pytest.mark.spark
def test_clean_orders_flags_rejects_and_dedupes(spark, st, cfg):
    res = st.clean_orders(_orders(spark), cfg.business_rules)
    rows = {r.order_id: r for r in res.df.collect()}
    assert sorted(rows) == ["o1", "o2", "o3", "o4"]                       # o5 rejected, o1 deduped
    assert rows["o1"].order_status == "delivered" and rows["o1"].is_late_delivery is False
    assert rows["o2"].is_late_delivery is True
    assert rows["o3"].is_sale_eligible is False
    assert rows["o4"].is_known_status is False and rows["o4"].is_sale_eligible is False
    assert rows["o1"]._ingested_at == TS                                   # most recent ingestion wins
    rej = res.rejected.collect()
    assert [(r.business_key, r.reason) for r in rej] == [("o5", "null customer_id")]


@pytest.mark.spark
def test_deduplicate_is_deterministic_and_preserves_line_items(spark, st):
    rows = [("o1", 1, 10.0, TS), ("o1", 1, 10.0, TS), ("o1", 2, 5.0, TS), ("o1", 1, 99.0, TS)]
    df = spark.createDataFrame(rows, "order_id STRING, order_item_id INT, price DOUBLE, _ingested_at TIMESTAMP")
    first = sorted(tuple(r) for r in st.deduplicate(df, ["order_id", "order_item_id"]).collect())
    again = sorted(tuple(r) for r in st.deduplicate(df, ["order_id", "order_item_id"]).collect())
    assert first == again                                   # idempotent / deterministic survivor
    assert [r[1] for r in first] == [1, 2]                  # two legitimate line items survive


def _fact_inputs(spark, st, cfg):
    orders = st.clean_orders(_orders(spark), cfg.business_rules).df
    items = spark.createDataFrame([
        ("o1", 1, "p1", "s1", TS, Decimal("100.00"), Decimal("10.00"), Decimal("110.00")),
        ("o1", 2, "p2", "s1", TS, Decimal("50.00"), Decimal("5.00"), Decimal("55.00")),
        ("o2", 1, "p1", "s2", TS, Decimal("20.00"), Decimal("0.00"), Decimal("20.00")),
    ], "order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, shipping_limit_date TIMESTAMP, "
       "price DECIMAL(12,2), freight_value DECIMAL(12,2), item_total DECIMAL(12,2)")
    payments = spark.createDataFrame([
        ("o1", 1, "credit_card", 3, Decimal("150.00")),
        ("o1", 2, "voucher", 1, Decimal("15.00")),       # 2 payments x 2 items -> must not multiply
        ("o2", 1, "boleto", 1, Decimal("20.00")),
    ], "order_id STRING, payment_sequential INT, payment_type STRING, payment_installments INT, "
       "payment_value DECIMAL(12,2)")
    customers = spark.createDataFrame([(f"c{i}", f"u{i}", "01037", "sp", "SP") for i in range(1, 5)],
                                      "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, "
                                      "customer_city STRING, customer_state STRING")
    products = spark.createDataFrame([("p1", "beleza_saude", "health_beauty", "translated"),
                                      ("p2", None, None, "missing_in_source")],
                                     "product_id STRING, product_category_name STRING, "
                                     "product_category_name_english STRING, category_resolution STRING")
    sellers = spark.createDataFrame([("s1", "01037", "sp", "SP"), ("s2", "20000", "rj", "RJ")],
                                    "seller_id STRING, seller_zip_code_prefix STRING, seller_city STRING, seller_state STRING")
    reviews = spark.createDataFrame([("r1", "o1", 5), ("r2", "o1", 3)], "review_id STRING, order_id STRING, review_score INT")
    return orders, items, customers, products, sellers, payments, reviews


@pytest.mark.spark
def test_fact_grain_no_row_multiplication_and_payment_allocation(spark, st, cfg):
    from pyspark.sql import functions as F

    fact = st.build_fact_order_details(*_fact_inputs(spark, st, cfg))
    assert fact.count() == 3                                                    # one row per item
    assert fact.groupBy("order_id", "order_item_id").count().filter("count > 1").count() == 0
    o1 = {r.order_item_id: r for r in fact.filter("order_id = 'o1'").collect()}
    assert o1[1].order_payment_total == Decimal("165.00") and o1[1].payment_count == 2
    assert o1[1].primary_payment_type == "credit_card"
    assert o1[1].order_items_total == Decimal("165.00") and o1[1].order_item_count == 2
    assert o1[1].review_score_avg == 4.0
    # allocated payment sums back to the order payment total (no double counting)
    total_alloc = fact.agg(F.sum("item_payment_allocated")).first()[0]
    assert abs(float(total_alloc) - 185.0) < 1e-6
    assert float(o1[1].item_payment_allocated) == pytest.approx(110.0)


@pytest.mark.spark
def test_reject_orphans_moves_children_without_parent(spark, st, cfg):
    orders = st.clean_orders(_orders(spark), cfg.business_rules).df
    items = spark.createDataFrame([("o1", 1), ("o404", 1)], "order_id STRING, order_item_id INT")
    empty_rej = st.to_rejected(items.limit(0), "order_items", ["order_id"], "x")
    res = st.reject_orphans(st.Cleaned(items, empty_rej, ["order_id", "order_item_id"]),
                            orders, "order_items", ["order_id"], "orders")
    assert [r.order_id for r in res.df.collect()] == ["o1"]
    assert [r.business_key for r in res.rejected.collect()] == ["o404|1"]


@pytest.mark.spark
def test_geolocation_collapses_to_zip_and_rejects_outliers(spark, st, cfg):
    rows = [("1037", -23.5, -46.6, "sao paulo", "SP"), ("01037", -23.6, -46.7, "sao paulo", "SP"),
            ("01037", -23.55, -46.65, "são paulo", "SP"), ("01037", 40.0, -3.0, "madrid", "SP")]
    df = spark.createDataFrame([r + ("snap",) for r in rows],
                               "geolocation_zip_code_prefix STRING, geolocation_lat DOUBLE, geolocation_lng DOUBLE, "
                               "geolocation_city STRING, geolocation_state STRING, _snapshot_id STRING")
    res = st.build_geolocation_zip(df, cfg.business_rules["brazil_bbox"])
    out = res.df.collect()
    assert len(out) == 1 and out[0].zip_code_prefix == "01037" and out[0].source_point_count == 3
    assert out[0].city == "sao paulo"                                          # modal city
    assert res.rejected.count() == 1


@pytest.mark.spark
def test_gold_aggregation_matches_metric_definition(spark, st, cfg):
    """Re-run the daily-sales SQL logic against a tiny fact to check AOV / GMV definitions."""
    fact = st.build_fact_order_details(*_fact_inputs(spark, st, cfg))
    fact.createOrReplaceTempView("fact_order_details")
    stmt = gt.split_statements(gt.discover_gold_objects()[0].sql())[0]
    select_sql = stmt[stmt.index("WITH order_level"):].replace("IDENTIFIER(:fact_order_details)", "fact_order_details")
    rows = {r.order_date.day: r for r in spark.sql(select_sql).collect()}
    day1 = rows[1]                                         # o1 and o2 purchased on 2018-01-01
    assert day1.order_count == 2 and day1.item_count == 3
    assert day1.gmv == Decimal("170.00") and day1.freight_value == Decimal("15.00")
    assert day1.payment_total == Decimal("185.00")
    assert day1.avg_order_value == Decimal("92.50")        # (165 + 20) / 2 orders


@pytest.mark.spark
def test_bronze_csv_parsing_quarantines_malformed_rows(spark, cfg):
    """Upload the malformed fixture to the landing volume and read it with the bronze reader."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    target = f"{cfg.landing_path}/_unit_test/orders_malformed.csv"
    try:
        with open(FIXTURES / "orders_malformed.csv", "rb") as fh:
            w.files.upload(target, fh, overwrite=True)
    except Exception as exc:
        pytest.skip(f"landing volume not available yet ({exc}); run the validate step first")
    try:
        df = bi.read_source_csv(spark, target, cfg.sources["orders"])
        rows = {r.order_id: r for r in df.collect()}
        assert rows["o1"]._corrupt_record is None
        assert rows["o2"]._corrupt_record is not None              # bad timestamp kept + flagged
        assert rows["o3"].order_status == "shipped\nlate"          # multi-line quoted field parsed
        assert rows["o4"]._corrupt_record is not None              # extra column flagged
        assert len(rows) == 4                                      # nothing silently dropped
    finally:
        w.files.delete(target)
