import pytest

from common import data_quality as dq


# ---- pure-Python behaviour ----------------------------------------------------------------

def test_evaluate_operators():
    assert dq.evaluate(0, "==", 0)
    assert dq.evaluate(5, ">=", 3)
    assert not dq.evaluate(0.02, "<=", 0.01)
    assert not dq.evaluate(None, "==", 0)  # missing observation never passes
    with pytest.raises(ValueError):
        dq.evaluate(1, "!=", 1)


def test_status_depends_on_severity():
    crit = dq.make_result("silver", "t", "c", 3, "==", 0, dq.CRITICAL)
    warn = dq.make_result("silver", "t", "c", 3, "==", 0, dq.WARNING)
    ok = dq.make_result("silver", "t", "c", 0, "==", 0, dq.CRITICAL)
    assert (crit.status, warn.status, ok.status) == ("FAIL", "WARN", "PASS")
    assert "observed=3" in crit.message


class _NoWriteRunner(dq.DataQualityRunner):
    def write(self):
        self.written = True


def test_runner_raises_only_on_critical_failures():
    r = _NoWriteRunner(None, "t", "run1", "silver")
    r.add(dq.make_result("silver", "t", "warn_check", 1, "==", 0, dq.WARNING))
    assert r.finalize() == {"PASS": 0, "WARN": 1, "FAIL": 0}

    r.add(dq.make_result("silver", "t", "crit_check", 1, "==", 0, dq.CRITICAL))
    with pytest.raises(dq.DataQualityError, match="crit_check"):
        r.finalize()
    assert r.written  # results are persisted before failing


def test_values_equal_tolerance():
    assert dq.values_equal("g", "t", "recon", 100.004, 100.0, 0.01).passed
    assert not dq.values_equal("g", "t", "recon", 100.5, 100.0, 0.01).passed


# ---- Spark-backed checks ------------------------------------------------------------------



@pytest.mark.spark
def test_unique_key_counts_extra_rows(spark):
    df = spark.createDataFrame([("a", 1), ("a", 2), ("a", 3), ("b", 1)], "k STRING, v INT")
    res = dq.unique_key(df, "silver", "t", ["k"])
    assert res.observed_value == 2 and res.status == "FAIL"
    assert dq.unique_key(df, "silver", "t", ["k", "v"]).passed


@pytest.mark.spark
def test_not_null_and_non_negative(spark):
    df = spark.createDataFrame([("a", 1.0), (None, -2.0), ("c", None)], "k STRING, amt DOUBLE")
    nn = {r.check_name: r.observed_value for r in dq.not_null(df, "s", "t", ["k", "amt"])}
    assert nn == {"not_null[k]": 1.0, "not_null[amt]": 1.0}
    assert dq.non_negative(df, "s", "t", ["amt"])[0].observed_value == 1.0


@pytest.mark.spark
def test_referential_integrity_detects_orphans(spark):
    parent = spark.createDataFrame([("o1",), ("o2",)], "order_id STRING")
    child = spark.createDataFrame([("o1",), ("o1",), ("o9",)], "order_id STRING")
    res = dq.referential_integrity(child, parent, "silver", "items", ["order_id"], "orders")
    assert res.observed_value == 1 and not res.passed


@pytest.mark.spark
def test_violation_ratio_threshold_and_accepted_values(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(s,) for s in ["delivered"] * 98 + ["mystery", "canceled"]], "status STRING")
    ratio = dq.violations(df, "s", "t", "mystery_ratio", F.col("status") == "mystery", max_ratio=0.02)
    assert ratio.passed and ratio.observed_value == 0.01
    av = dq.accepted_values(df, "s", "t", "status", ["delivered", "canceled"])
    assert av.observed_value == 1 and av.status == "WARN"
