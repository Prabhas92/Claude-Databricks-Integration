"""Reusable data-quality checks.

Every check computes a single aggregate on the cluster (no driver-side collection of data) and
returns a CheckResult. A DataQualityRunner collects results, appends them to the auditable
results table and fails the pipeline when any *critical* check fails.
"""

from __future__ import annotations

import operator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

CRITICAL = "critical"
WARNING = "warning"

_OPS: dict[str, Callable[[float, float], bool]] = {
    "==": operator.eq,
    "<=": operator.le,
    ">=": operator.ge,
    "<": operator.lt,
    ">": operator.gt,
}

DQ_RESULTS_DDL = (
    "run_id STRING, layer STRING, dataset STRING, check_name STRING, severity STRING, "
    "status STRING, observed_value DOUBLE, expected_condition STRING, message STRING, "
    "executed_at TIMESTAMP"
)


class DataQualityError(RuntimeError):
    """Raised when one or more critical data-quality checks fail."""


@dataclass(frozen=True)
class CheckResult:
    layer: str
    dataset: str
    check_name: str
    severity: str
    passed: bool
    observed_value: float | None
    expected_condition: str
    message: str

    @property
    def status(self) -> str:
        if self.passed:
            return "PASS"
        return "FAIL" if self.severity == CRITICAL else "WARN"


def evaluate(observed: float | None, op: str, threshold: float) -> bool:
    """Pure comparison helper (unit-testable). A missing observation never passes."""
    if observed is None:
        return False
    if op not in _OPS:
        raise ValueError(f"Unsupported operator {op!r}")
    return _OPS[op](observed, threshold)


def make_result(layer: str, dataset: str, check_name: str, observed: float | None, op: str,
                threshold: float, severity: str = CRITICAL, detail: str = "") -> CheckResult:
    passed = evaluate(observed, op, threshold)
    expected = f"value {op} {threshold:g}"
    message = f"{check_name}: observed={observed} expected {op} {threshold:g}"
    if detail:
        message += f" ({detail})"
    return CheckResult(layer, dataset, check_name, severity, passed,
                       None if observed is None else float(observed), expected, message)


def scalar(df: DataFrame, expr: Column) -> float:
    value = df.agg(expr.alias("v")).first()["v"]
    return float(value) if value is not None else 0.0


# ---- check library ------------------------------------------------------------------------

def row_count_at_least(df: DataFrame, layer: str, dataset: str, minimum: int,
                       severity: str = CRITICAL) -> CheckResult:
    return make_result(layer, dataset, "row_count_min", df.count(), ">=", minimum, severity)


def unique_key(df: DataFrame, layer: str, dataset: str, keys: list[str],
               severity: str = CRITICAL) -> CheckResult:
    dupes = df.groupBy(*keys).count().filter(F.col("count") > 1)
    observed = scalar(dupes, F.coalesce(F.sum(F.col("count") - 1), F.lit(0)))
    return make_result(layer, dataset, f"unique_key[{','.join(keys)}]", observed, "==", 0,
                       severity, "extra rows sharing a business key")


def not_null(df: DataFrame, layer: str, dataset: str, columns: Iterable[str],
             severity: str = CRITICAL) -> list[CheckResult]:
    columns = list(columns)
    row = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).first()
    return [make_result(layer, dataset, f"not_null[{c}]", row[c] or 0, "==", 0, severity)
            for c in columns]


def violations(df: DataFrame, layer: str, dataset: str, check_name: str, condition: Column,
               severity: str = CRITICAL, max_ratio: float = 0.0, detail: str = "") -> CheckResult:
    """Count rows where `condition` is TRUE (a violation). Passes if ratio <= max_ratio."""
    row = df.agg(F.count(F.lit(1)).alias("n"),
                 F.sum(F.when(condition, 1).otherwise(0)).alias("bad")).first()
    n, bad = row["n"] or 0, row["bad"] or 0
    if max_ratio == 0:
        return make_result(layer, dataset, check_name, bad, "==", 0, severity, detail)
    ratio = bad / n if n else 0.0
    return make_result(layer, dataset, check_name, round(ratio, 6), "<=", max_ratio, severity,
                       f"{bad} of {n} rows. {detail}".strip())


def accepted_values(df: DataFrame, layer: str, dataset: str, column: str, allowed: list[str],
                    severity: str = WARNING) -> CheckResult:
    cond = F.col(column).isNotNull() & ~F.col(column).isin(allowed)
    return violations(df, layer, dataset, f"accepted_values[{column}]", cond, severity,
                      detail=f"allowed={allowed}")


def non_negative(df: DataFrame, layer: str, dataset: str, columns: list[str],
                 severity: str = CRITICAL) -> list[CheckResult]:
    return [violations(df, layer, dataset, f"non_negative[{c}]", F.col(c) < 0, severity)
            for c in columns]


def referential_integrity(child: DataFrame, parent: DataFrame, layer: str, dataset: str,
                          keys: list[str], parent_name: str,
                          severity: str = CRITICAL) -> CheckResult:
    orphans = child.select(*keys).join(parent.select(*keys).distinct(), keys, "left_anti")
    return make_result(layer, dataset, f"fk[{','.join(keys)}]->{parent_name}", orphans.count(),
                       "==", 0, severity, "orphan child rows")


def values_equal(layer: str, dataset: str, check_name: str, observed: float, expected: float,
                 tolerance: float = 0.0, severity: str = CRITICAL) -> CheckResult:
    """Reconciliation check between two already-computed aggregates."""
    diff = abs(float(observed) - float(expected))
    return make_result(layer, dataset, check_name, round(diff, 4), "<=", tolerance, severity,
                       f"observed={observed} expected={expected}")


# ---- runner -------------------------------------------------------------------------------

class DataQualityRunner:
    def __init__(self, spark: SparkSession, results_table: str, run_id: str, layer: str):
        self.spark = spark
        self.results_table = results_table
        self.run_id = run_id
        self.layer = layer
        self.results: list[CheckResult] = []

    def add(self, result: CheckResult | Iterable[CheckResult]) -> None:
        if isinstance(result, CheckResult):
            self.results.append(result)
        else:
            self.results.extend(result)

    @property
    def critical_failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "FAIL"]

    def summary(self) -> dict[str, int]:
        out = {"PASS": 0, "WARN": 0, "FAIL": 0}
        for r in self.results:
            out[r.status] += 1
        return out

    def write(self) -> None:
        if not self.results:
            return
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = [(self.run_id, r.layer, r.dataset, r.check_name, r.severity, r.status,
                 r.observed_value, r.expected_condition, r.message, now) for r in self.results]
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.results_table} ({DQ_RESULTS_DDL}) "
                       "COMMENT 'Auditable data-quality check results for every pipeline run'")
        (self.spark.createDataFrame(rows, DQ_RESULTS_DDL)
         .write.mode("append").saveAsTable(self.results_table))

    def finalize(self) -> dict[str, int]:
        """Persist results, then raise if any critical check failed."""
        self.write()
        if self.critical_failures:
            details = "; ".join(r.message for r in self.critical_failures)
            raise DataQualityError(
                f"{len(self.critical_failures)} critical {self.layer} DQ check(s) failed: {details}")
        return self.summary()
