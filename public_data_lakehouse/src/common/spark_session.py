"""Return a SparkSession on Databricks, or a serverless Databricks Connect session locally."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def is_databricks_runtime() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark() -> SparkSession:
    if is_databricks_runtime():
        return SparkSession.builder.getOrCreate()
    # Local development / tests: run on the workspace's serverless compute via Databricks Connect.
    # Auth comes from the CLI profile (DATABRICKS_CONFIG_PROFILE); no credentials in code.
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless().getOrCreate()
