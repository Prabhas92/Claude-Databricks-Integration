"""Shared fixtures.

Spark tests run on the workspace's serverless compute through Databricks Connect (no local JVM is
needed). Set LAKEHOUSE_SKIP_SPARK_TESTS=1 to run only the pure-Python tests.
"""

import os
import time

import pytest

# Naive datetimes in fixtures are interpreted in the local timezone by Spark Connect; pin to UTC
# (same as the serverless session timezone) so dates do not shift across midnight.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    if os.environ.get("LAKEHOUSE_SKIP_SPARK_TESTS") == "1":
        pytest.skip("Spark tests disabled via LAKEHOUSE_SKIP_SPARK_TESTS")
    try:
        from databricks.connect import DatabricksSession

        session = DatabricksSession.builder.serverless().getOrCreate()
        session.range(1).count()
    except Exception as exc:  # no auth / no compute -> skip, don't fail the pure tests
        pytest.skip(f"Databricks Connect serverless session unavailable: {exc}")
    yield session


@pytest.fixture(scope="session")
def cfg():
    from common.config import load_config

    return load_config()
