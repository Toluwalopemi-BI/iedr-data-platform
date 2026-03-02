"""Shared pytest fixtures for IEDR unit tests.

Provides a local PySpark session for testing transforms without Databricks.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local PySpark session for unit testing."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("iedr-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()
