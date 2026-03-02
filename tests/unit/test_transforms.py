"""Unit tests for shared.transforms — validates core PySpark transformations.

Uses chispa for DataFrame equality assertions.
Runs locally without Databricks via the spark fixture in conftest.py.
"""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from shared.transforms import add_global_id, deduplicate_by_key, normalize_nulls


# ---------------------------------------------------------------------------
# normalize_nulls
# ---------------------------------------------------------------------------

class TestNormalizeNulls:
    """Test null normalization across common patterns."""

    def test_replaces_null_string(self, spark):
        df = spark.createDataFrame([("NULL",), ("hello",)], ["col1"])
        result = normalize_nulls(df)
        values = [row.col1 for row in result.collect()]
        assert values == [None, "hello"]

    def test_replaces_empty_string(self, spark):
        df = spark.createDataFrame([("",), ("data",)], ["col1"])
        result = normalize_nulls(df)
        values = [row.col1 for row in result.collect()]
        assert values == [None, "data"]

    def test_replaces_na_variants(self, spark):
        df = spark.createDataFrame([("N/A",), ("n/a",), ("NA",), ("None",)], ["col1"])
        result = normalize_nulls(df)
        values = [row.col1 for row in result.collect()]
        assert all(v is None for v in values)

    def test_preserves_valid_data(self, spark):
        df = spark.createDataFrame([("valid",), ("123",), ("  spaced  ",)], ["col1"])
        result = normalize_nulls(df)
        values = [row.col1 for row in result.collect()]
        assert values == ["valid", "123", "  spaced  "]

    def test_handles_multiple_columns(self, spark):
        df = spark.createDataFrame([("NULL", "hello"), ("data", "N/A")], ["a", "b"])
        result = normalize_nulls(df)
        rows = result.collect()
        assert rows[0].a is None and rows[0].b == "hello"
        assert rows[1].a == "data" and rows[1].b is None


# ---------------------------------------------------------------------------
# deduplicate_by_key
# ---------------------------------------------------------------------------

class TestDeduplicateByKey:
    """Test deduplication keeps latest record per key."""

    def test_keeps_latest_by_timestamp(self, spark):
        data = [
            ("A", "2024-01-01", "old"),
            ("A", "2024-06-01", "new"),
            ("B", "2024-03-01", "only"),
        ]
        df = spark.createDataFrame(data, ["key", "_ingestion_ts", "value"])
        result = deduplicate_by_key(df, "key", "_ingestion_ts")
        rows = {row.key: row.value for row in result.collect()}
        assert rows["A"] == "new"
        assert rows["B"] == "only"
        assert result.count() == 2

    def test_single_record_preserved(self, spark):
        df = spark.createDataFrame([("X", "2024-01-01")], ["key", "_ingestion_ts"])
        result = deduplicate_by_key(df, "key")
        assert result.count() == 1


# ---------------------------------------------------------------------------
# add_global_id
# ---------------------------------------------------------------------------

class TestAddGlobalId:
    """Test global ID generation."""

    def test_creates_composite_id(self, spark):
        df = spark.createDataFrame([("u1", "123"), ("u2", "456")], ["utility", "local_id"])
        result = add_global_id(df, "utility", "local_id", "global_id")
        values = [row.global_id for row in result.collect()]
        assert values == ["u1::123", "u2::456"]

    def test_null_local_id_produces_null(self, spark):
        df = spark.createDataFrame([("u1", None)], ["utility", "local_id"])
        result = add_global_id(df, "utility", "local_id", "global_id")
        assert result.collect()[0].global_id is None
