"""Unit tests for shared.transforms module.

Tests run locally with PySpark — no Databricks cluster needed.
Uses chispa for DataFrame equality assertions.
"""

import pytest
from pyspark.sql import functions as F
from chispa.dataframe_comparer import assert_df_equality

from shared.transforms import normalize_nulls, deduplicate_by_key, add_global_id


class TestNormalizeNulls:
    """Tests for the normalize_nulls function."""

    def test_replaces_null_string(self, spark):
        df = spark.createDataFrame([("NULL",), ("valid",)], ["col1"])
        result = normalize_nulls(df)
        assert result.filter("col1 IS NULL").count() == 1
        assert result.filter("col1 = 'valid'").count() == 1

    def test_replaces_empty_string(self, spark):
        df = spark.createDataFrame([("",), ("data",)], ["col1"])
        result = normalize_nulls(df)
        assert result.filter("col1 IS NULL").count() == 1

    def test_replaces_na_variants(self, spark):
        df = spark.createDataFrame([("N/A",), ("n/a",), ("NA",), ("None",)], ["col1"])
        result = normalize_nulls(df)
        assert result.filter("col1 IS NULL").count() == 4

    def test_preserves_valid_data(self, spark):
        df = spark.createDataFrame([("abc",), ("123",), ("Solar PV",)], ["col1"])
        result = normalize_nulls(df)
        assert result.filter("col1 IS NULL").count() == 0

    def test_handles_multiple_columns(self, spark):
        df = spark.createDataFrame([("NULL", "valid"), ("data", "N/A")], ["a", "b"])
        result = normalize_nulls(df)
        row1 = result.collect()[0]
        row2 = result.collect()[1]
        assert row1["a"] is None and row1["b"] == "valid"
        assert row2["a"] == "data" and row2["b"] is None


class TestDeduplicateByKey:
    """Tests for the deduplicate_by_key function."""

    def test_keeps_latest_record(self, spark):
        from datetime import datetime
        df = spark.createDataFrame(
            [
                ("A", datetime(2024, 1, 1), "old"),
                ("A", datetime(2024, 6, 1), "new"),
                ("B", datetime(2024, 3, 1), "only"),
            ],
            ["id", "_ingestion_ts", "value"],
        )
        result = deduplicate_by_key(df, "id")
        assert result.count() == 2
        a_row = result.filter("id = 'A'").collect()[0]
        assert a_row["value"] == "new"

    def test_single_record_per_key_unchanged(self, spark):
        from datetime import datetime
        df = spark.createDataFrame(
            [("X", datetime(2024, 1, 1), "val")],
            ["id", "_ingestion_ts", "value"],
        )
        result = deduplicate_by_key(df, "id")
        assert result.count() == 1


class TestAddGlobalId:
    """Tests for the add_global_id function."""

    def test_creates_composite_id(self, spark):
        df = spark.createDataFrame([("utility_1", "F001")], ["utility_id", "feeder_id"])
        result = add_global_id(df, "utility_id", "feeder_id", "global_id")
        assert result.collect()[0]["global_id"] == "utility_1::F001"

    def test_null_local_id_produces_null_global(self, spark):
        df = spark.createDataFrame([("utility_1", None)], ["utility_id", "feeder_id"])
        result = add_global_id(df, "utility_id", "feeder_id", "global_id")
        assert result.collect()[0]["global_id"] is None
