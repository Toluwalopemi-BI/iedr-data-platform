"""Integration tests for the IEDR DLT pipeline.

These tests require a Databricks workspace with Unity Catalog.
They are NOT run in CI — only after TEST deployment via cd-test.yml.

Usage:
    databricks bundle run --job iedr_integration_tests -t test
"""

# NOTE: Integration tests are run as a Databricks notebook job.
# They validate end-to-end data flow after DLT pipeline execution.


def test_bronze_tables_populated():
    """Verify all 6 bronze tables have rows after pipeline run."""
    bronze_tables = [
        "u1_circuits_raw", "u1_install_der_raw", "u1_planned_der_raw",
        "u2_circuits_raw", "u2_install_der_raw", "u2_planned_der_raw",
    ]
    for table in bronze_tables:
        count = spark.table(f"bronze.{table}").count()  # noqa: F821
        assert count > 0, f"bronze.{table} is empty"


def test_gold_dim_feeder_has_both_utilities():
    """Verify dim_feeder contains feeders from both utilities."""
    df = spark.table("gold.dim_feeder")  # noqa: F821
    utilities = [r.utility_id for r in df.select("utility_id").distinct().collect()]
    assert "utility_1" in utilities
    assert "utility_2" in utilities


def test_platinum_freshness_not_stale():
    """Verify no datasets are flagged as stale (>35 days)."""
    df = spark.table("platinum.v_ingestion_metadata")  # noqa: F821
    stale = df.filter("is_stale = true").count()
    assert stale == 0, f"{stale} dataset(s) are stale (>35 days since last load)"


def test_gold_fact_der_feeder_coverage():
    """Verify installed DER feeder null rate is within acceptable bounds."""
    df = spark.table("platinum.v_data_quality_report")  # noqa: F821
    for row in df.collect():
        null_pct = row.installed_null_feeder_pct or 0
        assert null_pct < 50, (
            f"{row.utility_id}: installed DER feeder null rate "
            f"is {null_pct}% — above 50% threshold"
        )
