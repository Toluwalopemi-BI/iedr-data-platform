# Databricks notebook source
# ===========================================================================
# IEDR Silver Layer — Circuit / Feeder Data Cleansing
# ===========================================================================
# Purpose:  Cleanse, type-cast, deduplicate, and validate circuit data
#           for each utility. Handles the critical schema differences:
#           - U1: Segment-level (64K rows, ~243 segments per feeder)
#           - U2: Feeder-level (1.9K rows, already aggregated)
#
# Tables Created:
#   - silver.u1_circuits_clean
#   - silver.u2_circuits_clean
#
# Key Transformations:
#   1. NULL normalization: "NULL", "", "N/A" → true SQL NULL
#   2. Type casting: string → double, timestamp, int
#   3. Deduplication: latest ingestion per natural key
#   4. DLT Expectations: validate non-null IDs, positive voltage, HC data
# ===========================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _normalize_nulls(df):
    """Replace common null-like strings with true SQL NULL across all columns."""
    null_values = ["NULL", "null", "N/A", "n/a", "NA", "na", "None", "none", ""]
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)).isin(null_values), None)
            .otherwise(F.col(col_name)),
        )
    return df


# ===========================================================================
# Utility 1 — Segment-Level Circuit Data
# ===========================================================================

@dlt.table(
    name="u1_circuits_clean",
    comment=(
        "Cleansed Utility 1 circuit segments. Typed, deduped, DQ-validated. "
        "Segment-level granularity (~243 segments per feeder). "
        "Gold layer aggregates these to feeder level."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_feeder_id", "feeder_id IS NOT NULL")
@dlt.expect("valid_section_id", "section_id IS NOT NULL")
@dlt.expect("positive_voltage", "voltage_kv > 0")
@dlt.expect_or_drop("has_hc_data", "feeder_max_hc_mw IS NOT NULL")
def u1_circuits_clean():
    raw = dlt.read("u1_circuits_raw")
    raw = _normalize_nulls(raw)

    # Deduplication: keep latest ingestion per section
    w = Window.partitionBy("NYHCPV_csv_NSECTION").orderBy(F.desc("_ingestion_ts"))

    return (
        raw.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        # Type casting
        .withColumn("section_id", F.col("NYHCPV_csv_NSECTION").cast("long"))
        .withColumn("feeder_id", F.col("NYHCPV_csv_NFEEDER").cast("long"))
        .withColumn("feeder_name", F.trim(F.col("NYHCPV_csv_FFEEDER")))
        .withColumn("voltage_kv", F.col("NYHCPV_csv_NVOLTAGE").cast("double"))
        .withColumn("segment_max_hc_mw", F.col("NYHCPV_csv_NMAXHC").cast("double"))
        .withColumn("segment_map_color", F.trim(F.col("NYHCPV_csv_NMAPCOLOR")))
        .withColumn("feeder_max_hc_mw", F.col("NYHCPV_csv_FMAXHC").cast("double"))
        .withColumn("feeder_min_hc_mw", F.col("NYHCPV_csv_FMINHC").cast("double"))
        .withColumn("feeder_voltage_kv", F.col("NYHCPV_csv_FVOLTAGE").cast("double"))
        .withColumn("hca_date", F.to_timestamp(F.col("NYHCPV_csv_FHCADATE")))
        .withColumn("num_phases", F.col("Circuits_Phase3_NUMPHASES").cast("int"))
        .withColumn("circuit_id", F.col("Circuits_Phase3_CIRCUIT").cast("long"))
        .withColumn("shape_length", F.col("Shape_Length").cast("double"))
        # Metadata
        .withColumn("_utility_id", F.lit("utility_1"))
        .select(
            "section_id", "feeder_id", "feeder_name", "circuit_id",
            "voltage_kv", "feeder_voltage_kv", "num_phases",
            "segment_max_hc_mw", "segment_map_color",
            "feeder_max_hc_mw", "feeder_min_hc_mw",
            "hca_date", "shape_length",
            "_utility_id", "_source_file", "_ingestion_ts",
        )
    )


# ===========================================================================
# Utility 2 — Feeder-Level Circuit Data
# ===========================================================================

@dlt.table(
    name="u2_circuits_clean",
    comment=(
        "Cleansed Utility 2 feeder data. Already at feeder granularity. "
        "1,909 rows — one per feeder."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_feeder_id", "feeder_id IS NOT NULL")
@dlt.expect("positive_voltage", "voltage_kv > 0")
def u2_circuits_clean():
    raw = dlt.read("u2_circuits_raw")
    raw = _normalize_nulls(raw)

    # Deduplication: keep latest ingestion per feeder
    w = Window.partitionBy("Master_CDF").orderBy(F.desc("_ingestion_ts"))

    return (
        raw.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        # Type casting
        .withColumn("feeder_id", F.trim(F.col("Master_CDF")))
        .withColumn("voltage_kv", F.col("feeder_voltage").cast("double"))
        .withColumn("feeder_max_hc_mw", F.col("feeder_max_hc").cast("double"))
        .withColumn("feeder_min_hc_mw", F.col("feeder_min_hc").cast("double"))
        .withColumn("dg_since_refresh_mw", F.col("feeder_dg_connected_since_refresh").cast("double"))
        .withColumn("hca_date", F.to_timestamp(F.col("hca_refresh_date")))
        .withColumn("map_color", F.lower(F.trim(F.col("color"))))
        .withColumn("shape_length", F.col("shape_length").cast("double"))
        # Metadata
        .withColumn("_utility_id", F.lit("utility_2"))
        .select(
            "feeder_id", "voltage_kv",
            "feeder_max_hc_mw", "feeder_min_hc_mw",
            "dg_since_refresh_mw", "hca_date",
            "map_color", "shape_length",
            "_utility_id", "_source_file", "_ingestion_ts",
        )
    )
