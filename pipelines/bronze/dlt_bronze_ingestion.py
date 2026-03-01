# Databricks notebook source
# ===========================================================================
# IEDR Bronze Layer — Raw Data Ingestion via Auto Loader
# ===========================================================================
# Purpose:  Ingest raw utility CSV files from cloud landing zone into
#           append-only Delta streaming tables. All columns read as strings
#           to prevent type-casting failures at ingestion.
#
# Tables Created:
#   - bronze.u1_circuits_raw
#   - bronze.u1_install_der_raw
#   - bronze.u1_planned_der_raw
#   - bronze.u2_circuits_raw
#   - bronze.u2_install_der_raw
#   - bronze.u2_planned_der_raw
#
# Design Decisions:
#   1. inferSchema=false: Read everything as strings. Silver handles casting.
#   2. schemaEvolutionMode=addNewColumns: New columns auto-added, never fail.
#   3. Metadata enrichment: _source_file, _ingestion_ts, _utility_id.
#   4. Append-only: Bronze is an immutable audit log.
# ===========================================================================

import dlt
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LANDING_PATH = spark.conf.get("iedr.landing_path")
ENV = spark.conf.get("iedr.environment", "dev")


def _build_bronze_stream(source_path, utility_id):
    """Build a raw Auto Loader stream with metadata enrichment.

    Args:
        source_path: Cloud storage path to watch for new CSV files.
        utility_id: Identifier for the source utility (e.g., 'utility_1').

    Returns:
        Streaming DataFrame with all columns as strings plus metadata columns.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")                    # All strings
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaHints", "")              # No hints needed
        .load(source_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_utility_id", F.lit(utility_id))
        .withColumn("_environment", F.lit(ENV))
    )


# ===========================================================================
# Utility 1 — Bronze Tables
# ===========================================================================

@dlt.table(
    name="u1_circuits_raw",
    comment="Raw Utility 1 circuit segment data. Append-only. All columns as strings.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "_utility_id",
    },
)
def u1_circuits_raw():
    return _build_bronze_stream(
        f"{LANDING_PATH}/utility_1/circuits/",
        "utility_1",
    )


@dlt.table(
    name="u1_install_der_raw",
    comment="Raw Utility 1 installed DER data. Append-only.",
    table_properties={"quality": "bronze"},
)
def u1_install_der_raw():
    return _build_bronze_stream(
        f"{LANDING_PATH}/utility_1/installed_der/",
        "utility_1",
    )


@dlt.table(
    name="u1_planned_der_raw",
    comment="Raw Utility 1 planned DER data. Append-only.",
    table_properties={"quality": "bronze"},
)
def u1_planned_der_raw():
    return _build_bronze_stream(
        f"{LANDING_PATH}/utility_1/planned_der/",
        "utility_1",
    )


# ===========================================================================
# Utility 2 — Bronze Tables
# ===========================================================================

@dlt.table(
    name="u2_circuits_raw",
    comment="Raw Utility 2 circuit/feeder data. Append-only. All columns as strings.",
    table_properties={"quality": "bronze"},
)
def u2_circuits_raw():
    return _build_bronze_stream(
        f"{LANDING_PATH}/utility_2/circuits/",
        "utility_2",
    )


@dlt.table(
    name="u2_install_der_raw",
    comment="Raw Utility 2 installed DER data. Append-only.",
    table_properties={"quality": "bronze"},
)
def u2_install_der_raw():
    return _build_bronze_stream(
        f"{LANDING_PATH}/utility_2/installed_der/",
        "utility_2",
    )


@dlt.table(
    name="u2_planned_der_raw",
    comment="Raw Utility 2 planned DER data. Append-only.",
    table_properties={"quality": "bronze"},
)
def u2_planned_der_raw():
    return _build_bronze_stream(
        f"{LANDING_PATH}/utility_2/planned_der/",
        "utility_2",
    )
