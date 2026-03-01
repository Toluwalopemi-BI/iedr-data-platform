# Databricks notebook source
# ===========================================================================
# IEDR Gold Layer — Unified DER Fact Tables
# ===========================================================================
# Purpose:  Create unified installed and planned DER fact tables by
#           unioning all utilities into a canonical schema.
#
# Tables Created:
#   - gold.fact_installed_der
#   - gold.fact_planned_der
# ===========================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="fact_installed_der",
    comment=(
        "Unified installed DER across all utilities. "
        "Canonical columns: der_id, feeder_id, utility_id, der_type, nameplate_rating_kw. "
        "~39K rows expected (13.7K U1 + 25.5K U2)."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
@dlt.expect("valid_der_id", "der_id IS NOT NULL")
@dlt.expect("valid_der_type", "der_type IS NOT NULL")
def fact_installed_der():
    # ── Utility 1 ──
    u1 = dlt.read("u1_install_der_clean")
    u1_canonical = (
        u1.select(
            F.col("project_id").alias("der_id"),
            F.col("feeder_id"),
            F.col("_utility_id").alias("utility_id"),
            F.col("der_type"),
            F.col("nameplate_rating_kw"),
            F.col("is_hybrid"),
            F.col("project_type").alias("source_type_code"),
            F.lit(None).cast("string").alias("service_address"),
            F.lit(None).cast("double").alias("interconnection_cost"),
        )
    )

    # ── Utility 2 ──
    u2 = dlt.read("u2_install_der_clean")
    u2_canonical = (
        u2.select(
            F.col("der_id"),
            F.col("feeder_id"),
            F.col("_utility_id").alias("utility_id"),
            F.col("der_type"),
            F.col("nameplate_rating_kw"),
            F.col("is_hybrid"),
            F.lit(None).cast("string").alias("source_type_code"),
            F.col("service_address"),
            F.col("interconnection_cost"),
        )
    )

    unified = u1_canonical.unionByName(u2_canonical, allowMissingColumns=True)

    return (
        unified
        .withColumn(
            "global_der_id",
            F.concat_ws("::", F.col("utility_id"), F.col("der_id")),
        )
        .withColumn(
            "global_feeder_id",
            F.when(F.col("feeder_id").isNotNull(),
                   F.concat_ws("::", F.col("utility_id"), F.col("feeder_id")))
            .otherwise(None),
        )
        .withColumn("der_status", F.lit("installed"))
        .withColumn("_feeder_missing", F.col("feeder_id").isNull())
        .withColumn("_gold_created_ts", F.current_timestamp())
    )


@dlt.table(
    name="fact_planned_der",
    comment=(
        "Unified planned DER across all utilities. "
        "Includes _feeder_missing flag for records without feeder assignment. "
        "~32.6K rows expected (1.7K U1 + 31K U2)."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
@dlt.expect("valid_der_type", "der_type IS NOT NULL")
def fact_planned_der():
    # ── Utility 1 ──
    u1 = dlt.read("u1_planned_der_clean")
    u1_canonical = (
        u1.select(
            F.col("project_id").alias("der_id"),
            F.col("feeder_id"),
            F.col("_utility_id").alias("utility_id"),
            F.col("der_type"),
            F.col("nameplate_rating_kw"),
            F.col("is_hybrid"),
            F.col("project_status").alias("queue_status"),
            F.col("in_service_date").alias("planned_date"),
            F.col("_feeder_missing"),
        )
    )

    # ── Utility 2 ──
    u2 = dlt.read("u2_planned_der_clean")
    u2_canonical = (
        u2.select(
            F.col("request_id").alias("der_id"),
            F.col("feeder_id"),
            F.col("_utility_id").alias("utility_id"),
            F.col("der_type"),
            F.col("nameplate_rating_kw"),
            F.col("is_hybrid"),
            F.col("der_status").alias("queue_status"),
            F.col("planned_install_date").alias("planned_date"),
            F.col("_feeder_missing"),
        )
    )

    unified = u1_canonical.unionByName(u2_canonical, allowMissingColumns=True)

    return (
        unified
        .withColumn(
            "global_der_id",
            F.concat_ws("::", F.col("utility_id"), F.col("der_id")),
        )
        .withColumn(
            "global_feeder_id",
            F.when(F.col("feeder_id").isNotNull(),
                   F.concat_ws("::", F.col("utility_id"), F.col("feeder_id")))
            .otherwise(None),
        )
        .withColumn("der_status", F.lit("planned"))
        .withColumn("_gold_created_ts", F.current_timestamp())
    )
