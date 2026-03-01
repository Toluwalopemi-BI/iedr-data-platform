# Databricks notebook source
# ===========================================================================
# IEDR Gold Layer — Unified Feeder Dimension
# ===========================================================================
# Purpose:  Create a single feeder dimension table by unioning all utilities.
#           Critical aggregation: U1 segments → feeder level using first()
#           on pre-computed FMAXHC/FMINHC columns. U2 already feeder-level.
#
# Tables Created:
#   - gold.dim_feeder
#
# Design Decision:
#   We use first() on U1's feeder-level HC columns (not SUM/MAX on segments)
#   because HC is pre-computed by the utility's power-flow study and
#   repeated identically on every segment row within a feeder.
# ===========================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="dim_feeder",
    comment=(
        "Unified feeder dimension across all utilities. Feeder-level granularity. "
        "U1 segments aggregated to feeders. Composite key: (utility_id, feeder_id). "
        "2,175 rows expected (266 U1 + 1,909 U2)."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
@dlt.expect("valid_feeder_id", "feeder_id IS NOT NULL")
@dlt.expect("valid_utility_id", "utility_id IS NOT NULL")
def dim_feeder():
    # ── Utility 1: Aggregate ~64K segments → 266 feeders ──
    u1 = dlt.read("u1_circuits_clean")
    u1_feeders = (
        u1.groupBy("feeder_id")
        .agg(
            F.first("voltage_kv").alias("voltage_kv"),
            F.first("feeder_max_hc_mw").alias("max_hosting_capacity_mw"),
            F.first("feeder_min_hc_mw").alias("min_hosting_capacity_mw"),
            F.first("hca_date").alias("hca_refresh_date"),
            F.count("section_id").alias("segment_count"),
            F.first("feeder_name").alias("feeder_name"),
        )
        .withColumn("utility_id", F.lit("utility_1"))
        .withColumn("feeder_id", F.col("feeder_id").cast("string"))
        .withColumn("map_color", F.lit(None).cast("string"))
        .withColumn("dg_since_refresh_mw", F.lit(None).cast("double"))
    )

    # ── Utility 2: Already feeder-level (1,909 rows) ──
    u2 = dlt.read("u2_circuits_clean")
    u2_feeders = (
        u2.select(
            F.col("feeder_id"),
            F.col("voltage_kv"),
            F.col("feeder_max_hc_mw").alias("max_hosting_capacity_mw"),
            F.col("feeder_min_hc_mw").alias("min_hosting_capacity_mw"),
            F.col("hca_date").alias("hca_refresh_date"),
            F.lit(1).alias("segment_count"),
            F.lit(None).cast("string").alias("feeder_name"),
            F.col("map_color"),
            F.col("dg_since_refresh_mw"),
        )
        .withColumn("utility_id", F.lit("utility_2"))
    )

    # ── Union with canonical schema ──
    unified = u1_feeders.unionByName(u2_feeders, allowMissingColumns=True)

    return (
        unified
        .withColumn(
            "global_feeder_id",
            F.concat_ws("::", F.col("utility_id"), F.col("feeder_id")),
        )
        .withColumn("has_hosting_capacity", F.col("max_hosting_capacity_mw") > 0)
        .withColumn("_gold_created_ts", F.current_timestamp())
        .select(
            "global_feeder_id", "feeder_id", "utility_id", "feeder_name",
            "voltage_kv", "max_hosting_capacity_mw", "min_hosting_capacity_mw",
            "hca_refresh_date", "segment_count", "map_color",
            "dg_since_refresh_mw", "has_hosting_capacity", "_gold_created_ts",
        )
    )
