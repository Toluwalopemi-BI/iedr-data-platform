# Databricks notebook source
# ===========================================================================
# IEDR Platinum Layer — API-Ready Materialized Views
# ===========================================================================
# Purpose:  Pre-materialized query shapes optimized for IEDR API endpoints.
#           Z-ORDERed on primary filter columns for Delta data-skipping.
#
# Tables Created:
#   - platinum.v_feeders_by_hc       (Req 2c: feeders filtered by HC)
#   - platinum.v_der_by_feeder       (Req 2d: DER records for a feeder)
#   - platinum.v_feeder_map_summary  (Map rendering: one row per feeder)
# ===========================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="v_feeders_by_hc",
    comment=(
        "API view: all feeders with hosting capacity data. "
        "Z-ORDERed on max_hosting_capacity_mw for efficient range scans. "
        "Supports: GET /api/feeders?min_hc=X&utility=Y"
    ),
    table_properties={
        "quality": "platinum",
        "pipelines.autoOptimize.zOrderCols": "max_hosting_capacity_mw",
    },
)
def v_feeders_by_hc():
    return (
        dlt.read("dim_feeder")
        .select(
            "global_feeder_id", "feeder_id", "utility_id", "feeder_name",
            "voltage_kv", "max_hosting_capacity_mw", "min_hosting_capacity_mw",
            "hca_refresh_date", "segment_count", "has_hosting_capacity",
        )
    )


@dlt.table(
    name="v_der_by_feeder",
    comment=(
        "API view: installed + planned DER for any feeder. "
        "Z-ORDERed on feeder_id for point-lookup performance. "
        "Supports: GET /api/feeders/{id}/der"
    ),
    table_properties={
        "quality": "platinum",
        "pipelines.autoOptimize.zOrderCols": "feeder_id",
    },
)
def v_der_by_feeder():
    # ── Installed DER ──
    inst = (
        dlt.read("fact_installed_der")
        .select(
            F.col("global_der_id").alias("der_id"),
            "feeder_id", "global_feeder_id", "utility_id",
            "der_type", "nameplate_rating_kw", "is_hybrid",
            "der_status",
        )
    )

    # ── Planned DER ──
    plan = (
        dlt.read("fact_planned_der")
        .select(
            F.col("global_der_id").alias("der_id"),
            "feeder_id", "global_feeder_id", "utility_id",
            "der_type", "nameplate_rating_kw", "is_hybrid",
            "der_status",
        )
    )

    return inst.unionByName(plan, allowMissingColumns=True)


@dlt.table(
    name="v_feeder_map_summary",
    comment=(
        "API view: one row per feeder with aggregated counts for map rendering. "
        "Includes installed_count, planned_count, total_capacity_kw, "
        "and hosting capacity for tooltip display."
    ),
    table_properties={
        "quality": "platinum",
        "pipelines.autoOptimize.zOrderCols": "utility_id,feeder_id",
    },
)
def v_feeder_map_summary():
    feeders = dlt.read("dim_feeder")
    installed = dlt.read("fact_installed_der")
    planned = dlt.read("fact_planned_der")

    # Aggregate installed DER per feeder
    inst_agg = (
        installed.filter(F.col("feeder_id").isNotNull())
        .groupBy("utility_id", "feeder_id")
        .agg(
            F.count("*").alias("installed_der_count"),
            F.sum("nameplate_rating_kw").alias("installed_capacity_kw"),
            F.countDistinct("der_type").alias("installed_type_count"),
        )
    )

    # Aggregate planned DER per feeder
    plan_agg = (
        planned.filter(F.col("feeder_id").isNotNull())
        .groupBy("utility_id", "feeder_id")
        .agg(
            F.count("*").alias("planned_der_count"),
            F.sum("nameplate_rating_kw").alias("planned_capacity_kw"),
        )
    )

    return (
        feeders
        .join(inst_agg, on=["utility_id", "feeder_id"], how="left")
        .join(plan_agg, on=["utility_id", "feeder_id"], how="left")
        .fillna(0, subset=[
            "installed_der_count", "installed_capacity_kw",
            "installed_type_count", "planned_der_count", "planned_capacity_kw",
        ])
        .withColumn(
            "total_der_count",
            F.col("installed_der_count") + F.col("planned_der_count"),
        )
        .select(
            "global_feeder_id", "feeder_id", "utility_id",
            "voltage_kv", "max_hosting_capacity_mw", "min_hosting_capacity_mw",
            "has_hosting_capacity",
            "installed_der_count", "installed_capacity_kw", "installed_type_count",
            "planned_der_count", "planned_capacity_kw", "total_der_count",
        )
    )
