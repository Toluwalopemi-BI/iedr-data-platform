# Databricks notebook source
# ===========================================================================
# IEDR Platinum Layer — Data Quality Report
# ===========================================================================
# Purpose:  Materialized view tracking data quality metrics per utility.
#           Powers the IEDR data quality dashboard and alerting.
#
# Tables Created:
#   - platinum.v_data_quality_report
#   - platinum.v_ingestion_metadata
# ===========================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="v_data_quality_report",
    comment=(
        "Per-utility data quality metrics. Tracks null rates, row counts, "
        "and feeder coverage. Refreshed each pipeline run. "
        "Alert threshold: >5% null feeder IDs on installed DER."
    ),
    table_properties={"quality": "platinum"},
)
def v_data_quality_report():
    # ── Installed DER null feeder rates ──
    inst = dlt.read("fact_installed_der")
    inst_dq = (
        inst.groupBy("utility_id")
        .agg(
            F.count("*").alias("installed_total_rows"),
            F.sum(F.when(F.col("_feeder_missing"), 1).otherwise(0)).alias("installed_null_feeder_count"),
            F.avg("nameplate_rating_kw").alias("installed_avg_rating_kw"),
            F.countDistinct("der_type").alias("installed_distinct_types"),
        )
        .withColumn(
            "installed_null_feeder_pct",
            F.round(F.col("installed_null_feeder_count") / F.col("installed_total_rows") * 100, 2),
        )
    )

    # ── Planned DER null feeder rates ──
    plan = dlt.read("fact_planned_der")
    plan_dq = (
        plan.groupBy("utility_id")
        .agg(
            F.count("*").alias("planned_total_rows"),
            F.sum(F.when(F.col("_feeder_missing"), 1).otherwise(0)).alias("planned_null_feeder_count"),
        )
        .withColumn(
            "planned_null_feeder_pct",
            F.round(F.col("planned_null_feeder_count") / F.col("planned_total_rows") * 100, 2),
        )
    )

    # ── Feeder dimension stats ──
    feeders = dlt.read("dim_feeder")
    feeder_dq = (
        feeders.groupBy("utility_id")
        .agg(
            F.count("*").alias("feeder_count"),
            F.sum(F.when(F.col("has_hosting_capacity"), 1).otherwise(0)).alias("feeders_with_hc"),
            F.avg("max_hosting_capacity_mw").alias("avg_max_hc_mw"),
        )
    )

    return (
        inst_dq
        .join(plan_dq, on="utility_id", how="full_outer")
        .join(feeder_dq, on="utility_id", how="full_outer")
        .withColumn("_report_ts", F.current_timestamp())
    )


@dlt.table(
    name="v_ingestion_metadata",
    comment="Freshness tracking per utility per dataset. Alert if >35 days stale.",
    table_properties={"quality": "platinum"},
)
def v_ingestion_metadata():
    datasets = [
        ("u1_circuits_raw", "utility_1", "circuits"),
        ("u1_install_der_raw", "utility_1", "installed_der"),
        ("u1_planned_der_raw", "utility_1", "planned_der"),
        ("u2_circuits_raw", "utility_2", "circuits"),
        ("u2_install_der_raw", "utility_2", "installed_der"),
        ("u2_planned_der_raw", "utility_2", "planned_der"),
    ]

    frames = []
    for table_name, utility_id, dataset_type in datasets:
        df = (
            dlt.read(table_name)
            .agg(
                F.max("_ingestion_ts").alias("last_ingestion_ts"),
                F.count("*").alias("total_rows"),
                F.countDistinct("_source_file").alias("source_file_count"),
            )
            .withColumn("utility_id", F.lit(utility_id))
            .withColumn("dataset_type", F.lit(dataset_type))
            .withColumn("source_table", F.lit(table_name))
            .withColumn(
                "days_since_last_load",
                F.datediff(F.current_timestamp(), F.col("last_ingestion_ts")),
            )
            .withColumn(
                "is_stale",
                F.col("days_since_last_load") > 35,
            )
        )
        frames.append(df)

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f, allowMissingColumns=True)

    return result.withColumn("_report_ts", F.current_timestamp())
