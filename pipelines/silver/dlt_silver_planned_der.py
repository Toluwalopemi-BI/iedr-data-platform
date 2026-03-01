# Databricks notebook source
# ===========================================================================
# IEDR Silver Layer — Planned DER Cleansing
# ===========================================================================
# Purpose:  Cleanse planned DER records. Key data quality challenge:
#           - U1: 25.2% of records have NULL feeder assignment
#           - U2: 1.9% NULL, but 75%+ null in status columns
#
# Tables Created:
#   - silver.u1_planned_der_clean
#   - silver.u2_planned_der_clean
# ===========================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


U1_TECH_COLUMNS = [
    "SolarPV", "EnergyStorageSystem", "Wind", "MicroTurbine",
    "SynchronousGenerator", "InductionGenerator", "FarmWaste",
    "FuelCell", "CombinedHeatandPower", "GasTurbine",
    "Hydro", "InternalCombustionEngine", "SteamTurbine", "Other",
]

U1_TYPE_MAP = {
    "SolarPV": "Solar", "EnergyStorageSystem": "Energy Storage",
    "Wind": "Wind", "MicroTurbine": "Micro Turbine",
    "SynchronousGenerator": "Synchronous Generator",
    "InductionGenerator": "Induction Generator",
    "FarmWaste": "Farm Waste", "FuelCell": "Fuel Cell",
    "CombinedHeatandPower": "Combined Heat & Power",
    "GasTurbine": "Gas Turbine", "Hydro": "Hydro",
    "InternalCombustionEngine": "Internal Combustion Engine",
    "SteamTurbine": "Steam Turbine", "Other": "Other",
}


def _normalize_nulls(df):
    """Replace common null-like strings with true SQL NULL."""
    null_values = ["NULL", "null", "N/A", "n/a", "NA", "None", "none", ""]
    for c in df.columns:
        df = df.withColumn(c, F.when(F.trim(F.col(c)).isin(null_values), None).otherwise(F.col(c)))
    return df


# ===========================================================================
# Utility 1 — Planned DER (25.2% missing feeder IDs)
# ===========================================================================

@dlt.table(
    name="u1_planned_der_clean",
    comment=(
        "Cleansed Utility 1 planned DER. WARNING: 25.2% of records have "
        "NULL ProjectCircuitID (feeder not yet assigned). These records are "
        "kept but flagged with _feeder_missing=True."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_project_id", "project_id IS NOT NULL")
@dlt.expect("valid_der_type", "der_type IS NOT NULL")
@dlt.expect("has_feeder_assignment", "feeder_id IS NOT NULL")  # soft warn, not drop
def u1_planned_der_clean():
    raw = dlt.read("u1_planned_der_raw")
    raw = _normalize_nulls(raw)

    w = Window.partitionBy("ProjectID").orderBy(F.desc("_ingestion_ts"))
    deduped = raw.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # Derive DER type from wide-format columns
    type_expr = F.lit(None).cast("string")
    for col_name in reversed(U1_TECH_COLUMNS):
        canonical = U1_TYPE_MAP[col_name]
        type_expr = F.when(F.col(col_name).cast("double") > 0, F.lit(canonical)).otherwise(type_expr)

    return (
        deduped
        .withColumn("project_id", F.trim(F.col("ProjectID")))
        .withColumn("project_type", F.trim(F.col("ProjectType")))
        .withColumn("project_status", F.trim(F.col("ProjectStatus")))
        .withColumn("nameplate_rating_kw", F.col("NamePlateRating").cast("double"))
        .withColumn("feeder_id", F.trim(F.col("ProjectCircuitID")))
        .withColumn("is_hybrid", F.col("Hybrid").cast("boolean"))
        .withColumn("der_type", type_expr)
        .withColumn("in_service_date", F.to_timestamp(F.col("InServiceDate")))
        .withColumn("completion_date", F.to_timestamp(F.col("CompletionDate")))
        .withColumn("_feeder_missing", F.col("feeder_id").isNull())
        .withColumn("_utility_id", F.lit("utility_1"))
        .select(
            "project_id", "project_type", "project_status", "der_type",
            "nameplate_rating_kw", "feeder_id", "is_hybrid",
            "in_service_date", "completion_date",
            "_feeder_missing", "_utility_id", "_source_file", "_ingestion_ts",
        )
    )


# ===========================================================================
# Utility 2 — Planned DER (1.9% missing feeder IDs)
# ===========================================================================

@dlt.table(
    name="u2_planned_der_clean",
    comment="Cleansed Utility 2 planned DER. 1.9% null feeder IDs flagged.",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_der_type", "der_type IS NOT NULL")
@dlt.expect("has_nameplate", "nameplate_rating_kw IS NOT NULL")
@dlt.expect("has_feeder_assignment", "feeder_id IS NOT NULL")
def u2_planned_der_clean():
    raw = dlt.read("u2_planned_der_raw")
    raw = _normalize_nulls(raw)

    w = Window.partitionBy("INTERCONNECTION_QUEUE_REQUEST_ID").orderBy(F.desc("_ingestion_ts"))

    return (
        raw.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("request_id", F.trim(F.col("INTERCONNECTION_QUEUE_REQUEST_ID")))
        .withColumn("queue_position", F.col("INTERCONNECTION_QUEUE_POSITION").cast("int"))
        .withColumn("der_type", F.trim(F.col("DER_TYPE")))
        .withColumn("nameplate_rating_kw", F.col("DER_NAMEPLATE_RATING").cast("double"))
        .withColumn("inverter_rating_kw", F.col("INVERTER_NAMEPLATE_RATING").cast("double"))
        .withColumn("feeder_id", F.trim(F.col("DER_INTERCONNECTION_LOCATION")))
        .withColumn("planned_install_date", F.to_timestamp(F.col("PLANNED_INSTALLATION_DATE")))
        .withColumn("der_status", F.trim(F.col("DER_STATUS")))
        .withColumn("status_rationale", F.trim(F.col("DER_STATUS_RATIONALE")))
        .withColumn("substation_total_mw", F.col("TOTAL_MW_FOR_SUBSTATION").cast("double"))
        .withColumn("is_hybrid", F.lit(False))
        .withColumn("_feeder_missing", F.col("feeder_id").isNull())
        .withColumn("_utility_id", F.lit("utility_2"))
        .select(
            "request_id", "queue_position", "der_type", "nameplate_rating_kw",
            "inverter_rating_kw", "feeder_id", "planned_install_date",
            "der_status", "status_rationale", "substation_total_mw", "is_hybrid",
            "_feeder_missing", "_utility_id", "_source_file", "_ingestion_ts",
        )
    )
