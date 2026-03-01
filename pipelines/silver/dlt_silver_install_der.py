# Databricks notebook source
# ===========================================================================
# IEDR Silver Layer — Installed DER Cleansing
# ===========================================================================
# Purpose:  Cleanse installed DER records for each utility.
#           Critical schema difference handled here:
#           - U1: Wide-format (14 boolean technology columns)
#             → Unpivot to single der_type + nameplate_rating
#           - U2: Already has single DER_TYPE column
#
# Tables Created:
#   - silver.u1_install_der_clean
#   - silver.u2_install_der_clean
# ===========================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# U1 DER Technology columns (wide-format boolean flags)
# ---------------------------------------------------------------------------
U1_TECH_COLUMNS = [
    "SolarPV", "EnergyStorageSystem", "Wind", "MicroTurbine",
    "SynchronousGenerator", "InductionGenerator", "FarmWaste",
    "FuelCell", "CombinedHeatandPower", "GasTurbine",
    "Hydro", "InternalCombustionEngine", "SteamTurbine", "Other",
]

# Canonical mapping for U1 wide-format columns
U1_TYPE_MAP = {
    "SolarPV": "Solar",
    "EnergyStorageSystem": "Energy Storage",
    "Wind": "Wind",
    "MicroTurbine": "Micro Turbine",
    "SynchronousGenerator": "Synchronous Generator",
    "InductionGenerator": "Induction Generator",
    "FarmWaste": "Farm Waste",
    "FuelCell": "Fuel Cell",
    "CombinedHeatandPower": "Combined Heat & Power",
    "GasTurbine": "Gas Turbine",
    "Hydro": "Hydro",
    "InternalCombustionEngine": "Internal Combustion Engine",
    "SteamTurbine": "Steam Turbine",
    "Other": "Other",
}


def _normalize_nulls(df):
    """Replace common null-like strings with true SQL NULL."""
    null_values = ["NULL", "null", "N/A", "n/a", "NA", "None", "none", ""]
    for c in df.columns:
        df = df.withColumn(c, F.when(F.trim(F.col(c)).isin(null_values), None).otherwise(F.col(c)))
    return df


# ===========================================================================
# Utility 1 — Wide-Format DER → Derive Type from Boolean Columns
# ===========================================================================

@dlt.table(
    name="u1_install_der_clean",
    comment=(
        "Cleansed Utility 1 installed DER. Wide-format technology columns "
        "derived to single der_type. Typed and deduped."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_project_id", "project_id IS NOT NULL")
@dlt.expect("has_nameplate", "nameplate_rating_kw IS NOT NULL")
@dlt.expect("valid_der_type", "der_type IS NOT NULL")
def u1_install_der_clean():
    raw = dlt.read("u1_install_der_raw")
    raw = _normalize_nulls(raw)

    # Deduplicate by ProjectID
    w = Window.partitionBy("ProjectID").orderBy(F.desc("_ingestion_ts"))
    deduped = raw.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # Derive DER type from wide-format boolean columns
    # Logic: find the first technology column with a non-zero value
    type_expr = F.lit(None).cast("string")
    for col_name in reversed(U1_TECH_COLUMNS):
        canonical = U1_TYPE_MAP[col_name]
        type_expr = (
            F.when(
                F.col(col_name).cast("double") > 0,
                F.lit(canonical),
            )
            .otherwise(type_expr)
        )

    return (
        deduped
        .withColumn("project_id", F.trim(F.col("ProjectID")))
        .withColumn("project_type", F.trim(F.col("ProjectType")))
        .withColumn("nameplate_rating_kw", F.col("NamePlateRating").cast("double"))
        .withColumn("feeder_id", F.trim(F.col("ProjectCircuitID")))
        .withColumn("is_hybrid", F.col("Hybrid").cast("boolean"))
        .withColumn("der_type", type_expr)
        .withColumn("total_charges_cesir", F.col("TotalChargesCESIR").cast("double"))
        .withColumn("total_charges_construction", F.col("TotalChargesConstruction").cast("double"))
        .withColumn("cesir_est", F.col("CESIR_EST").cast("double"))
        .withColumn("system_upgrade_est", F.col("SystemUpgrade_EST").cast("double"))
        .withColumn("_utility_id", F.lit("utility_1"))
        .select(
            "project_id", "project_type", "der_type", "nameplate_rating_kw",
            "feeder_id", "is_hybrid",
            "total_charges_cesir", "total_charges_construction",
            "cesir_est", "system_upgrade_est",
            "_utility_id", "_source_file", "_ingestion_ts",
        )
    )


# ===========================================================================
# Utility 2 — Single DER_TYPE Column (Simpler)
# ===========================================================================

@dlt.table(
    name="u2_install_der_clean",
    comment="Cleansed Utility 2 installed DER. Single DER_TYPE column, typed and deduped.",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_der_id", "der_id IS NOT NULL")
@dlt.expect("has_nameplate", "nameplate_rating_kw IS NOT NULL")
@dlt.expect("valid_der_type", "der_type IS NOT NULL")
def u2_install_der_clean():
    raw = dlt.read("u2_install_der_raw")
    raw = _normalize_nulls(raw)

    w = Window.partitionBy("DER_ID").orderBy(F.desc("_ingestion_ts"))

    return (
        raw.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("der_id", F.trim(F.col("DER_ID")))
        .withColumn("der_type", F.trim(F.col("DER_TYPE")))
        .withColumn("nameplate_rating_kw", F.col("DER_NAMEPLATE_RATING").cast("double"))
        .withColumn("feeder_id", F.trim(F.col("DER_INTERCONNECTION_LOCATION")))
        .withColumn("service_address", F.trim(F.col("SERVICE_STREET_ADDRESS")))
        .withColumn("interconnection_cost", F.col("INTERCONNECTION_COST").cast("double"))
        .withColumn("is_hybrid", F.lit(False))
        .withColumn("_utility_id", F.lit("utility_2"))
        .select(
            "der_id", "der_type", "nameplate_rating_kw",
            "feeder_id", "service_address",
            "interconnection_cost", "is_hybrid",
            "_utility_id", "_source_file", "_ingestion_ts",
        )
    )
