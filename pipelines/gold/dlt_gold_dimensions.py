# Databricks notebook source
# ===========================================================================
# IEDR Gold Layer — Reference Dimension Tables
# ===========================================================================
# Purpose:  Static/semi-static dimension tables for the star schema.
#
# Tables Created:
#   - gold.dim_utility      (utility metadata)
#   - gold.dim_der_type     (canonical DER type lookup)
# ===========================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="dim_utility",
    comment="Utility metadata dimension. One row per onboarded utility.",
    table_properties={"quality": "gold"},
)
def dim_utility():
    data = [
        ("utility_1", "Utility 1", "Upstate NY", "monthly", True),
        ("utility_2", "Utility 2", "Downstate NY", "monthly", True),
    ]
    schema = "utility_id STRING, utility_name STRING, region STRING, refresh_cadence STRING, is_active BOOLEAN"
    return spark.createDataFrame(data, schema).withColumn("_created_ts", F.current_timestamp())


@dlt.table(
    name="dim_der_type",
    comment="Canonical DER technology type lookup. Maps source-specific codes to standard names.",
    table_properties={"quality": "gold"},
)
def dim_der_type():
    data = [
        ("SOLAR", "Solar", "Solar Photovoltaic", "Renewable"),
        ("ESS", "Energy Storage", "Battery Energy Storage System", "Storage"),
        ("WIND", "Wind", "Wind Turbine", "Renewable"),
        ("HYDRO", "Hydro", "Hydroelectric", "Renewable"),
        ("CHP", "Combined Heat & Power", "Combined Heat and Power", "Conventional"),
        ("FUEL_CELL", "Fuel Cell", "Hydrogen/Natural Gas Fuel Cell", "Emerging"),
        ("GAS_TURBINE", "Gas Turbine", "Natural Gas Turbine", "Conventional"),
        ("MICRO_TURBINE", "Micro Turbine", "Micro Gas Turbine", "Conventional"),
        ("FARM_WASTE", "Farm Waste", "Agricultural Waste Digester", "Renewable"),
        ("ICE", "Internal Combustion Engine", "Diesel/Gas IC Engine", "Conventional"),
        ("STEAM", "Steam Turbine", "Steam Turbine Generator", "Conventional"),
        ("SYNC_GEN", "Synchronous Generator", "Synchronous Machine", "Conventional"),
        ("IND_GEN", "Induction Generator", "Induction Machine", "Conventional"),
        ("OTHER", "Other", "Other/Unclassified DER", "Other"),
        ("NATURAL_GAS", "Natural Gas", "Natural Gas Generator", "Conventional"),
        ("LANDFILL_GAS", "Landfill Gas", "Landfill Gas Recovery", "Renewable"),
        ("BIO_GAS", "Bio Gas", "Biogas Digester", "Renewable"),
        ("GEO_THERMAL", "Geo-Thermal", "Geothermal Generator", "Renewable"),
    ]
    schema = "type_code STRING, type_name STRING, description STRING, category STRING"
    return spark.createDataFrame(data, schema).withColumn("_created_ts", F.current_timestamp())
