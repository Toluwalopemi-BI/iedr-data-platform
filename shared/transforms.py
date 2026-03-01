"""IEDR Shared Transforms — Reusable PySpark transformation functions.

These functions are used across Silver/Gold layers and are
independently testable via pytest + chispa (no Databricks required).
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def normalize_nulls(df: DataFrame) -> DataFrame:
    """Replace common null-like string values with true SQL NULL.

    Handles: "NULL", "null", "N/A", "n/a", "NA", "None", "none", ""

    Args:
        df: Input DataFrame with string columns.

    Returns:
        DataFrame with null-like strings replaced by None.
    """
    null_values = ["NULL", "null", "N/A", "n/a", "NA", "na", "None", "none", ""]
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)).isin(null_values), None).otherwise(F.col(col_name)),
        )
    return df


def deduplicate_by_key(df: DataFrame, key_column: str, order_column: str = "_ingestion_ts") -> DataFrame:
    """Keep only the latest record per natural key.

    Args:
        df: Input DataFrame.
        key_column: Column to partition by (natural key).
        order_column: Column to order by descending (latest first).

    Returns:
        Deduplicated DataFrame.
    """
    w = Window.partitionBy(key_column).orderBy(F.desc(order_column))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def derive_u1_der_type(df: DataFrame, tech_columns: list[str], type_map: dict) -> DataFrame:
    """Derive DER type from U1 wide-format boolean technology columns.

    Scans columns in order and returns the first with a non-zero value.

    Args:
        df: DataFrame with technology columns as strings.
        tech_columns: List of technology column names.
        type_map: Dict mapping column name to canonical type name.

    Returns:
        DataFrame with added 'der_type' column.
    """
    type_expr = F.lit(None).cast("string")
    for col_name in reversed(tech_columns):
        canonical = type_map.get(col_name, col_name)
        type_expr = F.when(F.col(col_name).cast("double") > 0, F.lit(canonical)).otherwise(type_expr)
    return df.withColumn("der_type", type_expr)


def add_global_id(df: DataFrame, utility_col: str, local_id_col: str, output_col: str) -> DataFrame:
    """Create a globally unique ID by concatenating utility_id and local ID.

    Format: "utility_1::12345"

    Args:
        df: Input DataFrame.
        utility_col: Column with utility identifier.
        local_id_col: Column with utility-local identifier.
        output_col: Name for the new global ID column.

    Returns:
        DataFrame with added global ID column.
    """
    return df.withColumn(
        output_col,
        F.when(
            F.col(local_id_col).isNotNull(),
            F.concat_ws("::", F.col(utility_col), F.col(local_id_col)),
        ).otherwise(None),
    )
