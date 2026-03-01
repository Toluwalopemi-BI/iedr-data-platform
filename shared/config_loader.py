"""IEDR Config Loader — Read YAML configuration files.

Provides functions to load utility registry, schema contracts,
and DER type mappings from the config/ directory.
"""

import os
from pathlib import Path

import yaml


def _get_config_dir() -> Path:
    """Resolve the config directory relative to the repo root."""
    # When running in Databricks, configs are in the workspace
    # When running locally (tests), configs are relative to repo root
    candidates = [
        Path(__file__).parent.parent / "config",
        Path("/Workspace") / "config",
        Path("config"),
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("Cannot find config/ directory")


def load_yaml(filename: str) -> dict:
    """Load a YAML config file by name.

    Args:
        filename: Name of the YAML file (e.g., 'utility_registry.yaml').

    Returns:
        Parsed YAML as a dictionary.
    """
    config_dir = _get_config_dir()
    filepath = config_dir / filename
    with open(filepath) as f:
        return yaml.safe_load(f)


def get_utility_config(utility_id: str) -> dict:
    """Get configuration for a specific utility.

    Args:
        utility_id: Utility identifier (e.g., 'utility_1').

    Returns:
        Dictionary with utility-specific column mappings and metadata.
    """
    registry = load_yaml("utility_registry.yaml")
    utilities = registry.get("utilities", {})
    if utility_id not in utilities:
        raise KeyError(f"Utility '{utility_id}' not found in registry. Available: {list(utilities.keys())}")
    return utilities[utility_id]


def get_column_map(utility_id: str, dataset_type: str) -> dict:
    """Get source-to-canonical column mapping for a utility dataset.

    Args:
        utility_id: e.g., 'utility_1'
        dataset_type: e.g., 'circuits', 'installed_der', 'planned_der'

    Returns:
        Dictionary mapping canonical column names to source column names.
    """
    config = get_utility_config(utility_id)
    dataset_config = config.get(dataset_type, {})
    return dataset_config.get("column_map", {})


def get_der_type_mapping(utility_id: str) -> dict:
    """Get DER type mapping for a specific utility.

    Args:
        utility_id: e.g., 'utility_1', 'utility_2'

    Returns:
        Dictionary mapping source type values to canonical names.
    """
    mappings = load_yaml("der_type_mapping.yaml")
    if utility_id == "utility_1":
        return mappings.get("u1_column_to_canonical", {})
    elif utility_id == "utility_2":
        return mappings.get("u2_type_to_canonical", {})
    else:
        raise KeyError(f"No DER type mapping for '{utility_id}'")


def get_schema_contract(layer: str, table_name: str) -> list[dict]:
    """Get expected schema contract for a table.

    Args:
        layer: 'gold' or 'platinum'
        table_name: e.g., 'dim_feeder', 'v_feeders_by_hc'

    Returns:
        List of column definitions [{name, type, nullable}, ...]
    """
    contracts = load_yaml("schema_contracts.yaml")
    layer_contracts = contracts.get(layer, {})
    table_contract = layer_contracts.get(table_name, {})
    return table_contract.get("columns", [])
