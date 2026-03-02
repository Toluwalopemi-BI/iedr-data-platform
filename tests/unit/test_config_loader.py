"""Unit tests for shared.config_loader — validates YAML config access."""

import pytest
from shared.config_loader import (
    get_column_map,
    get_der_type_mapping,
    get_schema_contract,
    get_utility_config,
    load_yaml,
)


class TestLoadYaml:
    """Test YAML file loading."""

    def test_loads_utility_registry(self):
        data = load_yaml("utility_registry.yaml")
        assert "utilities" in data
        assert "utility_1" in data["utilities"]
        assert "utility_2" in data["utilities"]

    def test_loads_schema_contracts(self):
        data = load_yaml("schema_contracts.yaml")
        assert "gold" in data
        assert "platinum" in data

    def test_loads_der_type_mapping(self):
        data = load_yaml("der_type_mapping.yaml")
        assert "u1_column_to_canonical" in data
        assert "u2_type_to_canonical" in data


class TestGetUtilityConfig:
    """Test utility-specific config retrieval."""

    def test_returns_u1_config(self):
        config = get_utility_config("utility_1")
        assert config["name"] == "Utility 1"
        assert "circuits" in config
        assert "installed_der" in config

    def test_returns_u2_config(self):
        config = get_utility_config("utility_2")
        assert config["region"] == "Downstate NY"

    def test_raises_on_unknown_utility(self):
        with pytest.raises(KeyError, match="utility_99"):
            get_utility_config("utility_99")


class TestGetColumnMap:
    """Test column mapping retrieval."""

    def test_u1_circuits_map(self):
        col_map = get_column_map("utility_1", "circuits")
        assert col_map["feeder_id"] == "NYHCPV_csv_NFEEDER"
        assert col_map["voltage_kv"] == "NYHCPV_csv_NVOLTAGE"

    def test_u2_installed_der_map(self):
        col_map = get_column_map("utility_2", "installed_der")
        assert col_map["der_type"] == "DER_TYPE"


class TestGetDerTypeMapping:
    """Test DER type mapping retrieval."""

    def test_u1_mapping(self):
        mapping = get_der_type_mapping("utility_1")
        assert mapping["SolarPV"] == "Solar"
        assert mapping["Wind"] == "Wind"

    def test_u2_mapping(self):
        mapping = get_der_type_mapping("utility_2")
        assert mapping["Solar"] == "Solar"
        assert mapping["Natural Gas"] == "Natural Gas"


class TestGetSchemaContract:
    """Test schema contract retrieval."""

    def test_gold_dim_feeder(self):
        cols = get_schema_contract("gold", "dim_feeder")
        names = [c["name"] for c in cols]
        assert "global_feeder_id" in names
        assert "utility_id" in names

    def test_platinum_v_feeders_by_hc(self):
        cols = get_schema_contract("platinum", "v_feeders_by_hc")
        assert len(cols) > 0
