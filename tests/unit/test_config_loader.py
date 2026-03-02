"""Unit tests for shared.config_loader module."""

import pytest
from shared.config_loader import (
    load_yaml,
    get_utility_config,
    get_column_map,
    get_der_type_mapping,
    get_schema_contract,
)


class TestLoadYaml:
    """Tests for YAML loading."""

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
    """Tests for utility config retrieval."""

    def test_returns_u1_config(self):
        config = get_utility_config("utility_1")
        assert config["name"] == "Utility 1"
        assert "circuits" in config
        assert "installed_der" in config
        assert "planned_der" in config

    def test_returns_u2_config(self):
        config = get_utility_config("utility_2")
        assert config["name"] == "Utility 2"

    def test_raises_on_unknown_utility(self):
        with pytest.raises(KeyError, match="utility_99"):
            get_utility_config("utility_99")


class TestGetColumnMap:
    """Tests for column mapping retrieval."""

    def test_u1_circuits_column_map(self):
        col_map = get_column_map("utility_1", "circuits")
        assert col_map["feeder_id"] == "NYHCPV_csv_NFEEDER"
        assert col_map["voltage_kv"] == "NYHCPV_csv_NVOLTAGE"

    def test_u2_installed_der_column_map(self):
        col_map = get_column_map("utility_2", "installed_der")
        assert col_map["der_type"] == "DER_TYPE"
        assert col_map["nameplate_rating"] == "DER_NAMEPLATE_RATING"


class TestGetDerTypeMapping:
    """Tests for DER type mapping retrieval."""

    def test_u1_mapping_has_solar(self):
        mapping = get_der_type_mapping("utility_1")
        assert mapping["SolarPV"] == "Solar"

    def test_u2_mapping_has_solar(self):
        mapping = get_der_type_mapping("utility_2")
        assert mapping["Solar"] == "Solar"

    def test_raises_on_unknown_utility(self):
        with pytest.raises(KeyError):
            get_der_type_mapping("utility_99")


class TestGetSchemaContract:
    """Tests for schema contract retrieval."""

    def test_dim_feeder_contract(self):
        columns = get_schema_contract("gold", "dim_feeder")
        assert len(columns) > 0
        names = [c["name"] for c in columns]
        assert "global_feeder_id" in names
        assert "utility_id" in names

    def test_v_feeders_by_hc_contract(self):
        columns = get_schema_contract("platinum", "v_feeders_by_hc")
        assert len(columns) > 0
