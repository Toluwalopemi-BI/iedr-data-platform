"""Schema Contract Validator — CI job to verify schema_contracts.yaml is well-formed.

This script is called by the CI pipeline to ensure:
1. schema_contracts.yaml parses correctly
2. All referenced tables have column definitions
3. Column types are valid Spark SQL types
4. No duplicate column names within a table

Usage: python tests/validate_schemas.py
"""

import sys
from pathlib import Path

import yaml

VALID_TYPES = {"string", "long", "int", "double", "float", "boolean", "timestamp", "date", "binary"}


def validate():
    config_path = Path(__file__).parent.parent / "config" / "schema_contracts.yaml"
    if not config_path.exists():
        print(f"ERROR: {config_path} not found")
        sys.exit(1)

    with open(config_path) as f:
        contracts = yaml.safe_load(f)

    errors = []

    for layer, tables in contracts.items():
        if not isinstance(tables, dict):
            errors.append(f"Layer '{layer}' should be a dict of tables")
            continue

        for table_name, table_def in tables.items():
            columns = table_def.get("columns", [])
            if not columns:
                errors.append(f"{layer}.{table_name}: no columns defined")
                continue

            seen_names = set()
            for col in columns:
                name = col.get("name")
                col_type = col.get("type")

                if not name:
                    errors.append(f"{layer}.{table_name}: column missing 'name'")
                if not col_type:
                    errors.append(f"{layer}.{table_name}.{name}: missing 'type'")
                elif col_type not in VALID_TYPES:
                    errors.append(
                        f"{layer}.{table_name}.{name}: invalid type '{col_type}'. "
                        f"Valid: {VALID_TYPES}"
                    )
                if name in seen_names:
                    errors.append(f"{layer}.{table_name}: duplicate column '{name}'")
                seen_names.add(name)

    if errors:
        print(f"Schema validation FAILED ({len(errors)} errors):")
        for e in errors:
            print(f"  ✗ {e}")
        sys.exit(1)
    else:
        total_tables = sum(len(t) for t in contracts.values())
        print(f"Schema validation PASSED — {total_tables} tables validated across {len(contracts)} layers")


if __name__ == "__main__":
    validate()
