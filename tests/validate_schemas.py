"""Schema Contract Validation Script.

Run during CI to verify that schema_contracts.yaml is well-formed
and all referenced tables have valid column definitions.

Usage:
    python tests/validate_schemas.py
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.config_loader import load_yaml


VALID_TYPES = {"string", "double", "long", "int", "boolean", "timestamp", "date", "float"}


def validate_contracts():
    """Validate schema_contracts.yaml structure and types."""
    contracts = load_yaml("schema_contracts.yaml")
    errors = []

    for layer, tables in contracts.items():
        if layer not in ("gold", "platinum"):
            errors.append(f"Unknown layer: {layer}")
            continue

        if not isinstance(tables, dict):
            errors.append(f"Layer '{layer}' should be a dict, got {type(tables)}")
            continue

        for table_name, table_def in tables.items():
            columns = table_def.get("columns", [])
            if not columns:
                errors.append(f"{layer}.{table_name}: no columns defined")
                continue

            col_names = set()
            for col in columns:
                # Check required fields
                for field in ("name", "type", "nullable"):
                    if field not in col:
                        errors.append(f"{layer}.{table_name}: column missing '{field}': {col}")

                # Check valid type
                col_type = col.get("type", "")
                if col_type not in VALID_TYPES:
                    errors.append(
                        f"{layer}.{table_name}.{col.get('name')}: "
                        f"invalid type '{col_type}'. Valid: {VALID_TYPES}"
                    )

                # Check duplicate column names
                col_name = col.get("name", "")
                if col_name in col_names:
                    errors.append(f"{layer}.{table_name}: duplicate column '{col_name}'")
                col_names.add(col_name)

    return errors


def main():
    print("Validating schema contracts...")
    errors = validate_contracts()

    if errors:
        print(f"\n❌ {len(errors)} validation error(s):")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        contracts = load_yaml("schema_contracts.yaml")
        total_tables = sum(len(tables) for tables in contracts.values())
        total_cols = sum(
            len(t.get("columns", []))
            for tables in contracts.values()
            for t in tables.values()
        )
        print(f"✅ All contracts valid: {total_tables} tables, {total_cols} columns")
        sys.exit(0)


if __name__ == "__main__":
    main()
