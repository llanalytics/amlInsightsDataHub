#!/usr/bin/env python3
from __future__ import annotations

from app.dim_schema_validator import DIM_SCHEMA_DIR, collect_all_schema_lookup_names, validate_all_dim_schemas
from app.lov_loader import DEFAULT_LOV_VALUES_PATH, lookup_names_from_csv


def main() -> int:
    errors = validate_all_dim_schemas(DIM_SCHEMA_DIR)

    schema_lookup_names, lookup_errors = collect_all_schema_lookup_names(DIM_SCHEMA_DIR)
    errors.extend(lookup_errors)

    if not lookup_errors:
        try:
            lov_lookup_names = lookup_names_from_csv(DEFAULT_LOV_VALUES_PATH)
            missing_in_lov = sorted(schema_lookup_names - lov_lookup_names)
            if missing_in_lov:
                errors.append(
                    "Schema dq.lookup_name values missing from lov_values.csv: "
                    + ", ".join(missing_in_lov)
                )
        except Exception as exc:
            errors.append(f"Unable to validate schema lookup names against lov_values.csv: {exc}")

    if errors:
        print("Dimension schema check failed")
        for e in errors:
            print(f"  - {e}")
        return 1

    print(f"Dimension schema check passed ({DIM_SCHEMA_DIR})")
    print(f"LOV alignment check passed ({DEFAULT_LOV_VALUES_PATH})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
