#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SCHEMA_DIR = BASE_DIR / "config" / "dim_schemas"
DEFAULT_CSV_DIR = BASE_DIR / "data" / "sample"


def load_schema(schema_path: Path) -> dict:
    with schema_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        return {}
    return payload


def read_header(csv_path: Path) -> list[str]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return []
    return [h.strip() for h in header if h is not None]


def match_table_name(file_name: str, table_names: list[str]) -> str | None:
    lower = file_name.lower()
    for table in sorted(table_names, key=len, reverse=True):
        if lower.startswith(table.lower()):
            return table
    return None


def gather_csv_files(csv_files: list[str], csv_dir: str | None) -> list[Path]:
    paths: list[Path] = []
    for raw in csv_files:
        p = Path(raw)
        if not p.is_absolute():
            p = BASE_DIR / p
        paths.append(p)

    if csv_dir:
        d = Path(csv_dir)
        if not d.is_absolute():
            d = BASE_DIR / d
        paths.extend(sorted(d.glob("*.csv")))

    dedup: dict[str, Path] = {}
    for p in paths:
        dedup[str(p.resolve())] = p
    return list(dedup.values())


def check_headers(csv_path: Path, schema: dict, table_name: str) -> list[str]:
    errors: list[str] = []
    header = read_header(csv_path)
    if not header:
        return [f"{csv_path.name}: empty CSV or missing header row"]

    required = schema.get("required", [])
    properties = schema.get("properties", {})
    additional_properties = bool(schema.get("additionalProperties", True))

    required_set = {str(x) for x in required if isinstance(x, str)}
    property_set = {str(k) for k in properties.keys()} if isinstance(properties, dict) else set()
    header_set = set(header)

    missing_required = sorted(required_set - header_set)
    if missing_required:
        errors.append(
            f"{csv_path.name} ({table_name}): missing required header(s): {', '.join(missing_required)}"
        )

    if not additional_properties:
        unknown = sorted(header_set - property_set)
        if unknown:
            errors.append(
                f"{csv_path.name} ({table_name}): unknown header(s) not in schema properties: {', '.join(unknown)}"
            )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate CSV headers against dimension JSON schemas (required/properties).",
    )
    parser.add_argument(
        "--csv",
        action="append",
        default=[],
        help="Specific CSV file to validate (can be used multiple times).",
    )
    parser.add_argument(
        "--dir",
        default=str(DEFAULT_CSV_DIR),
        help="Directory of CSV files to validate (default: data/sample).",
    )
    parser.add_argument(
        "--schema-dir",
        default=str(DEFAULT_SCHEMA_DIR),
        help="Directory containing <table>.json schema files (default: config/dim_schemas).",
    )
    parser.add_argument(
        "--strict-unmapped",
        action="store_true",
        help="Fail if a CSV file does not map to a dimension schema by filename prefix.",
    )
    args = parser.parse_args()

    schema_dir = Path(args.schema_dir)
    if not schema_dir.is_absolute():
        schema_dir = BASE_DIR / schema_dir

    schema_paths = sorted(schema_dir.glob("*.json"))
    table_names = [p.stem for p in schema_paths]
    if not table_names:
        print(f"No schema files found in {schema_dir}")
        return 1

    schema_by_table: dict[str, dict] = {}
    for p in schema_paths:
        try:
            schema_by_table[p.stem] = load_schema(p)
        except Exception as exc:
            print(f"{p.name}: failed to parse schema JSON: {exc}")
            return 1

    csv_paths = gather_csv_files(args.csv, args.dir)
    if not csv_paths:
        print("No CSV files found to validate")
        return 1

    failures: list[str] = []
    checked = 0
    skipped = 0

    for csv_path in csv_paths:
        if not csv_path.exists() or not csv_path.is_file():
            failures.append(f"{csv_path}: file not found")
            continue

        table = match_table_name(csv_path.name, table_names)
        if table is None:
            skipped += 1
            message = f"{csv_path.name}: skipped (no matching dimension schema prefix)"
            if args.strict_unmapped:
                failures.append(message)
            else:
                print(message)
            continue

        checked += 1
        failures.extend(check_headers(csv_path, schema_by_table.get(table, {}), table))

    if failures:
        print("CSV header check failed")
        for failure in failures:
            print(f"  - {failure}")
        print(f"Checked: {checked}, Skipped: {skipped}")
        return 1

    print("CSV header check passed")
    print(f"Checked: {checked}, Skipped: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
