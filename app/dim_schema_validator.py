from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable


BASE_DIR = Path(__file__).resolve().parent.parent
DIM_SCHEMA_DIR = BASE_DIR / "config" / "dim_schemas"
ALLOWED_SCHEMA_KEYS = {"required", "properties", "additionalProperties"}
ALLOWED_PROPERTY_KEYS = {"type", "maxLength", "pattern", "enum", "minimum", "maximum", "dq"}
ALLOWED_TYPES = {"string", "number", "integer"}
ALLOWED_DQ_KEYS = {"not_null", "regex", "lookup_name", "dimension_lookup"}


class DimSchemaError(ValueError):
    pass


def _schema_path(table_name: str) -> Path:
    return DIM_SCHEMA_DIR / f"{table_name}.json"


def _clean(value: Any) -> str:
    return str(value).strip()


def list_dim_schema_files(schema_dir: Path = DIM_SCHEMA_DIR) -> list[Path]:
    return sorted(schema_dir.glob("*.json"))


def load_dim_schema(table_name: str) -> dict[str, Any]:
    path = _schema_path(table_name)
    if not path.exists():
        raise DimSchemaError(f"Schema file not found for {table_name}: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DimSchemaError(f"Invalid JSON schema for {table_name}: {exc}") from exc


def load_schema_file(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DimSchemaError(f"Invalid JSON in schema file {path.name}: {exc}") from exc


def _validate_dq_definition(schema_name: str, field_name: str, dq_rules: dict[str, Any]) -> None:
    unknown = sorted(set(dq_rules.keys()) - ALLOWED_DQ_KEYS)
    if unknown:
        raise DimSchemaError(
            f"{schema_name}: '{field_name}' dq has unsupported keys: {', '.join(unknown)}"
        )

    not_null = dq_rules.get("not_null")
    if not_null is not None and not isinstance(not_null, bool):
        raise DimSchemaError(f"{schema_name}: '{field_name}' dq.not_null must be true or false")

    regex = dq_rules.get("regex")
    if regex is not None:
        if not isinstance(regex, str):
            raise DimSchemaError(f"{schema_name}: '{field_name}' dq.regex must be a string")
        try:
            re.compile(regex)
        except re.error as exc:
            raise DimSchemaError(f"{schema_name}: '{field_name}' dq.regex invalid: {exc}") from exc

    lookup_name = dq_rules.get("lookup_name")
    if lookup_name is not None and (not isinstance(lookup_name, str) or not lookup_name.strip()):
        raise DimSchemaError(f"{schema_name}: '{field_name}' dq.lookup_name must be a non-empty string")

    dimension_lookup = dq_rules.get("dimension_lookup")
    if dimension_lookup is not None:
        if not isinstance(dimension_lookup, dict):
            raise DimSchemaError(f"{schema_name}: '{field_name}' dq.dimension_lookup must be an object")
        table = dimension_lookup.get("table")
        field = dimension_lookup.get("field")
        if not isinstance(table, str) or not table.strip():
            raise DimSchemaError(
                f"{schema_name}: '{field_name}' dq.dimension_lookup.table must be a non-empty string"
            )
        if not isinstance(field, str) or not field.strip():
            raise DimSchemaError(
                f"{schema_name}: '{field_name}' dq.dimension_lookup.field must be a non-empty string"
            )


def validate_schema_definition(schema_name: str, schema: dict[str, Any]) -> None:
    unknown_schema_keys = sorted(set(schema.keys()) - ALLOWED_SCHEMA_KEYS)
    if unknown_schema_keys:
        raise DimSchemaError(
            f"{schema_name}: unknown schema keys: {', '.join(unknown_schema_keys)}"
        )

    required = schema.get("required", [])
    properties = schema.get("properties", {})
    additional_properties = schema.get("additionalProperties", True)

    if not isinstance(required, list) or any(not isinstance(i, str) for i in required):
        raise DimSchemaError(f"{schema_name}: 'required' must be a list of strings")

    if not isinstance(properties, dict):
        raise DimSchemaError(f"{schema_name}: 'properties' must be an object")

    if not isinstance(additional_properties, bool):
        raise DimSchemaError(f"{schema_name}: 'additionalProperties' must be true or false")

    for field_name, constraints in properties.items():
        if not isinstance(field_name, str):
            raise DimSchemaError(f"{schema_name}: property names must be strings")
        if not isinstance(constraints, dict):
            raise DimSchemaError(f"{schema_name}: constraints for '{field_name}' must be an object")

        unknown_property_keys = sorted(set(constraints.keys()) - ALLOWED_PROPERTY_KEYS)
        if unknown_property_keys:
            raise DimSchemaError(
                f"{schema_name}: '{field_name}' has unsupported keys: {', '.join(unknown_property_keys)}"
            )

        expected_type = constraints.get("type")
        if expected_type is not None and expected_type not in ALLOWED_TYPES:
            raise DimSchemaError(
                f"{schema_name}: '{field_name}' type must be one of {', '.join(sorted(ALLOWED_TYPES))}"
            )

        max_len = constraints.get("maxLength")
        if max_len is not None and (not isinstance(max_len, int) or max_len < 0):
            raise DimSchemaError(f"{schema_name}: '{field_name}' maxLength must be a non-negative integer")

        pattern = constraints.get("pattern")
        if pattern is not None:
            if not isinstance(pattern, str):
                raise DimSchemaError(f"{schema_name}: '{field_name}' pattern must be a string")
            try:
                re.compile(pattern)
            except re.error as exc:
                raise DimSchemaError(f"{schema_name}: '{field_name}' pattern is invalid regex: {exc}") from exc

        enum = constraints.get("enum")
        if enum is not None and (not isinstance(enum, list) or len(enum) == 0):
            raise DimSchemaError(f"{schema_name}: '{field_name}' enum must be a non-empty list")

        minimum = constraints.get("minimum")
        maximum = constraints.get("maximum")
        if minimum is not None and not isinstance(minimum, (int, float)):
            raise DimSchemaError(f"{schema_name}: '{field_name}' minimum must be numeric")
        if maximum is not None and not isinstance(maximum, (int, float)):
            raise DimSchemaError(f"{schema_name}: '{field_name}' maximum must be numeric")
        if minimum is not None and maximum is not None and float(minimum) > float(maximum):
            raise DimSchemaError(f"{schema_name}: '{field_name}' minimum cannot be greater than maximum")

        if expected_type != "string" and (max_len is not None or pattern is not None):
            raise DimSchemaError(
                f"{schema_name}: '{field_name}' maxLength/pattern require type='string'"
            )

        if expected_type not in {"number", "integer"} and (minimum is not None or maximum is not None):
            raise DimSchemaError(
                f"{schema_name}: '{field_name}' minimum/maximum require type='number' or 'integer'"
            )

        dq_rules = constraints.get("dq")
        if dq_rules is not None:
            if not isinstance(dq_rules, dict):
                raise DimSchemaError(f"{schema_name}: '{field_name}' dq must be an object")
            _validate_dq_definition(schema_name, field_name, dq_rules)

    missing_properties = sorted(set(required) - set(properties.keys()))
    if missing_properties:
        raise DimSchemaError(
            f"{schema_name}: required fields missing from properties: {', '.join(missing_properties)}"
        )


def validate_all_dim_schemas(schema_dir: Path = DIM_SCHEMA_DIR) -> list[str]:
    errors: list[str] = []
    files = list_dim_schema_files(schema_dir)
    if not files:
        errors.append(f"No schema files found in {schema_dir}")
        return errors

    for path in files:
        try:
            schema = load_schema_file(path)
            if not isinstance(schema, dict):
                raise DimSchemaError(f"{path.name}: top-level JSON must be an object")
            validate_schema_definition(path.name, schema)
        except DimSchemaError as exc:
            errors.append(str(exc))
    return errors


def lookup_names_in_schema(schema: dict[str, Any]) -> set[str]:
    lookup_names: set[str] = set()
    properties = schema.get("properties", {})
    if not isinstance(properties, dict):
        return lookup_names

    for constraints in properties.values():
        if not isinstance(constraints, dict):
            continue
        dq = constraints.get("dq")
        if not isinstance(dq, dict):
            continue
        lookup_name = dq.get("lookup_name")
        if isinstance(lookup_name, str) and lookup_name.strip():
            lookup_names.add(lookup_name.strip())

    return lookup_names


def collect_all_schema_lookup_names(schema_dir: Path = DIM_SCHEMA_DIR) -> tuple[set[str], list[str]]:
    lookup_names: set[str] = set()
    errors: list[str] = []

    files = list_dim_schema_files(schema_dir)
    if not files:
        return lookup_names, [f"No schema files found in {schema_dir}"]

    for path in files:
        try:
            schema = load_schema_file(path)
            if not isinstance(schema, dict):
                raise DimSchemaError(f"{path.name}: top-level JSON must be an object")
            validate_schema_definition(path.name, schema)
            lookup_names.update(lookup_names_in_schema(schema))
        except DimSchemaError as exc:
            errors.append(str(exc))

    return lookup_names, errors


def _validate_type(value: Any, expected_type: str, field_name: str) -> None:
    if expected_type == "string":
        if not isinstance(value, str):
            raise DimSchemaError(f"'{field_name}' must be a string")
        return
    if expected_type == "number":
        try:
            float(_clean(value))
        except Exception as exc:
            raise DimSchemaError(f"'{field_name}' must be numeric") from exc
        return
    if expected_type == "integer":
        try:
            int(_clean(value))
        except Exception as exc:
            raise DimSchemaError(f"'{field_name}' must be an integer") from exc
        return

    raise DimSchemaError(f"Unsupported type '{expected_type}' in schema for '{field_name}'")


def validate_dim_attrs(
    table_name: str,
    attrs: dict[str, Any],
    lov_checker: Callable[[str, str], bool] | None = None,
    dimension_checker: Callable[[str, str, str], bool] | None = None,
) -> None:
    schema = load_dim_schema(table_name)
    validate_schema_definition(table_name, schema)

    required = schema.get("required", [])
    properties = schema.get("properties", {})
    additional_properties = bool(schema.get("additionalProperties", True))

    for field in required:
        value = attrs.get(field)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise DimSchemaError(f"Missing required attribute '{field}'")

    if not additional_properties:
        unknown = sorted([key for key in attrs.keys() if key not in properties])
        if unknown:
            raise DimSchemaError(f"Unknown attribute(s): {', '.join(unknown)}")

    for field_name, constraints in properties.items():
        value = attrs.get(field_name)
        has_value = value is not None and (not isinstance(value, str) or value.strip() != "")

        expected_type = constraints.get("type")
        if has_value and expected_type:
            _validate_type(value, expected_type, field_name)

        if has_value and isinstance(value, str):
            max_len = constraints.get("maxLength")
            if max_len is not None and len(value) > int(max_len):
                raise DimSchemaError(f"'{field_name}' exceeds maxLength={max_len}")

            pattern = constraints.get("pattern")
            if pattern and not re.fullmatch(str(pattern), value):
                raise DimSchemaError(f"'{field_name}' does not match pattern '{pattern}'")

        enum = constraints.get("enum")
        if has_value and enum and value not in enum:
            raise DimSchemaError(f"'{field_name}' must be one of: {', '.join(map(str, enum))}")

        if has_value and expected_type in {"number", "integer"}:
            numeric = float(_clean(value))
            minimum = constraints.get("minimum")
            maximum = constraints.get("maximum")
            if minimum is not None and numeric < float(minimum):
                raise DimSchemaError(f"'{field_name}' must be >= {minimum}")
            if maximum is not None and numeric > float(maximum):
                raise DimSchemaError(f"'{field_name}' must be <= {maximum}")

        dq_rules = constraints.get("dq", {})
        if dq_rules.get("not_null", False) and not has_value:
            raise DimSchemaError(f"DQ not_null failed for '{field_name}'")

        dq_regex = dq_rules.get("regex")
        if has_value and dq_regex and not re.fullmatch(str(dq_regex), str(value)):
            raise DimSchemaError(f"DQ regex failed for '{field_name}'")

        dq_lookup_name = dq_rules.get("lookup_name")
        if has_value and dq_lookup_name:
            if lov_checker is None:
                raise DimSchemaError(
                    f"LOV checker is required for lookup validation on '{field_name}'"
                )
            if not lov_checker(str(dq_lookup_name), str(value)):
                raise DimSchemaError(
                    f"DQ lookup failed for '{field_name}': value '{value}' is not valid for lookup '{dq_lookup_name}'"
                )

        dq_dimension_lookup = dq_rules.get("dimension_lookup")
        if has_value and dq_dimension_lookup:
            if dimension_checker is None:
                raise DimSchemaError(
                    f"Dimension checker is required for dimension lookup validation on '{field_name}'"
                )
            dim_table = str(dq_dimension_lookup.get("table"))
            dim_field = str(dq_dimension_lookup.get("field"))
            if not dimension_checker(dim_table, dim_field, str(value)):
                raise DimSchemaError(
                    f"DQ dimension lookup failed for '{field_name}': value '{value}' "
                    f"is not present in {dim_table}.{dim_field}"
                )
