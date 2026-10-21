#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
SCHEMAS = {
    "readiness-report-v1": ROOT / "schemas/evidence/readiness-report-v1.schema.json",
    "warmup-report-v1": ROOT / "schemas/evidence/warmup-report-v1.schema.json",
    "latency-attribution-v3": ROOT / "schemas/evidence/latency-attribution-v3.schema.json",
    "raw-transport-trace-v1": ROOT / "schemas/evidence/raw-transport-trace-v1.schema.json",
    "run-status-v1": ROOT / "schemas/evidence/run-status-v1.schema.json",
    "checkpoint-failure-classification-v1": ROOT / "schemas/evidence/checkpoint-failure-classification-v1.schema.json",
}


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def type_matches(value: Any, expected: str | list[str]) -> bool:
    if isinstance(expected, list):
        return any(type_matches(value, item) for item in expected)
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "null":
        return value is None
    return False


def validate_instance(value: Any, schema: dict[str, Any], location: str = "$") -> list[str]:
    errors: list[str] = []
    expected_type = schema.get("type")
    if expected_type is not None and not type_matches(value, expected_type):
        return [f"{location} must have type {expected_type}."]
    if "const" in schema and value != schema["const"]:
        errors.append(f"{location} must equal {schema['const']!r}.")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{location} is not an approved value.")
    if isinstance(value, str):
        if len(value) < int(schema.get("minLength", 0)):
            errors.append(f"{location} is too short.")
        if schema.get("pattern") and re.search(schema["pattern"], value) is None:
            errors.append(f"{location} does not match its required pattern.")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{location} is below its minimum.")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{location} is above its maximum.")
    if isinstance(value, list):
        if len(value) < int(schema.get("minItems", 0)):
            errors.append(f"{location} has too few items.")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, item in enumerate(value):
                errors.extend(validate_instance(item, item_schema, f"{location}[{index}]"))
    if isinstance(value, dict):
        for name in schema.get("required", []):
            if name not in value:
                errors.append(f"{location}.{name} is required.")
        properties = schema.get("properties", {})
        for name, child_schema in properties.items():
            if name in value:
                errors.extend(validate_instance(value[name], child_schema, f"{location}.{name}"))
        if schema.get("additionalProperties") is False:
            for name in sorted(set(value) - set(properties)):
                errors.append(f"{location}.{name} is not allowed.")
    return errors


def validate_named(value: Any, schema_name: str) -> list[str]:
    if schema_name not in SCHEMAS:
        return [f"Unknown schema: {schema_name}"]
    return validate_instance(value, read_json(SCHEMAS[schema_name]))


def write_validated(path: Path, value: Any, schema_name: str) -> None:
    errors = validate_named(value, schema_name)
    if errors:
        raise ValueError("\n".join(errors))
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", choices=sorted(SCHEMAS), required=True)
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    errors = validate_named(read_json(args.input), args.schema)
    if errors:
        for error in errors:
            print(f"Schema validation failure: {error}")
        return 1
    print(f"Schema validation passed: {args.schema} {args.input}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
