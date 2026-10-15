import json
from pathlib import Path
from typing import Any

SCHEMA_FILE = Path(__file__).parent / ".reference_schema.json"


class SchemaDriftError(Exception):
    pass


def _infer_schema(records: list[dict]) -> dict[str, str]:
    schema: dict[str, str] = {}
    for record in records:
        for key, value in record.items():
            schema[key] = type(value).__name__
    return schema


def check_schema_drift(records: list[dict], source: str) -> dict[str, Any]:
    if not records:
        return {"drift_detected": False, "details": None}

    current_schema = _infer_schema(records)

    if not SCHEMA_FILE.exists():
        SCHEMA_FILE.write_text(json.dumps({source: current_schema}, indent=2))
        return {"drift_detected": False, "details": "reference schema initialized"}

    all_schemas = json.loads(SCHEMA_FILE.read_text())
    reference_schema = all_schemas.get(source)

    if reference_schema is None:
        all_schemas[source] = current_schema
        SCHEMA_FILE.write_text(json.dumps(all_schemas, indent=2))
        return {"drift_detected": False, "details": "reference schema initialized"}

    missing_fields = set(reference_schema) - set(current_schema)
    new_fields = set(current_schema) - set(reference_schema)
    type_changes = {
        field: (reference_schema[field], current_schema[field])
        for field in set(reference_schema) & set(current_schema)
        if reference_schema[field] != current_schema[field]
    }

    is_breaking = bool(missing_fields or type_changes)
    drift_detected = bool(missing_fields or new_fields or type_changes)

    if drift_detected:
        details = {
            "missing_fields": sorted(missing_fields),
            "new_fields": sorted(new_fields),
            "type_changes": type_changes,
            "is_breaking": is_breaking,
        }
        if is_breaking:
            raise SchemaDriftError(
                f"Breaking schema drift in source '{source}': {details}"
            )
        return {"drift_detected": True, "details": details}

    return {"drift_detected": False, "details": None}

