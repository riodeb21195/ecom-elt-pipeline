"""
Unit-Tests fuer die Extract-Schicht: Schema-Drift-Erkennung und Checkpoint-Logik.
Laufen ohne laufende Docker-Services (kein API/MinIO-Zugriff noetig) -- pure Logik-Tests,
damit CI schnell und ohne Infrastruktur laeuft.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parents[2] / "extract"))

from schema_drift_check import SchemaDriftError, check_schema_drift  # noqa: E402


@pytest.fixture(autouse=True)
def isolate_schema_file(tmp_path, monkeypatch):
    import schema_drift_check
    monkeypatch.setattr(schema_drift_check, "SCHEMA_FILE", tmp_path / "schema.json")
    yield


def test_first_run_initializes_reference_schema_without_error():
    records = [{"order_id": "1", "review_score": 5}]
    result = check_schema_drift(records, source="orders")
    assert result["drift_detected"] is False


def test_additive_new_field_is_non_breaking_warning():
    check_schema_drift([{"order_id": "1", "review_score": 5}], source="orders")
    result = check_schema_drift(
        [{"order_id": "2", "review_score": 4, "new_field": "x"}], source="orders"
    )
    assert result["drift_detected"] is True
    assert result["details"]["is_breaking"] is False
    assert "new_field" in result["details"]["new_fields"]


def test_missing_field_raises_breaking_error():
    check_schema_drift([{"order_id": "1", "review_score": 5}], source="orders")
    with pytest.raises(SchemaDriftError):
        check_schema_drift([{"order_id": "2"}], source="orders")


def test_type_change_raises_breaking_error():
    check_schema_drift([{"order_id": "1", "review_score": 5}], source="orders")
    with pytest.raises(SchemaDriftError):
        check_schema_drift([{"order_id": "2", "review_score": "five"}], source="orders")


def test_empty_batch_is_not_drift():
    result = check_schema_drift([], source="orders")
    assert result["drift_detected"] is False
