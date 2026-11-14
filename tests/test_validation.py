"""Unit tests for validation rules in SparkSkewLab."""

from typing import Set
import pytest

from src.config import (
    HOT_CUSTOMER_ID,
    REASON_DUPLICATE_CUSTOMER_ID,
    REASON_DUPLICATE_EVENT_ID,
    REASON_INVALID_CUSTOMER_REF,
    REASON_INVALID_CUSTOMER_SEGMENT,
    REASON_INVALID_EVENT_TYPE,
    REASON_INVALID_TIMESTAMP,
    REASON_MISSING_CITY,
    REASON_MISSING_CUSTOMER_ID,
    REASON_MISSING_CUSTOMER_NAME,
    REASON_MISSING_EVENT_ID,
    REASON_NON_NUMERIC_AMOUNT,
    REASON_NON_POSITIVE_AMOUNT,
)
from src.validation import (
    is_valid_iso8601_timestamp,
    validate_customer,
    validate_order_event,
)


def test_valid_customer():
    """Verify standard valid customer passes validation."""
    record = {
        "customer_id": "CUST_000100",
        "customer_name": "Alice Johnson",
        "city": "Chicago",
        "customer_segment": "ENTERPRISE",
    }
    seen_ids: Set[str] = set()
    res = validate_customer(record, seen_ids)
    assert res.is_valid
    assert len(res.rejection_reasons) == 0
    assert "CUST_000100" in seen_ids


def test_customer_missing_fields():
    """Verify validation catches null or empty fields in customer record."""
    seen_ids: Set[str] = set()
    res = validate_customer({"customer_id": "", "customer_name": "Bob", "city": "NYC", "customer_segment": "VIP"}, seen_ids)
    assert not res.is_valid
    assert REASON_MISSING_CUSTOMER_ID in res.rejection_reasons

    res = validate_customer({"customer_id": "CUST_0001", "customer_name": "", "city": "NYC", "customer_segment": "VIP"}, seen_ids)
    assert not res.is_valid
    assert REASON_MISSING_CUSTOMER_NAME in res.rejection_reasons

    res = validate_customer({"customer_id": "CUST_0002", "customer_name": "Bob", "city": "", "customer_segment": "VIP"}, seen_ids)
    assert not res.is_valid
    assert REASON_MISSING_CITY in res.rejection_reasons

    res = validate_customer({"customer_id": "CUST_0003", "customer_name": "Bob", "city": "NYC", "customer_segment": "UNKNOWN_SEGMENT"}, seen_ids)
    assert not res.is_valid
    assert REASON_INVALID_CUSTOMER_SEGMENT in res.rejection_reasons


def test_duplicate_customer_id():
    """Verify duplicate customer ID is flagged."""
    seen_ids: Set[str] = set()
    rec1 = {"customer_id": "CUST_000001", "customer_name": "User One", "city": "Denver", "customer_segment": "RETAIL"}
    rec2 = {"customer_id": "CUST_000001", "customer_name": "User Two", "city": "Austin", "customer_segment": "VIP"}

    res1 = validate_customer(rec1, seen_ids)
    assert res1.is_valid

    res2 = validate_customer(rec2, seen_ids)
    assert not res2.is_valid
    assert REASON_DUPLICATE_CUSTOMER_ID in res2.rejection_reasons


def test_valid_order_event():
    """Verify standard valid order event passes validation."""
    valid_customers = {"CUST_000100", HOT_CUSTOMER_ID}
    seen_events: Set[str] = set()

    record = {
        "event_id": "EVT_00000001",
        "customer_id": "CUST_000100",
        "order_amount": "149.99",
        "event_type": "PURCHASE",
        "event_timestamp": "2026-11-01T12:00:00+00:00",
        "source_file": "order_events.csv",
    }
    res = validate_order_event(record, valid_customers, seen_events)
    assert res.is_valid
    assert len(res.rejection_reasons) == 0
    assert "EVT_00000001" in seen_events


def test_order_event_duplicate_id_policy():
    """Verify first-occurrence-wins policy for duplicate event IDs."""
    valid_customers = {"CUST_000100"}
    seen_events: Set[str] = set()

    rec1 = {
        "event_id": "EVT_DUP_001",
        "customer_id": "CUST_000100",
        "order_amount": "100.00",
        "event_type": "PURCHASE",
        "event_timestamp": "2026-11-01T10:00:00+00:00",
    }
    rec2 = {
        "event_id": "EVT_DUP_001",
        "customer_id": "CUST_000100",
        "order_amount": "200.00",
        "event_type": "PAYMENT",
        "event_timestamp": "2026-11-01T10:05:00+00:00",
    }

    res1 = validate_order_event(rec1, valid_customers, seen_events)
    assert res1.is_valid

    res2 = validate_order_event(rec2, valid_customers, seen_events)
    assert not res2.is_valid
    assert REASON_DUPLICATE_EVENT_ID in res2.rejection_reasons


def test_order_event_invalid_customer_reference():
    """Verify non-existent customer reference triggers referential integrity failure."""
    valid_customers = {"CUST_000100"}
    seen_events: Set[str] = set()

    rec = {
        "event_id": "EVT_00000002",
        "customer_id": "CUST_NONEXISTENT_9999",
        "order_amount": "50.00",
        "event_type": "PURCHASE",
        "event_timestamp": "2026-11-01T10:00:00+00:00",
    }
    res = validate_order_event(rec, valid_customers, seen_events)
    assert not res.is_valid
    assert REASON_INVALID_CUSTOMER_REF in res.rejection_reasons


def test_order_event_negative_and_non_numeric_amount():
    """Verify order amount rules."""
    valid_customers = {"CUST_000100"}
    seen_events: Set[str] = set()

    rec_neg = {
        "event_id": "EVT_00000003",
        "customer_id": "CUST_000100",
        "order_amount": "-50.00",
        "event_type": "PURCHASE",
        "event_timestamp": "2026-11-01T10:00:00+00:00",
    }
    res_neg = validate_order_event(rec_neg, valid_customers, seen_events)
    assert not res_neg.is_valid
    assert REASON_NON_POSITIVE_AMOUNT in res_neg.rejection_reasons

    rec_bad = {
        "event_id": "EVT_00000004",
        "customer_id": "CUST_000100",
        "order_amount": "invalid_number",
        "event_type": "PURCHASE",
        "event_timestamp": "2026-11-01T10:00:00+00:00",
    }
    res_bad = validate_order_event(rec_bad, valid_customers, seen_events)
    assert not res_bad.is_valid
    assert REASON_NON_NUMERIC_AMOUNT in res_bad.rejection_reasons


def test_order_event_invalid_timestamp():
    """Verify ISO-8601 timestamp conformance."""
    valid_customers = {"CUST_000100"}
    seen_events: Set[str] = set()

    for bad_ts in ["", "not-a-timestamp", "2026/11/01 10:00", "2026-99-99T99:99:99"]:
        rec = {
            "event_id": f"EVT_TS_{hash(bad_ts)}",
            "customer_id": "CUST_000100",
            "order_amount": "25.00",
            "event_type": "PURCHASE",
            "event_timestamp": bad_ts,
        }
        res = validate_order_event(rec, valid_customers, seen_events)
        assert not res.is_valid
        assert REASON_INVALID_TIMESTAMP in res.rejection_reasons


def test_multiple_validation_errors_captured():
    """Verify multiple distinct failure reasons are captured on a single record."""
    valid_customers = {"CUST_000100"}
    seen_events: Set[str] = set()

    malformed_record = {
        "event_id": "",  # missing event ID
        "customer_id": "",  # missing customer ID
        "order_amount": "-10.00",  # non-positive amount
        "event_type": "INVALID_TYPE",  # invalid event type
        "event_timestamp": "invalid_date",  # invalid timestamp
    }
    res = validate_order_event(malformed_record, valid_customers, seen_events)
    assert not res.is_valid
    assert len(res.rejection_reasons) == 5
    assert REASON_MISSING_EVENT_ID in res.rejection_reasons
    assert REASON_MISSING_CUSTOMER_ID in res.rejection_reasons
    assert REASON_NON_POSITIVE_AMOUNT in res.rejection_reasons
    assert REASON_INVALID_EVENT_TYPE in res.rejection_reasons
    assert REASON_INVALID_TIMESTAMP in res.rejection_reasons
