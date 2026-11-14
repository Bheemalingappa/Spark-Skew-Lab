"""Validation rules and data quality evaluation for SparkSkewLab.

Enforces business rules, domain constraints, duplicate policies, and referential integrity
for customers and order events. Captures multi-failure diagnostics per record without double-counting
rejected rows.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
import logging
from typing import Any, Dict, List, Optional, Set

from src.config import (
    CUSTOMER_SEGMENTS,
    EVENT_TYPES,
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

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Outcome of validating a single record.
    
    Attributes:
        is_valid: True if the record passed all data quality checks.
        rejection_reasons: List of distinct failure reasons encountered.
    """
    is_valid: bool
    rejection_reasons: List[str] = field(default_factory=list)

    @property
    def primary_reason(self) -> str:
        """First or most prominent rejection reason."""
        return self.rejection_reasons[0] if self.rejection_reasons else ""

    @property
    def formatted_reasons(self) -> str:
        """Semicolon-delimited list of all failure reasons."""
        return "; ".join(self.rejection_reasons)


def is_valid_iso8601_timestamp(timestamp_str: Optional[str]) -> bool:
    """Check if a string adheres strictly to ISO-8601 format."""
    if not timestamp_str or not isinstance(timestamp_str, str):
        return False
    try:
        normalized = timestamp_str.replace("Z", "+00:00")
        datetime.fromisoformat(normalized)
        return True
    except (ValueError, TypeError):
        return False


def validate_customer(
    record: Dict[str, Any],
    seen_customer_ids: Set[str]
) -> ValidationResult:
    """Validate a single customer dimension record.
    
    Rules:
    1. customer_id must not be null or empty.
    2. customer_id must be unique across the batch (first seen is kept).
    3. customer_name must not be null or empty.
    4. city must not be null or empty.
    5. customer_segment must be valid and non-empty.
    """
    reasons: List[str] = []

    cust_id = (record.get("customer_id") or "").strip()
    if not cust_id:
        reasons.append(REASON_MISSING_CUSTOMER_ID)
    elif cust_id in seen_customer_ids:
        reasons.append(REASON_DUPLICATE_CUSTOMER_ID)

    cust_name = (record.get("customer_name") or "").strip()
    if not cust_name:
        reasons.append(REASON_MISSING_CUSTOMER_NAME)

    city = (record.get("city") or "").strip()
    if not city:
        reasons.append(REASON_MISSING_CITY)

    segment = (record.get("customer_segment") or "").strip()
    if not segment or segment not in CUSTOMER_SEGMENTS:
        reasons.append(REASON_INVALID_CUSTOMER_SEGMENT)

    # State update for uniqueness: register ID if present
    if cust_id and cust_id not in seen_customer_ids:
        seen_customer_ids.add(cust_id)

    is_valid = len(reasons) == 0
    return ValidationResult(is_valid=is_valid, rejection_reasons=reasons)


def validate_order_event(
    record: Dict[str, Any],
    valid_customer_ids: Set[str],
    seen_event_ids: Set[str]
) -> ValidationResult:
    """Validate a single transactional order event record.
    
    Duplicate Policy:
    - First occurrence wins: The first valid record encountered for an event_id is accepted.
    - Subsequent occurrences carrying the same event_id are rejected with duplicate_event_id.
    - Event ordering policy is arrival order within the stream.
    
    Rules:
    1. event_id must not be null or empty.
    2. event_id must be unique across the batch.
    3. customer_id must not be null or empty.
    4. customer_id must exist in valid_customer_ids (referential integrity).
    5. order_amount must be numeric and > 0.
    6. event_type must be valid and non-empty.
    7. event_timestamp must be valid ISO-8601.
    """
    reasons: List[str] = []

    # 1 & 2: Event ID null check and duplicate check
    event_id = (record.get("event_id") or "").strip()
    if not event_id:
        reasons.append(REASON_MISSING_EVENT_ID)
    elif event_id in seen_event_ids:
        reasons.append(REASON_DUPLICATE_EVENT_ID)

    # 3 & 4: Customer ID null check and referential integrity
    customer_id = (record.get("customer_id") or "").strip()
    if not customer_id:
        reasons.append(REASON_MISSING_CUSTOMER_ID)
    elif customer_id not in valid_customer_ids:
        reasons.append(REASON_INVALID_CUSTOMER_REF)

    # 5: Order amount must be numeric and > 0
    raw_amount = record.get("order_amount")
    if raw_amount is None or str(raw_amount).strip() == "":
        reasons.append(REASON_NON_NUMERIC_AMOUNT)
    else:
        try:
            amt = Decimal(str(raw_amount).strip())
            if amt <= Decimal("0"):
                reasons.append(REASON_NON_POSITIVE_AMOUNT)
        except (InvalidOperation, ValueError, TypeError):
            reasons.append(REASON_NON_NUMERIC_AMOUNT)

    # 6: Event type check
    event_type = (record.get("event_type") or "").strip()
    if not event_type or event_type not in EVENT_TYPES:
        reasons.append(REASON_INVALID_EVENT_TYPE)

    # 7: Timestamp ISO-8601 check
    raw_ts = record.get("event_timestamp")
    if not is_valid_iso8601_timestamp(raw_ts):
        reasons.append(REASON_INVALID_TIMESTAMP)

    # Register event_id in seen set if non-empty to enforce first-occurrence-wins policy
    if event_id and event_id not in seen_event_ids:
        seen_event_ids.add(event_id)

    is_valid = len(reasons) == 0
    return ValidationResult(is_valid=is_valid, rejection_reasons=reasons)
