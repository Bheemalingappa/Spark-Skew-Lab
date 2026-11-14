"""Quarantine handler for rejected records in SparkSkewLab.

Writes malformed, duplicate, or rule-violating records to segregated quarantine datasets
preserving complete lineage: original payload, primary rejection reason, multi-error list,
source_file, batch_id, and run_id.
"""

import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

QUARANTINE_CUSTOMER_FIELDS = [
    "customer_id",
    "customer_name",
    "city",
    "customer_segment",
    "rejection_reason",
    "all_rejection_reasons",
    "source_file",
    "batch_id",
    "run_id"
]

QUARANTINE_EVENT_FIELDS = [
    "event_id",
    "customer_id",
    "order_amount",
    "event_type",
    "event_timestamp",
    "rejection_reason",
    "all_rejection_reasons",
    "source_file",
    "batch_id",
    "run_id"
]


class QuarantineWriter:
    """Manages writing quarantined records with full diagnostic metadata."""

    def __init__(self, quarantine_dir: Path, run_id: str, batch_id: str):
        self.quarantine_dir = quarantine_dir
        self.run_id = run_id
        self.batch_id = batch_id
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

        self.customers_quarantine_path = self.quarantine_dir / "quarantined_customers.csv"
        self.events_quarantine_path = self.quarantine_dir / "quarantined_order_events.csv"

        self._cust_file = None
        self._cust_writer = None
        self._event_file = None
        self._event_writer = None

        self.quarantined_customers_count = 0
        self.quarantined_events_count = 0

    def open(self) -> "QuarantineWriter":
        """Open quarantine output files and write CSV headers."""
        self._cust_file = open(self.customers_quarantine_path, mode="w", newline="", encoding="utf-8")
        self._cust_writer = csv.DictWriter(self._cust_file, fieldnames=QUARANTINE_CUSTOMER_FIELDS)
        self._cust_writer.writeheader()

        self._event_file = open(self.events_quarantine_path, mode="w", newline="", encoding="utf-8")
        self._event_writer = csv.DictWriter(self._event_file, fieldnames=QUARANTINE_EVENT_FIELDS)
        self._event_writer.writeheader()
        return self

    def write_quarantined_customer(self, record: Dict[str, Any], reasons: List[str]) -> None:
        """Record a rejected customer with diagnostic attributes."""
        if not self._cust_writer:
            raise RuntimeError("QuarantineWriter must be opened before writing.")

        row = {
            "customer_id": record.get("customer_id", ""),
            "customer_name": record.get("customer_name", ""),
            "city": record.get("city", ""),
            "customer_segment": record.get("customer_segment", ""),
            "rejection_reason": reasons[0] if reasons else "unknown",
            "all_rejection_reasons": "; ".join(reasons),
            "source_file": record.get("source_file", ""),
            "batch_id": self.batch_id,
            "run_id": self.run_id,
        }
        self._cust_writer.writerow(row)
        self.quarantined_customers_count += 1

    def write_quarantined_event(self, record: Dict[str, Any], reasons: List[str]) -> None:
        """Record a rejected order event with diagnostic attributes."""
        if not self._event_writer:
            raise RuntimeError("QuarantineWriter must be opened before writing.")

        row = {
            "event_id": record.get("event_id", ""),
            "customer_id": record.get("customer_id", ""),
            "order_amount": record.get("order_amount", ""),
            "event_type": record.get("event_type", ""),
            "event_timestamp": record.get("event_timestamp", ""),
            "rejection_reason": reasons[0] if reasons else "unknown",
            "all_rejection_reasons": "; ".join(reasons),
            "source_file": record.get("source_file", ""),
            "batch_id": self.batch_id,
            "run_id": self.run_id,
        }
        self._event_writer.writerow(row)
        self.quarantined_events_count += 1

    def close(self) -> None:
        """Flush and close active quarantine file handles."""
        if self._cust_file:
            self._cust_file.close()
            self._cust_file = None
        if self._event_file:
            self._event_file.close()
            self._event_file = None

    def __enter__(self) -> "QuarantineWriter":
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
