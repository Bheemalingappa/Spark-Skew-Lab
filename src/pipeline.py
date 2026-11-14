"""Ingestion and data quality validation pipeline for SparkSkewLab.

Coordinates:
- Ingestion of customer dimension and transactional events
- Customer key validation and reference set construction
- Event schema, business rule, and referential integrity validation
- Segregated outputs for clean and quarantined datasets
- Data quality auditing and operational run reporting
"""

from collections import Counter
import csv
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set
import uuid

from src.config import (
    CUSTOMER_FIELDS,
    ORDER_EVENT_FIELDS,
    OUTPUTS_CLEAN_DIR,
    OUTPUTS_LOGS_DIR,
    OUTPUTS_QUARANTINE_DIR,
    OUTPUTS_REPORTS_DIR,
    REASON_DUPLICATE_CUSTOMER_ID,
    REASON_DUPLICATE_EVENT_ID,
    REASON_INVALID_CUSTOMER_REF,
    REASON_MISSING_CITY,
    REASON_MISSING_CUSTOMER_ID,
    REASON_MISSING_CUSTOMER_NAME,
    REASON_MISSING_EVENT_ID,
    REASON_MISSING_TIMESTAMP,
)
from src.ingestion import ingest_customers, ingest_order_events
from src.quarantine import QuarantineWriter
from src.validation import validate_customer, validate_order_event

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("SparkSkewLab.Pipeline")

CLEAN_CUSTOMER_FIELDS = CUSTOMER_FIELDS + ["source_file", "batch_id", "run_id"]
CLEAN_EVENT_FIELDS = ORDER_EVENT_FIELDS + ["batch_id", "run_id"]


def generate_run_id() -> str:
    """Generate a unique, chronological run identifier."""
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    return f"run_{now_str}_{short_uuid}"


class DataPipeline:
    """Orchestrates end-to-end ingestion, validation, quarantine, and reporting."""

    def __init__(
        self,
        input_dir: Path,
        batch_id: str = "batch_001",
        run_id: Optional[str] = None,
        clean_dir: Optional[Path] = None,
        quarantine_dir: Optional[Path] = None,
        reports_dir: Optional[Path] = None
    ):
        self.input_dir = Path(input_dir)
        self.batch_id = batch_id
        self.run_id = run_id or generate_run_id()

        self.clean_dir = clean_dir or OUTPUTS_CLEAN_DIR
        self.quarantine_dir = quarantine_dir or OUTPUTS_QUARANTINE_DIR
        self.reports_dir = reports_dir or OUTPUTS_REPORTS_DIR

        self.clean_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.customers_input_path = self.input_dir / "customers.csv"
        self.events_input_path = self.input_dir / "order_events.csv"

        self.clean_customers_path = self.clean_dir / "clean_customers.csv"
        self.clean_events_path = self.clean_dir / "clean_order_events.csv"

        # Operational metrics tracking
        self.metrics: Dict[str, Any] = {
            "customers": {"input": 0, "accepted": 0, "rejected": 0},
            "events": {"input": 0, "accepted": 0, "rejected": 0},
            "reasons": Counter(),
        }

    def run(self) -> Dict[str, Any]:
        """Execute the validation pipeline."""
        start_time = datetime.now(timezone.utc)
        logger.info(f"Starting pipeline {self.run_id} for batch {self.batch_id} (input: {self.input_dir})")
        status = "SUCCESS"
        error_details = None

        try:
            with QuarantineWriter(self.quarantine_dir, self.run_id, self.batch_id) as q_writer:
                # 1. Process customer dimension
                valid_customer_ids = self._process_customers(q_writer)

                # 2. Process order events against valid customer dimension
                self._process_order_events(valid_customer_ids, q_writer)

        except Exception as e:
            status = "FAILED"
            error_details = str(e)
            logger.error(f"Pipeline execution encountered failure: {e}", exc_info=True)
            raise
        finally:
            end_time = datetime.now(timezone.utc)
            # 3. Generate Reports regardless of run status
            dq_report = self._generate_data_quality_report()
            run_report = self._generate_run_report(start_time, end_time, status, error_details)

            logger.info(
                f"Pipeline {self.run_id} completed with status {status}. "
                f"Accepted Events: {self.metrics['events']['accepted']:,}, "
                f"Quarantined Events: {self.metrics['events']['rejected']:,}"
            )

        return {
            "status": status,
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "data_quality_report": dq_report,
            "run_report": run_report
        }

    def _process_customers(self, q_writer: QuarantineWriter) -> Set[str]:
        """Ingest, validate, and partition customer records."""
        seen_customer_ids: Set[str] = set()
        valid_customer_ids: Set[str] = set()

        with open(self.clean_customers_path, mode="w", newline="", encoding="utf-8") as clean_file:
            clean_writer = csv.DictWriter(clean_file, fieldnames=CLEAN_CUSTOMER_FIELDS)
            clean_writer.writeheader()

            for record in ingest_customers(self.customers_input_path, self.batch_id):
                self.metrics["customers"]["input"] += 1
                validation = validate_customer(record, seen_customer_ids)

                if validation.is_valid:
                    valid_customer_ids.add(record["customer_id"])
                    clean_row = {
                        "customer_id": record["customer_id"],
                        "customer_name": record["customer_name"],
                        "city": record["city"],
                        "customer_segment": record["customer_segment"],
                        "source_file": record["source_file"],
                        "batch_id": self.batch_id,
                        "run_id": self.run_id,
                    }
                    clean_writer.writerow(clean_row)
                    self.metrics["customers"]["accepted"] += 1
                else:
                    q_writer.write_quarantined_customer(record, validation.rejection_reasons)
                    self.metrics["customers"]["rejected"] += 1
                    for r in validation.rejection_reasons:
                        self.metrics["reasons"][r] += 1

        logger.info(
            f"Customer validation completed: {self.metrics['customers']['input']:,} input, "
            f"{self.metrics['customers']['accepted']:,} accepted, "
            f"{self.metrics['customers']['rejected']:,} quarantined."
        )
        return valid_customer_ids

    def _process_order_events(self, valid_customer_ids: Set[str], q_writer: QuarantineWriter) -> None:
        """Ingest, validate, and partition order events."""
        seen_event_ids: Set[str] = set()

        with open(self.clean_events_path, mode="w", newline="", encoding="utf-8") as clean_file:
            clean_writer = csv.DictWriter(clean_file, fieldnames=CLEAN_EVENT_FIELDS)
            clean_writer.writeheader()

            for record in ingest_order_events(self.events_input_path, self.batch_id):
                self.metrics["events"]["input"] += 1
                validation = validate_order_event(record, valid_customer_ids, seen_event_ids)

                if validation.is_valid:
                    clean_row = {
                        "event_id": record["event_id"],
                        "customer_id": record["customer_id"],
                        "order_amount": record["order_amount"],
                        "event_type": record["event_type"],
                        "event_timestamp": record["event_timestamp"],
                        "source_file": record["source_file"],
                        "batch_id": self.batch_id,
                        "run_id": self.run_id,
                    }
                    clean_writer.writerow(clean_row)
                    self.metrics["events"]["accepted"] += 1
                else:
                    q_writer.write_quarantined_event(record, validation.rejection_reasons)
                    self.metrics["events"]["rejected"] += 1
                    for r in validation.rejection_reasons:
                        self.metrics["reasons"][r] += 1

        logger.info(
            f"Events validation completed: {self.metrics['events']['input']:,} input, "
            f"{self.metrics['events']['accepted']:,} accepted, "
            f"{self.metrics['events']['rejected']:,} quarantined."
        )

    def _generate_data_quality_report(self) -> Dict[str, Any]:
        """Compile and persist comprehensive data quality audit report."""
        reasons_dict = dict(self.metrics["reasons"])

        duplicate_count = (
            reasons_dict.get(REASON_DUPLICATE_CUSTOMER_ID, 0) +
            reasons_dict.get(REASON_DUPLICATE_EVENT_ID, 0)
        )
        invalid_reference_count = reasons_dict.get(REASON_INVALID_CUSTOMER_REF, 0)
        missing_fields = {
            "missing_customer_id": reasons_dict.get(REASON_MISSING_CUSTOMER_ID, 0),
            "missing_event_id": reasons_dict.get(REASON_MISSING_EVENT_ID, 0),
            "missing_customer_name": reasons_dict.get(REASON_MISSING_CUSTOMER_NAME, 0),
            "missing_city": reasons_dict.get(REASON_MISSING_CITY, 0),
            "missing_or_invalid_timestamp": reasons_dict.get("invalid_or_missing_timestamp", 0),
        }

        report = {
            "project": "SparkSkewLab",
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "input_directory": str(self.input_dir.resolve()),
            "total_input_customers": self.metrics["customers"]["input"],
            "accepted_customers": self.metrics["customers"]["accepted"],
            "rejected_customers": self.metrics["customers"]["rejected"],
            "total_input_events": self.metrics["events"]["input"],
            "accepted_events": self.metrics["events"]["accepted"],
            "rejected_events": self.metrics["events"]["rejected"],
            "rejection_counts_grouped_by_reason": reasons_dict,
            "duplicate_count": duplicate_count,
            "invalid_reference_count": invalid_reference_count,
            "missing_field_counts": missing_fields,
            "reconciliation": {
                "customers_balanced": (
                    self.metrics["customers"]["input"] ==
                    self.metrics["customers"]["accepted"] + self.metrics["customers"]["rejected"]
                ),
                "events_balanced": (
                    self.metrics["events"]["input"] ==
                    self.metrics["events"]["accepted"] + self.metrics["events"]["rejected"]
                )
            },
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

        report_path = self.reports_dir / "data_quality_report.json"
        with open(report_path, mode="w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        return report

    def _generate_run_report(
        self,
        start_time: datetime,
        end_time: datetime,
        status: str,
        error_details: Optional[str]
    ) -> Dict[str, Any]:
        """Compile and persist execution run report."""
        duration_seconds = round((end_time - start_time).total_seconds(), 4)

        report = {
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "start_timestamp": start_time.isoformat(),
            "end_timestamp": end_time.isoformat(),
            "duration_seconds": duration_seconds,
            "status": status,
            "input_and_output_counts": {
                "total_input_rows": self.metrics["customers"]["input"] + self.metrics["events"]["input"],
                "total_accepted_rows": self.metrics["customers"]["accepted"] + self.metrics["events"]["accepted"],
                "total_quarantined_rows": self.metrics["customers"]["rejected"] + self.metrics["events"]["rejected"],
                "customers": self.metrics["customers"],
                "events": self.metrics["events"]
            },
            "output_paths": {
                "clean_customers": str(self.clean_customers_path.resolve()),
                "clean_order_events": str(self.clean_events_path.resolve()),
                "quarantined_customers": str((self.quarantine_dir / "quarantined_customers.csv").resolve()),
                "quarantined_order_events": str((self.quarantine_dir / "quarantined_order_events.csv").resolve()),
            },
            "error_details": error_details
        }

        report_path = self.reports_dir / "run_report.json"
        with open(report_path, mode="w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        return report
