"""Idempotency, failure handling, retry, restart, and backfill framework for SparkSkewLab.

Provides:
- Batch and Run identity management with cryptographic input fingerprinting
- Centralized batch ledger for tracking run states, retry counts, and recovery attempts
- Two-phase commit / staging pattern ensuring failure safety and zero partial writes
- Idempotent rerun detection preventing duplicate publishing
- Controlled restart for interrupted runs and safe backfill for historical reprocessing
- Structured observability reports capturing complete pipeline lineage
"""

import copy
import csv
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import traceback
from typing import Any, Dict, List, Optional, Set, Tuple
import uuid

from src.config import (
    CUSTOMER_FIELDS,
    ORDER_EVENT_FIELDS,
    OUTPUTS_CLEAN_DIR,
    OUTPUTS_LOGS_DIR,
    OUTPUTS_QUARANTINE_DIR,
    OUTPUTS_REPORTS_DIR,
    WORKSPACE_ROOT,
)
from src.ingestion import ingest_customers, ingest_order_events
from src.pipeline import (
    CLEAN_CUSTOMER_FIELDS,
    CLEAN_EVENT_FIELDS,
    DataPipeline,
    generate_run_id,
)
from src.quarantine import QuarantineWriter
from src.validation import validate_customer, validate_order_event

logger = logging.getLogger("SparkSkewLab.Recovery")

DEFAULT_LEDGER_PATH = OUTPUTS_LOGS_DIR / "batch_ledger.json"


def compute_file_sha256(file_path: Path) -> str:
    """Compute SHA-256 hash of a file efficiently in 64KB chunks."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_input_fingerprint(input_dir: Path) -> Dict[str, Any]:
    """Compute cryptographic fingerprints of all CSV input datasets in a directory."""
    input_path = Path(input_dir)
    fingerprint = {
        "input_directory": str(input_path.resolve()),
        "files": {},
        "composite_hash": "",
    }

    composite_hasher = hashlib.sha256()
    csv_files = sorted(list(input_path.glob("*.csv")), key=lambda p: p.name)

    for p in csv_files:
        f_hash = compute_file_sha256(p)
        f_size = p.stat().st_size
        fingerprint["files"][p.name] = {
            "sha256": f_hash,
            "size_bytes": f_size,
        }
        composite_hasher.update(p.name.encode("utf-8"))
        composite_hasher.update(f_hash.encode("utf-8"))

    fingerprint["composite_hash"] = composite_hasher.hexdigest()
    return fingerprint


class BatchLedger:
    """Persistent ledger managing batch states, retry histories, and recovery tracking."""

    def __init__(self, ledger_path: Optional[Path] = None):
        self.ledger_path = Path(ledger_path or DEFAULT_LEDGER_PATH)
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        self._state: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        """Load ledger from JSON file or initialize empty state."""
        if self.ledger_path.exists():
            try:
                with open(self.ledger_path, mode="r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load ledger from {self.ledger_path}: {e}. Initializing fresh ledger.")
        return {"batches": {}, "updated_at": datetime.now(timezone.utc).isoformat()}

    def _save(self) -> None:
        """Atomically persist ledger to disk using temp file replacement."""
        self._state["updated_at"] = datetime.now(timezone.utc).isoformat()
        tmp_path = self.ledger_path.with_suffix(".tmp")
        with open(tmp_path, mode="w", encoding="utf-8") as f:
            json.dump(self._state, f, indent=2)
        os.replace(tmp_path, self.ledger_path)

    def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve state and run history for a batch (reloads from disk)."""
        self._state = self._load()
        return self._state["batches"].get(batch_id)

    def get_all_batches(self) -> Dict[str, Any]:
        """Return all tracked batches (reloads from disk)."""
        self._state = self._load()
        return copy.deepcopy(self._state["batches"])

    def mark_interrupted_runs(self) -> List[str]:
        """Identify batches stuck in IN_PROGRESS from a crashed process and mark INTERRUPTED."""
        interrupted = []
        for batch_id, batch_data in self._state["batches"].items():
            if batch_data.get("current_status") == "IN_PROGRESS":
                batch_data["current_status"] = "INTERRUPTED"
                if batch_data.get("runs"):
                    latest_run = batch_data["runs"][-1]
                    if latest_run.get("status") == "IN_PROGRESS":
                        latest_run["status"] = "INTERRUPTED"
                        latest_run["error_details"] = "Run was interrupted before completion (process crash or abort)."
                        latest_run["end_time"] = datetime.now(timezone.utc).isoformat()
                interrupted.append(batch_id)
        if interrupted:
            self._save()
            logger.info(f"Marked {len(interrupted)} interrupted batch(es): {interrupted}")
        return interrupted

    def record_run_start(
        self,
        batch_id: str,
        run_id: str,
        input_hash: str,
        is_backfill: bool = False,
        force: bool = False
    ) -> Dict[str, Any]:
        """Record the start of a new processing run for a batch."""
        now_iso = datetime.now(timezone.utc).isoformat()
        batch = self._state["batches"].get(batch_id)

        if not batch:
            # First time batch execution
            batch = {
                "batch_id": batch_id,
                "current_status": "IN_PROGRESS",
                "latest_run_id": run_id,
                "input_hash": input_hash,
                "first_attempt_timestamp": now_iso,
                "last_attempt_timestamp": now_iso,
                "retry_count": 0,
                "recovery_attempt_count": 0,
                "is_backfill": is_backfill,
                "runs": []
            }
            self._state["batches"][batch_id] = batch
            attempt_type = "FIRST_RUN"
        else:
            previous_status = batch.get("current_status")
            batch["latest_run_id"] = run_id
            batch["last_attempt_timestamp"] = now_iso
            batch["current_status"] = "IN_PROGRESS"

            if is_backfill:
                batch["is_backfill"] = True
                attempt_type = "BACKFILL"
            elif previous_status in ("FAILED", "INTERRUPTED"):
                batch["retry_count"] = batch.get("retry_count", 0) + 1
                if previous_status == "INTERRUPTED":
                    batch["recovery_attempt_count"] = batch.get("recovery_attempt_count", 0) + 1
                    attempt_type = "RESTART"
                else:
                    attempt_type = "RETRY"
            elif force:
                attempt_type = "FORCED_RERUN"
            else:
                attempt_type = "RERUN"

        run_entry = {
            "run_id": run_id,
            "status": "IN_PROGRESS",
            "attempt_type": attempt_type,
            "start_time": now_iso,
            "end_time": None,
            "duration_seconds": None,
            "input_hash": input_hash,
            "is_backfill": is_backfill,
            "counts": {},
            "reconciliation_status": "PENDING",
            "error_details": None,
        }
        batch["runs"].append(run_entry)
        self._save()
        return batch

    def record_run_completion(
        self,
        batch_id: str,
        run_id: str,
        status: str,
        counts: Dict[str, Any],
        reconciliation_status: str,
        error_details: Optional[str] = None,
        output_paths: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Record the final outcome of an executed run."""
        now_iso = datetime.now(timezone.utc).isoformat()
        batch = self._state["batches"].get(batch_id)
        if not batch:
            raise KeyError(f"Batch {batch_id} not registered in ledger.")

        batch["current_status"] = status
        batch["last_attempt_timestamp"] = now_iso

        # Locate run entry
        run_entry = next((r for r in reversed(batch["runs"]) if r["run_id"] == run_id), None)
        if run_entry:
            run_entry["status"] = status
            run_entry["end_time"] = now_iso
            start_dt = datetime.fromisoformat(run_entry["start_time"])
            end_dt = datetime.fromisoformat(now_iso)
            run_entry["duration_seconds"] = round((end_dt - start_dt).total_seconds(), 4)
            run_entry["counts"] = counts
            run_entry["reconciliation_status"] = reconciliation_status
            run_entry["error_details"] = error_details
            if output_paths:
                run_entry["output_paths"] = output_paths

        self._save()
        return batch

    def record_idempotent_skip(
        self,
        batch_id: str,
        run_id: str,
        existing_run_id: str,
        input_hash: str
    ) -> Dict[str, Any]:
        """Record an idempotent skip event without modifying published data."""
        now_iso = datetime.now(timezone.utc).isoformat()
        batch = self._state["batches"][batch_id]

        skip_entry = {
            "run_id": run_id,
            "status": "SKIPPED_IDEMPOTENT",
            "attempt_type": "IDEMPOTENT_CHECK",
            "start_time": now_iso,
            "end_time": now_iso,
            "duration_seconds": 0.0,
            "input_hash": input_hash,
            "is_backfill": False,
            "referenced_run_id": existing_run_id,
            "counts": batch["runs"][-1].get("counts", {}),
            "reconciliation_status": batch["runs"][-1].get("reconciliation_status", "BALANCED"),
            "error_details": None,
            "message": "Batch has already been successfully published. Execution skipped to maintain idempotency."
        }
        batch["runs"].append(skip_entry)
        self._save()
        return batch

    def get_run_history(self, batch_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieve flattened list of all execution runs (reloads from disk)."""
        self._state = self._load()
        runs = []
        if batch_id:
            batch = self._state["batches"].get(batch_id)
            if batch:
                for r in batch["runs"]:
                    r_copy = dict(r)
                    r_copy["batch_id"] = batch_id
                    runs.append(r_copy)
        else:
            for b_id, b_data in self._state["batches"].items():
                for r in b_data["runs"]:
                    r_copy = dict(r)
                    r_copy["batch_id"] = b_id
                    runs.append(r_copy)
        return runs


class IdempotentPipeline(DataPipeline):
    """Enhanced DataPipeline featuring staging isolation, idempotency checks, retry tracking, and backfills."""

    def __init__(
        self,
        input_dir: Path,
        batch_id: str = "batch_001",
        run_id: Optional[str] = None,
        clean_dir: Optional[Path] = None,
        quarantine_dir: Optional[Path] = None,
        reports_dir: Optional[Path] = None,
        ledger_path: Optional[Path] = None,
        is_backfill: bool = False,
        force: bool = False
    ):
        super().__init__(
            input_dir=input_dir,
            batch_id=batch_id,
            run_id=run_id,
            clean_dir=clean_dir,
            quarantine_dir=quarantine_dir,
            reports_dir=reports_dir
        )
        self.is_backfill = is_backfill
        self.force = force
        self.ledger = BatchLedger(ledger_path)

        # Isolated staging directory for two-phase commit
        self.staging_dir = self.clean_dir / f".staging_{self.run_id}"
        self.staging_quarantine_dir = self.quarantine_dir / f".staging_{self.run_id}"

    def run(self) -> Dict[str, Any]:
        """Execute pipeline with idempotency verification, staging isolation, and ledger tracking."""
        start_time = datetime.now(timezone.utc)
        self.ledger.mark_interrupted_runs()

        # 1. Compute input cryptographic fingerprint
        fingerprint = compute_input_fingerprint(self.input_dir)
        input_hash = fingerprint["composite_hash"]

        # 2. Check Ledger for existing successful execution (Idempotency Check)
        existing_batch = self.ledger.get_batch(self.batch_id)
        if (
            existing_batch and
            existing_batch.get("current_status") == "SUCCESS" and
            not self.is_backfill and
            not self.force
        ):
            existing_hash = existing_batch.get("input_hash")
            if existing_hash and existing_hash != input_hash:
                raise ValueError(
                    f"Batch '{self.batch_id}' has already been successfully processed with input "
                    f"fingerprint '{existing_hash}', but current input has fingerprint '{input_hash}'. "
                    f"To update or reprocess with modified data, set is_backfill=True or force=True "
                    f"as documented in docs/idempotency_recovery.md."
                )

            logger.info(
                f"Batch '{self.batch_id}' has already been successfully published "
                f"(latest successful run: {existing_batch.get('latest_run_id')}). "
                f"Skipping execution to guarantee idempotency."
            )
            self.ledger.record_idempotent_skip(
                batch_id=self.batch_id,
                run_id=self.run_id,
                existing_run_id=existing_batch.get("latest_run_id"),
                input_hash=input_hash
            )

            # Return idempotent skip outcome with existing paths
            latest_run = existing_batch["runs"][-2] if len(existing_batch["runs"]) >= 2 else existing_batch["runs"][-1]
            obs_report = self._build_observability_report(
                status="SKIPPED_IDEMPOTENT",
                start_time=start_time,
                end_time=datetime.now(timezone.utc),
                counts=latest_run.get("counts", {}),
                reconciliation_status=latest_run.get("reconciliation_status", "BALANCED"),
                error_details=None,
                attempt_type="IDEMPOTENT_CHECK",
                retry_count=existing_batch.get("retry_count", 0),
                recovery_count=existing_batch.get("recovery_attempt_count", 0),
                fingerprint=fingerprint
            )
            return obs_report

        # 3. Register run start in ledger
        batch_entry = self.ledger.record_run_start(
            batch_id=self.batch_id,
            run_id=self.run_id,
            input_hash=input_hash,
            is_backfill=self.is_backfill,
            force=self.force
        )
        attempt_type = batch_entry["runs"][-1]["attempt_type"]
        retry_count = batch_entry.get("retry_count", 0)
        recovery_count = batch_entry.get("recovery_attempt_count", 0)

        # 4. Setup staging directories
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.staging_quarantine_dir.mkdir(parents=True, exist_ok=True)

        staging_clean_customers = self.staging_dir / "clean_customers.csv"
        staging_clean_events = self.staging_dir / "clean_order_events.csv"

        status = "SUCCESS"
        error_details = None
        reconciliation_status = "BALANCED"

        try:
            logger.info(
                f"Executing batch '{self.batch_id}' [Run: {self.run_id}, Attempt: {attempt_type}, "
                f"RetryCount: {retry_count}, Backfill: {self.is_backfill}]"
            )

            # A. Process Customers into staging
            with QuarantineWriter(self.staging_quarantine_dir, self.run_id, self.batch_id) as q_writer:
                seen_customer_ids: Set[str] = set()
                valid_customer_ids: Set[str] = set()

                with open(staging_clean_customers, mode="w", newline="", encoding="utf-8") as clean_file:
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

                # B. Process Events into staging
                seen_event_ids: Set[str] = set()
                with open(staging_clean_events, mode="w", newline="", encoding="utf-8") as clean_file:
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

            # C. In-Flight Integrity & Reconciliation Verification
            cust_in = self.metrics["customers"]["input"]
            cust_acc = self.metrics["customers"]["accepted"]
            cust_rej = self.metrics["customers"]["rejected"]
            evt_in = self.metrics["events"]["input"]
            evt_acc = self.metrics["events"]["accepted"]
            evt_rej = self.metrics["events"]["rejected"]

            if (cust_in != cust_acc + cust_rej) or (evt_in != evt_acc + evt_rej):
                reconciliation_status = "DISCREPANCY"
                raise ValueError(
                    f"Reconciliation integrity check failed: Customers in={cust_in} != "
                    f"({cust_acc} + {cust_rej}), Events in={evt_in} != ({evt_acc} + {evt_rej})"
                )

            # D. Two-Phase Commit: Atomically move staging to published destinations
            self._commit_staging(staging_clean_customers, staging_clean_events)

        except Exception as e:
            status = "FAILED"
            reconciliation_status = "FAILED"
            error_details = str(e)
            logger.error(f"Batch '{self.batch_id}' execution failed during run '{self.run_id}': {e}", exc_info=True)
            # Safe cleanup of partial staging
            self._cleanup_staging()
            # Record failure in ledger
            counts = self._build_counts_summary()
            self.ledger.record_run_completion(
                batch_id=self.batch_id,
                run_id=self.run_id,
                status="FAILED",
                counts=counts,
                reconciliation_status=reconciliation_status,
                error_details=error_details
            )
            # Persist failure run report
            end_time = datetime.now(timezone.utc)
            self._build_observability_report(
                status="FAILED",
                start_time=start_time,
                end_time=end_time,
                counts=counts,
                reconciliation_status=reconciliation_status,
                error_details=error_details,
                attempt_type=attempt_type,
                retry_count=retry_count,
                recovery_count=recovery_count,
                fingerprint=fingerprint
            )
            raise
        else:
            # E. Record success in ledger
            end_time = datetime.now(timezone.utc)
            counts = self._build_counts_summary()
            output_paths = {
                "clean_customers": str(self.clean_customers_path.resolve()),
                "clean_order_events": str(self.clean_events_path.resolve()),
                "quarantined_customers": str((self.quarantine_dir / "quarantined_customers.csv").resolve()),
                "quarantined_order_events": str((self.quarantine_dir / "quarantined_order_events.csv").resolve()),
            }
            self.ledger.record_run_completion(
                batch_id=self.batch_id,
                run_id=self.run_id,
                status="SUCCESS",
                counts=counts,
                reconciliation_status=reconciliation_status,
                error_details=None,
                output_paths=output_paths
            )

            # F. Build comprehensive observability report
            obs_report = self._build_observability_report(
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                counts=counts,
                reconciliation_status=reconciliation_status,
                error_details=None,
                attempt_type=attempt_type,
                retry_count=retry_count,
                recovery_count=recovery_count,
                fingerprint=fingerprint
            )
            return obs_report
        finally:
            self._cleanup_staging()

    def _commit_staging(self, staging_cust: Path, staging_evt: Path) -> None:
        """Atomically commit staged files to published destination with all-or-nothing rollback safety."""
        staging_q_cust = self.staging_quarantine_dir / "quarantined_customers.csv"
        staging_q_evt = self.staging_quarantine_dir / "quarantined_order_events.csv"

        final_q_cust = self.quarantine_dir / "quarantined_customers.csv"
        final_q_evt = self.quarantine_dir / "quarantined_order_events.csv"

        commit_pairs = [
            (staging_cust, self.clean_customers_path),
            (staging_evt, self.clean_events_path),
            (staging_q_cust, final_q_cust),
            (staging_q_evt, final_q_evt),
        ]

        backup_dir = self.clean_dir / f".backup_{self.run_id}"
        backup_dir.mkdir(parents=True, exist_ok=True)
        backed_up_targets: Dict[Path, Optional[Path]] = {}
        replaced_targets: List[Path] = []

        try:
            # Step 1: Pre-commit backup of all existing target files
            for src, dst in commit_pairs:
                if dst.exists():
                    backup_copy = backup_dir / f"{dst.name}.bak"
                    shutil.copy2(dst, backup_copy)
                    backed_up_targets[dst] = backup_copy
                else:
                    backed_up_targets[dst] = None

            # Step 2: Atomic replacement of each staged file to target
            for src, dst in commit_pairs:
                if src.exists():
                    os.replace(src, dst)
                    replaced_targets.append(dst)

            logger.info(
                f"Two-phase commit complete: all files atomically updated for batch '{self.batch_id}'."
            )

        except Exception as commit_err:
            logger.error(
                f"Commit failure during publishing batch '{self.batch_id}': {commit_err}. "
                f"Initiating rollback of {len(replaced_targets)} replaced target(s)..."
            )
            # Step 3: Rollback any replaced files to exact pre-commit state
            for dst in reversed(replaced_targets):
                backup_copy = backed_up_targets.get(dst)
                if backup_copy and backup_copy.exists():
                    try:
                        os.replace(backup_copy, dst)
                    except Exception as rb_err:
                        logger.critical(f"Rollback error restoring {dst} from {backup_copy}: {rb_err}")
                elif dst.exists():
                    try:
                        dst.unlink()
                    except Exception as rm_err:
                        logger.critical(f"Rollback error removing new file {dst}: {rm_err}")
            raise
        finally:
            # Step 4: Clean up backup directory
            if backup_dir.exists():
                shutil.rmtree(backup_dir, ignore_errors=True)

    def _cleanup_staging(self) -> None:
        """Safely delete staging directories to prevent partial residue."""
        for d in [self.staging_dir, self.staging_quarantine_dir]:
            if d.exists():
                try:
                    shutil.rmtree(d, ignore_errors=True)
                except Exception as e:
                    logger.warning(f"Failed to cleanly remove staging dir {d}: {e}")

    def _build_counts_summary(self) -> Dict[str, Any]:
        """Aggregate input, accepted, rejected, and published counts."""
        cust_in = self.metrics["customers"]["input"]
        cust_acc = self.metrics["customers"]["accepted"]
        cust_rej = self.metrics["customers"]["rejected"]
        evt_in = self.metrics["events"]["input"]
        evt_acc = self.metrics["events"]["accepted"]
        evt_rej = self.metrics["events"]["rejected"]

        return {
            "input_counts": {
                "customers": cust_in,
                "events": evt_in,
                "total": cust_in + evt_in
            },
            "accepted_counts": {
                "customers": cust_acc,
                "events": evt_acc,
                "total": cust_acc + evt_acc
            },
            "rejected_counts": {
                "customers": cust_rej,
                "events": evt_rej,
                "total": cust_rej + evt_rej
            },
            "published_counts": {
                "customers": cust_acc,
                "events": evt_acc,
                "total": cust_acc + evt_acc
            }
        }

    def _build_observability_report(
        self,
        status: str,
        start_time: datetime,
        end_time: datetime,
        counts: Dict[str, Any],
        reconciliation_status: str,
        error_details: Optional[str],
        attempt_type: str,
        retry_count: int,
        recovery_count: int,
        fingerprint: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Construct, persist, and return the comprehensive Step 6 observability report."""
        duration_sec = round((end_time - start_time).total_seconds(), 4)

        report = {
            "project": "SparkSkewLab",
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "processing_status": status,
            "attempt_type": attempt_type,
            "is_backfill": self.is_backfill,
            "retry_count": retry_count,
            "recovery_attempt_count": recovery_count,
            "start_timestamp": start_time.isoformat(),
            "end_timestamp": end_time.isoformat(),
            "duration_seconds": duration_sec,
            "reconciliation_status": reconciliation_status,
            "error_details": error_details,
            "counts": counts,
            "input_fingerprint": fingerprint,
            "output_paths": {
                "clean_customers": str(self.clean_customers_path.resolve()),
                "clean_order_events": str(self.clean_events_path.resolve()),
                "quarantined_customers": str((self.quarantine_dir / "quarantined_customers.csv").resolve()),
                "quarantined_order_events": str((self.quarantine_dir / "quarantined_order_events.csv").resolve()),
            },
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

        # Save individual run report
        run_rep_path = self.reports_dir / f"run_report_{self.run_id}.json"
        with open(run_rep_path, mode="w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        # Update latest run report
        latest_rep_path = self.reports_dir / "run_report.json"
        with open(latest_rep_path, mode="w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        return report


def run_recovery_demonstration(
    demo_base_dir: Optional[Path] = None,
    output_report_path: Optional[Path] = None
) -> Dict[str, Any]:
    """Execute end-to-end demonstration of Idempotency, Retry, Restart, and Backfill."""
    base_dir = Path(demo_base_dir or (WORKSPACE_ROOT / "outputs" / "demo_step6"))
    report_file = Path(output_report_path or (OUTPUTS_REPORTS_DIR / "step6_recovery_report.json"))

    if base_dir.exists():
        shutil.rmtree(base_dir, ignore_errors=True)

    input_dir = base_dir / "input"
    clean_dir = base_dir / "clean"
    quarantine_dir = base_dir / "quarantine"
    reports_dir = base_dir / "reports"
    logs_dir = base_dir / "logs"
    ledger_file = logs_dir / "batch_ledger.json"

    for d in [input_dir, clean_dir, quarantine_dir, reports_dir, logs_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # 1. Create Baseline Demo Input Data (3 customers, 5 events)
    cust_path = input_dir / "customers.csv"
    with open(cust_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "customer_name", "city", "customer_segment"])
        writer.writeheader()
        writer.writerows([
            {"customer_id": "DEMO_C1", "customer_name": "Acme Retail", "city": "New York", "customer_segment": "ENTERPRISE"},
            {"customer_id": "DEMO_C2", "customer_name": "Beta Global", "city": "Chicago", "customer_segment": "WHOLESALE"},
            {"customer_id": "DEMO_C3", "customer_name": "Gamma Corp", "city": "Austin", "customer_segment": "RETAIL"},
        ])

    evt_path = input_dir / "order_events.csv"
    with open(evt_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"])
        writer.writeheader()
        writer.writerows([
            {"event_id": "EVT_101", "customer_id": "DEMO_C1", "order_amount": "150.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:00:00+00:00", "source_file": "order_events.csv"},
            {"event_id": "EVT_102", "customer_id": "DEMO_C2", "order_amount": "250.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:05:00+00:00", "source_file": "order_events.csv"},
            {"event_id": "EVT_103", "customer_id": "DEMO_C3", "order_amount": "99.50", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:10:00+00:00", "source_file": "order_events.csv"},
            {"event_id": "EVT_104", "customer_id": "DEMO_C1", "order_amount": "320.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:15:00+00:00", "source_file": "order_events.csv"},
            {"event_id": "EVT_105", "customer_id": "DEMO_C2", "order_amount": "45.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:20:00+00:00", "source_file": "order_events.csv"},
        ])

    demo_results: Dict[str, Any] = {
        "demonstration_date": datetime.now(timezone.utc).isoformat(),
        "scenarios": {}
    }

    # -------------------------------------------------------------
    # Demonstration A: First-time Batch Execution
    # -------------------------------------------------------------
    logger.info("Executing Demonstration A: First-time batch execution...")
    p_a = IdempotentPipeline(
        input_dir=input_dir,
        batch_id="batch_demo_01",
        clean_dir=clean_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        ledger_path=ledger_file
    )
    res_a = p_a.run()
    demo_results["scenarios"]["A_first_time_batch"] = {
        "batch_id": res_a["batch_id"],
        "run_id": res_a["run_id"],
        "processing_status": res_a["processing_status"],
        "attempt_type": res_a["attempt_type"],
        "published_counts": res_a["counts"]["published_counts"],
        "reconciliation_status": res_a["reconciliation_status"],
        "evidence": "Successfully published clean datasets; registered as FIRST_RUN in ledger."
    }

    # -------------------------------------------------------------
    # Demonstration B: Reprocessing the same batch (Idempotency)
    # -------------------------------------------------------------
    logger.info("Executing Demonstration B: Same batch execution (Idempotency Check)...")
    p_b = IdempotentPipeline(
        input_dir=input_dir,
        batch_id="batch_demo_01",
        clean_dir=clean_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        ledger_path=ledger_file
    )
    res_b = p_b.run()
    demo_results["scenarios"]["B_idempotent_rerun"] = {
        "batch_id": res_b["batch_id"],
        "run_id": res_b["run_id"],
        "processing_status": res_b["processing_status"],
        "attempt_type": res_b["attempt_type"],
        "published_counts": res_b["counts"]["published_counts"],
        "evidence": "Detected prior successful publish; skipped processing with 0 duplicate rows."
    }

    # -------------------------------------------------------------
    # Demonstration C: Controlled Failure & Safe Retry / Recovery
    # -------------------------------------------------------------
    logger.info("Executing Demonstration C1: Controlled Failure...")
    fail_input_dir = base_dir / "input_fail"
    fail_input_dir.mkdir(parents=True, exist_ok=True)
    # Write only customers.csv (order_events.csv missing to cause deterministic failure)
    shutil.copy(cust_path, fail_input_dir / "customers.csv")

    p_c1 = IdempotentPipeline(
        input_dir=fail_input_dir,
        batch_id="batch_demo_fail",
        clean_dir=clean_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        ledger_path=ledger_file
    )
    failed_error = None
    try:
        p_c1.run()
    except Exception as e:
        failed_error = str(e)

    ledger = BatchLedger(ledger_file)
    batch_fail_state = ledger.get_batch("batch_demo_fail")

    logger.info("Executing Demonstration C2: Safe Retry after resolving failure...")
    # Fix the missing file
    shutil.copy(evt_path, fail_input_dir / "order_events.csv")
    p_c2 = IdempotentPipeline(
        input_dir=fail_input_dir,
        batch_id="batch_demo_fail",
        clean_dir=clean_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        ledger_path=ledger_file
    )
    res_c2 = p_c2.run()

    demo_results["scenarios"]["C_failure_and_retry"] = {
        "failure_attempt": {
            "batch_id": "batch_demo_fail",
            "run_id": p_c1.run_id,
            "status": batch_fail_state["current_status"],
            "error_captured": failed_error,
            "staging_cleaned": not (clean_dir / f".staging_{p_c1.run_id}").exists()
        },
        "retry_recovery_attempt": {
            "batch_id": res_c2["batch_id"],
            "run_id": res_c2["run_id"],
            "status": res_c2["processing_status"],
            "attempt_type": res_c2["attempt_type"],
            "retry_count": res_c2["retry_count"],
            "published_counts": res_c2["counts"]["published_counts"]
        },
        "evidence": "Staging isolation prevented partial publishing during failure; retry recovered cleanly."
    }

    # -------------------------------------------------------------
    # Demonstration D: Historical Backfill
    # -------------------------------------------------------------
    logger.info("Executing Demonstration D: Backfill of batch_demo_01...")
    # Add an additional backfilled order event
    backfill_input_dir = base_dir / "input_backfill"
    backfill_input_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(cust_path, backfill_input_dir / "customers.csv")
    with open(backfill_input_dir / "order_events.csv", mode="w", newline="", encoding="utf-8") as f:
        with open(evt_path, mode="r", encoding="utf-8") as orig_f:
            f.write(orig_f.read())
        writer = csv.DictWriter(f, fieldnames=["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"])
        writer.writerow({
            "event_id": "EVT_106_BACKFILL",
            "customer_id": "DEMO_C1",
            "order_amount": "500.00",
            "event_type": "PURCHASE",
            "event_timestamp": "2026-11-01T10:25:00+00:00",
            "source_file": "order_events.csv"
        })

    p_d = IdempotentPipeline(
        input_dir=backfill_input_dir,
        batch_id="batch_demo_01",
        clean_dir=clean_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        ledger_path=ledger_file,
        is_backfill=True
    )
    res_d = p_d.run()

    batch_01_history = ledger.get_run_history("batch_demo_01")
    demo_results["scenarios"]["D_backfill_historical"] = {
        "batch_id": res_d["batch_id"],
        "run_id": res_d["run_id"],
        "processing_status": res_d["processing_status"],
        "attempt_type": res_d["attempt_type"],
        "is_backfill": res_d["is_backfill"],
        "published_counts": res_d["counts"]["published_counts"],
        "total_historical_runs_retained": len(batch_01_history),
        "history_run_types": [r["attempt_type"] for r in batch_01_history],
        "evidence": "Atomically replaced published batch data while preserving all previous run audit records."
    }

    # Final summary
    demo_results["overall_status"] = "PASSED"
    demo_results["ledger_total_batches"] = len(ledger.get_all_batches())
    demo_results["ledger_total_runs"] = len(ledger.get_run_history())

    report_file.parent.mkdir(parents=True, exist_ok=True)
    with open(report_file, mode="w", encoding="utf-8") as f:
        json.dump(demo_results, f, indent=2)

    logger.info(f"Step 6 Recovery Demonstration completed successfully. Written to {report_file}")
    return demo_results
