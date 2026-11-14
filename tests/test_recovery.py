"""Automated tests for idempotency, retry, restart, and backfill behaviors in SparkSkewLab."""

import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import pytest

from src.recovery import BatchLedger, IdempotentPipeline, compute_input_fingerprint


@pytest.fixture
def recovery_env(tmp_path: Path):
    """Isolated environment with input, clean, quarantine, reports, and logs directories."""
    inp = tmp_path / "input"
    clean = tmp_path / "clean"
    quar = tmp_path / "quarantine"
    rep = tmp_path / "reports"
    logs = tmp_path / "logs"

    for d in [inp, clean, quar, rep, logs]:
        d.mkdir(parents=True, exist_ok=True)

    # Populate standard valid input datasets
    cust_file = inp / "customers.csv"
    with open(cust_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "customer_name", "city", "customer_segment"])
        writer.writeheader()
        writer.writerows([
            {"customer_id": "C1", "customer_name": "Corp A", "city": "New York", "customer_segment": "ENTERPRISE"},
            {"customer_id": "C2", "customer_name": "Corp B", "city": "Chicago", "customer_segment": "RETAIL"},
        ])

    events_file = inp / "order_events.csv"
    with open(events_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"])
        writer.writeheader()
        writer.writerows([
            {"event_id": "E1", "customer_id": "C1", "order_amount": "100.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:00:00+00:00", "source_file": "order_events.csv"},
            {"event_id": "E2", "customer_id": "C2", "order_amount": "200.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:05:00+00:00", "source_file": "order_events.csv"},
        ])

    ledger_path = logs / "batch_ledger.json"

    return {
        "input": inp,
        "clean": clean,
        "quarantine": quar,
        "reports": rep,
        "logs": logs,
        "ledger_path": ledger_path
    }


def test_first_time_batch_execution(recovery_env):
    """Scenario 1: First-time batch execution succeeds and creates clean records and ledger history."""
    pipeline = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_001",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    result = pipeline.run()

    assert result["processing_status"] == "SUCCESS"
    assert result["attempt_type"] == "FIRST_RUN"
    assert result["retry_count"] == 0
    assert result["recovery_attempt_count"] == 0
    assert result["reconciliation_status"] == "BALANCED"

    # Verify published clean files exist
    clean_cust = recovery_env["clean"] / "clean_customers.csv"
    clean_evt = recovery_env["clean"] / "clean_order_events.csv"
    assert clean_cust.exists()
    assert clean_evt.exists()

    with open(clean_cust, "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 2
    with open(clean_evt, "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 2

    # Verify ledger state
    ledger = BatchLedger(recovery_env["ledger_path"])
    batch_data = ledger.get_batch("batch_001")
    assert batch_data is not None
    assert batch_data["current_status"] == "SUCCESS"
    assert len(batch_data["runs"]) == 1


def test_same_batch_submitted_twice_is_idempotent(recovery_env):
    """Scenario 2: Reprocessing the same batch skips publishing and avoids duplicate rows."""
    # First execution
    p1 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_002",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res1 = p1.run()
    assert res1["processing_status"] == "SUCCESS"

    clean_cust = recovery_env["clean"] / "clean_customers.csv"
    clean_evt = recovery_env["clean"] / "clean_order_events.csv"
    mtime_cust_before = clean_cust.stat().st_mtime_ns
    mtime_evt_before = clean_evt.stat().st_mtime_ns

    # Second execution of the exact same batch
    p2 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_002",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res2 = p2.run()

    # Must be recognized as an idempotent skip
    assert res2["processing_status"] == "SKIPPED_IDEMPOTENT"
    assert res2["attempt_type"] == "IDEMPOTENT_CHECK"

    # Files must not have been modified or duplicated
    assert clean_cust.stat().st_mtime_ns == mtime_cust_before
    assert clean_evt.stat().st_mtime_ns == mtime_evt_before

    with open(clean_cust, "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 2
    with open(clean_evt, "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 2


def test_same_input_with_same_batch_id(recovery_env):
    """Scenario 3: Same input data submitted with the same batch_id confirms idempotent skip."""
    p1 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_same_input",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res1 = p1.run()
    assert res1["processing_status"] == "SUCCESS"

    p2 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_same_input",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res2 = p2.run()
    assert res2["processing_status"] == "SKIPPED_IDEMPOTENT"
    assert res2["attempt_type"] == "IDEMPOTENT_CHECK"


def test_same_input_with_different_batch_id(recovery_env):
    """Scenario 4: Same input data processed under different batch_id is registered as a distinct batch."""
    p1 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_alpha",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res1 = p1.run()
    assert res1["processing_status"] == "SUCCESS"
    assert res1["batch_id"] == "batch_alpha"

    p2 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_beta",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res2 = p2.run()
    assert res2["processing_status"] == "SUCCESS"
    assert res2["batch_id"] == "batch_beta"
    assert res2["attempt_type"] == "FIRST_RUN"

    # Both batches are tracked in ledger independently
    ledger = BatchLedger(recovery_env["ledger_path"])
    assert ledger.get_batch("batch_alpha")["current_status"] == "SUCCESS"
    assert ledger.get_batch("batch_beta")["current_status"] == "SUCCESS"


def test_failure_before_publishing(recovery_env, monkeypatch):
    """Scenario 5: Failure during ingestion/validation cleans staging and creates no published records."""
    import src.recovery

    def mock_broken_event_ingest(*args, **kwargs):
        raise RuntimeError("Simulated Ingestion Corrupt File Crash")

    monkeypatch.setattr(src.recovery, "ingest_order_events", mock_broken_event_ingest)

    pipeline = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_fail_ingest",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )

    with pytest.raises(RuntimeError, match="Simulated Ingestion Corrupt File Crash"):
        pipeline.run()

    # Staging must be wiped
    staging_dirs = list(recovery_env["clean"].glob(".staging_*"))
    assert len(staging_dirs) == 0

    # Clean published files must NOT exist
    assert not (recovery_env["clean"] / "clean_order_events.csv").exists()

    # Ledger must record FAILED
    ledger = BatchLedger(recovery_env["ledger_path"])
    batch_data = ledger.get_batch("batch_fail_ingest")
    assert batch_data is not None
    assert batch_data["current_status"] == "FAILED"
    assert "Simulated Ingestion Corrupt File Crash" in batch_data["runs"][-1]["error_details"]


def test_failure_during_publishing(recovery_env, monkeypatch):
    """Scenario 6: Failure during atomic commit cleans staging and records failure status."""
    def mock_broken_commit(self, staging_cust, staging_evt):
        raise PermissionError("Simulated Lock / Permission Error During Commit")

    monkeypatch.setattr(IdempotentPipeline, "_commit_staging", mock_broken_commit)

    pipeline = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_fail_commit",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )

    with pytest.raises(PermissionError, match="Simulated Lock / Permission Error"):
        pipeline.run()

    # Staging must be cleanly wiped
    assert len(list(recovery_env["clean"].glob(".staging_*"))) == 0

    # Ledger records FAILED
    ledger = BatchLedger(recovery_env["ledger_path"])
    batch = ledger.get_batch("batch_fail_commit")
    assert batch["current_status"] == "FAILED"


def test_atomic_publication_rollback_when_later_replacement_fails(recovery_env, monkeypatch):
    """Verify all-or-nothing rollback: if replacing clean_order_events fails after clean_customers succeeds, clean_customers is rolled back to pre-commit state."""
    # 1. First publish initial version of files
    p_init = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_rollback_test",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    p_init.run()

    clean_cust = recovery_env["clean"] / "clean_customers.csv"
    clean_evt = recovery_env["clean"] / "clean_order_events.csv"
    orig_cust_content = clean_cust.read_text(encoding="utf-8")
    orig_evt_content = clean_evt.read_text(encoding="utf-8")

    # 2. Modify input slightly for a second run (to verify change would have happened)
    events_file = recovery_env["input"] / "order_events.csv"
    with open(events_file, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"])
        writer.writerow({
            "event_id": "E_NEW_TEST",
            "customer_id": "C1",
            "order_amount": "99.00",
            "event_type": "PURCHASE",
            "event_timestamp": "2026-11-01T10:30:00+00:00",
            "source_file": "order_events.csv"
        })

    # 3. Simulate failure ONLY when replacing clean_order_events.csv
    orig_replace = os.replace

    def mock_replace_conditional(src, dst):
        if "clean_order_events.csv" in str(dst) and not str(src).endswith(".bak"):
            raise OSError("Simulated Disk Full Failure on clean_order_events.csv replacement")
        return orig_replace(src, dst)

    monkeypatch.setattr(os, "replace", mock_replace_conditional)

    p_fail = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_rollback_test",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"],
        is_backfill=True
    )

    with pytest.raises(OSError, match="Simulated Disk Full Failure"):
        p_fail.run()

    # 4. Verify that clean_customers was rolled back to original pre-commit state!
    assert clean_cust.read_text(encoding="utf-8") == orig_cust_content
    assert clean_evt.read_text(encoding="utf-8") == orig_evt_content

    # 5. Verify batch cannot be marked SUCCESS
    ledger = BatchLedger(recovery_env["ledger_path"])
    batch_data = ledger.get_batch("batch_rollback_test")
    assert batch_data["current_status"] == "FAILED"

    # 6. Retry without monkeypatch produces correct outputs without duplicates
    monkeypatch.undo()
    p_retry = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_rollback_test",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"],
        is_backfill=True
    )
    res_retry = p_retry.run()
    assert res_retry["processing_status"] == "SUCCESS"
    with open(clean_evt, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 3
        assert any(r["event_id"] == "E_NEW_TEST" for r in rows)


def test_same_batch_with_changed_input_policy(recovery_env):
    """Verify policy: submitting modified input under same batch_id raises error unless is_backfill or force is set."""
    p1 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_policy_check",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res1 = p1.run()
    assert res1["processing_status"] == "SUCCESS"

    # Modify input
    events_file = recovery_env["input"] / "order_events.csv"
    with open(events_file, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"])
        writer.writerow({
            "event_id": "E_CHANGED_INPUT",
            "customer_id": "C1",
            "order_amount": "50.00",
            "event_type": "PURCHASE",
            "event_timestamp": "2026-11-01T10:40:00+00:00",
            "source_file": "order_events.csv"
        })

    # Submitting same batch_id without is_backfill or force must raise ValueError
    p_changed = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_policy_check",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    with pytest.raises(ValueError, match="already been successfully processed with input fingerprint"):
        p_changed.run()

    # Submitting with is_backfill=True succeeds
    p_backfill = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_policy_check",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"],
        is_backfill=True
    )
    res_backfill = p_backfill.run()
    assert res_backfill["processing_status"] == "SUCCESS"


def test_retry_after_failure(recovery_env, monkeypatch):
    """Scenario 7: Safe retry after failure transitions to SUCCESS and increments retry count."""
    import src.recovery

    # Run 1: Failure
    fail_state = {"should_fail": True}

    orig_validate = src.recovery.validate_order_event

    def mock_validate_with_fail(*args, **kwargs):
        if fail_state["should_fail"]:
            raise ValueError("Transient Network/Validation Timeout")
        return orig_validate(*args, **kwargs)

    monkeypatch.setattr(src.recovery, "validate_order_event", mock_validate_with_fail)

    p1 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_retry_01",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )

    with pytest.raises(ValueError, match="Transient Network/Validation Timeout"):
        p1.run()

    ledger = BatchLedger(recovery_env["ledger_path"])
    assert ledger.get_batch("batch_retry_01")["current_status"] == "FAILED"

    # Run 2: Resolve transient issue and Retry
    fail_state["should_fail"] = False

    p2 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_retry_01",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res2 = p2.run()

    assert res2["processing_status"] == "SUCCESS"
    assert res2["attempt_type"] == "RETRY"
    assert res2["retry_count"] == 1

    # Verify published files exist and are complete
    clean_evt = recovery_env["clean"] / "clean_order_events.csv"
    with open(clean_evt, "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 2


def test_restart_after_interrupted_run(recovery_env):
    """Scenario 8: Process crash leaves IN_PROGRESS; restart marks INTERRUPTED and recovers safely."""
    ledger = BatchLedger(recovery_env["ledger_path"])
    # Seed ledger with an unfinished/crashed run
    now_iso = datetime.now(timezone.utc).isoformat()
    ledger._state["batches"]["batch_crashed_01"] = {
        "batch_id": "batch_crashed_01",
        "current_status": "IN_PROGRESS",
        "latest_run_id": "run_crashed_000",
        "input_hash": "dummy_hash",
        "first_attempt_timestamp": now_iso,
        "last_attempt_timestamp": now_iso,
        "retry_count": 0,
        "recovery_attempt_count": 0,
        "is_backfill": False,
        "runs": [
            {
                "run_id": "run_crashed_000",
                "status": "IN_PROGRESS",
                "attempt_type": "FIRST_RUN",
                "start_time": now_iso,
                "end_time": None,
                "input_hash": "dummy_hash",
                "error_details": None,
            }
        ]
    }
    ledger._save()

    # Now run restart with IdempotentPipeline
    pipeline = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_crashed_01",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    result = pipeline.run()

    assert result["processing_status"] == "SUCCESS"
    assert result["attempt_type"] == "RESTART"
    assert result["recovery_attempt_count"] == 1

    # Check that previous run was recorded as INTERRUPTED and final is SUCCESS
    updated_batch = ledger.get_batch("batch_crashed_01")
    assert updated_batch["current_status"] == "SUCCESS"
    assert updated_batch["runs"][0]["status"] == "INTERRUPTED"
    assert updated_batch["runs"][1]["status"] == "SUCCESS"


def test_backfill_earlier_batch_replaces_atomically(recovery_env):
    """Scenario 9: Backfilling an earlier batch updates published data and preserves run provenance."""
    # Step A: Initial Run of batch_2026_10
    p1 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_2026_10",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    res1 = p1.run()
    assert res1["processing_status"] == "SUCCESS"
    assert res1["counts"]["accepted_counts"]["events"] == 2

    # Step B: Revise input data for backfill (add 1 additional event)
    events_file = recovery_env["input"] / "order_events.csv"
    with open(events_file, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"])
        writer.writerow({
            "event_id": "E3_BACKFILL",
            "customer_id": "C1",
            "order_amount": "300.00",
            "event_type": "PURCHASE",
            "event_timestamp": "2026-11-01T10:10:00+00:00",
            "source_file": "order_events.csv"
        })

    # Step C: Execute backfill
    p2 = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_2026_10",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"],
        is_backfill=True
    )
    res2 = p2.run()

    assert res2["processing_status"] == "SUCCESS"
    assert res2["attempt_type"] == "BACKFILL"
    assert res2["is_backfill"] is True
    assert res2["counts"]["accepted_counts"]["events"] == 3

    # Check published file updated atomically to 3 rows
    clean_evt = recovery_env["clean"] / "clean_order_events.csv"
    with open(clean_evt, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 3
        assert any(r["event_id"] == "E3_BACKFILL" for r in rows)


def test_historical_run_metadata_remains_available(recovery_env):
    """Scenario 10: Complete audit history across all attempts is retained in ledger."""
    ledger = BatchLedger(recovery_env["ledger_path"])

    # Create batch with 3 runs: initial, failed, backfill
    p1 = IdempotentPipeline(input_dir=recovery_env["input"], batch_id="batch_hist", clean_dir=recovery_env["clean"], quarantine_dir=recovery_env["quarantine"], reports_dir=recovery_env["reports"], ledger_path=recovery_env["ledger_path"])
    p1.run()

    p2 = IdempotentPipeline(input_dir=recovery_env["input"], batch_id="batch_hist", clean_dir=recovery_env["clean"], quarantine_dir=recovery_env["quarantine"], reports_dir=recovery_env["reports"], ledger_path=recovery_env["ledger_path"], is_backfill=True)
    p2.run()

    history = ledger.get_run_history("batch_hist")
    assert len(history) == 2
    assert history[0]["attempt_type"] == "FIRST_RUN"
    assert history[1]["attempt_type"] == "BACKFILL"
    for r in history:
        assert "run_id" in r
        assert "start_time" in r
        assert "end_time" in r
        assert "duration_seconds" in r


def test_no_duplicate_records_after_retry_or_recovery(recovery_env):
    """Scenario 11: Exact row balance with zero duplicate rows in published files after multiple attempts."""
    # Run 1
    p1 = IdempotentPipeline(input_dir=recovery_env["input"], batch_id="batch_no_dup", clean_dir=recovery_env["clean"], quarantine_dir=recovery_env["quarantine"], reports_dir=recovery_env["reports"], ledger_path=recovery_env["ledger_path"])
    p1.run()

    # Run 2 (Rerun with force to simulate full re-execution)
    p2 = IdempotentPipeline(input_dir=recovery_env["input"], batch_id="batch_no_dup", clean_dir=recovery_env["clean"], quarantine_dir=recovery_env["quarantine"], reports_dir=recovery_env["reports"], ledger_path=recovery_env["ledger_path"], force=True)
    p2.run()

    clean_cust = recovery_env["clean"] / "clean_customers.csv"
    clean_evt = recovery_env["clean"] / "clean_order_events.csv"

    with open(clean_cust, "r", encoding="utf-8") as f:
        cust_rows = list(csv.DictReader(f))
    with open(clean_evt, "r", encoding="utf-8") as f:
        evt_rows = list(csv.DictReader(f))

    assert len(cust_rows) == 2
    assert len(evt_rows) == 2
    assert len({r["customer_id"] for r in cust_rows}) == 2
    assert len({r["event_id"] for r in evt_rows}) == 2


def test_reconciliation_after_successful_recovery(recovery_env):
    """Scenario 12: Integrity invariant (input == accepted + rejected, published == accepted) holds true."""
    pipeline = IdempotentPipeline(
        input_dir=recovery_env["input"],
        batch_id="batch_recon_test",
        clean_dir=recovery_env["clean"],
        quarantine_dir=recovery_env["quarantine"],
        reports_dir=recovery_env["reports"],
        ledger_path=recovery_env["ledger_path"]
    )
    result = pipeline.run()

    assert result["reconciliation_status"] == "BALANCED"
    counts = result["counts"]

    # Invariant 1: Total input == Accepted + Rejected
    assert counts["input_counts"]["total"] == counts["accepted_counts"]["total"] + counts["rejected_counts"]["total"]

    # Invariant 2: Published == Accepted
    assert counts["published_counts"]["total"] == counts["accepted_counts"]["total"]
