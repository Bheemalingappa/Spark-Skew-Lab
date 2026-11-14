"""Integration tests for the end-to-end data pipeline in SparkSkewLab."""

import csv
import json
from pathlib import Path
import pytest

from src.ingestion import EmptyFileError, MissingFileError
from src.pipeline import DataPipeline


@pytest.fixture
def isolated_dirs(tmp_path: Path):
    """Create isolated input, clean, quarantine, and reports directory structure."""
    inp = tmp_path / "input"
    inp.mkdir(parents=True, exist_ok=True)
    clean = tmp_path / "clean"
    quar = tmp_path / "quarantine"
    rep = tmp_path / "reports"
    return {"input": inp, "clean": clean, "quarantine": quar, "reports": rep}


def create_mock_csv(path: Path, fieldnames: list, rows: list) -> None:
    """Helper to write CSV mock datasets."""
    with open(path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_pipeline_happy_path_reconciliation(isolated_dirs):
    """Verify clean ingestion, partitioning, and exact row balance."""
    inp = isolated_dirs["input"]
    clean = isolated_dirs["clean"]
    quar = isolated_dirs["quarantine"]
    rep = isolated_dirs["reports"]

    # 3 customers: 2 valid, 1 duplicate
    create_mock_csv(
        inp / "customers.csv",
        ["customer_id", "customer_name", "city", "customer_segment"],
        [
            {"customer_id": "C1", "customer_name": "Corp A", "city": "NYC", "customer_segment": "ENTERPRISE"},
            {"customer_id": "C2", "customer_name": "Corp B", "city": "LA", "customer_segment": "VIP"},
            {"customer_id": "C1", "customer_name": "Corp A Dup", "city": "NYC", "customer_segment": "RETAIL"},
        ]
    )

    # 4 events: 2 valid, 1 duplicate event_id, 1 invalid customer ref
    create_mock_csv(
        inp / "order_events.csv",
        ["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"],
        [
            {"event_id": "E1", "customer_id": "C1", "order_amount": "100.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:00:00+00:00", "source_file": "order_events.csv"},
            {"event_id": "E2", "customer_id": "C2", "order_amount": "200.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:01:00+00:00", "source_file": "order_events.csv"},
            {"event_id": "E1", "customer_id": "C1", "order_amount": "150.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:02:00+00:00", "source_file": "order_events.csv"},  # duplicate
            {"event_id": "E3", "customer_id": "C99_GHOST", "order_amount": "50.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:03:00+00:00", "source_file": "order_events.csv"},  # ghost customer
        ]
    )

    pipeline = DataPipeline(
        input_dir=inp,
        batch_id="test_batch_01",
        clean_dir=clean,
        quarantine_dir=quar,
        reports_dir=rep
    )
    result = pipeline.run()

    assert result["status"] == "SUCCESS"
    dq = result["data_quality_report"]

    # Customers reconciliation: 3 input = 2 accepted + 1 rejected
    assert dq["total_input_customers"] == 3
    assert dq["accepted_customers"] == 2
    assert dq["rejected_customers"] == 1
    assert dq["reconciliation"]["customers_balanced"]

    # Events reconciliation: 4 input = 2 accepted + 2 rejected
    assert dq["total_input_events"] == 4
    assert dq["accepted_events"] == 2
    assert dq["rejected_events"] == 2
    assert dq["reconciliation"]["events_balanced"]

    # Verify physical file row counts
    with open(clean / "clean_customers.csv", "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 2
    with open(clean / "clean_order_events.csv", "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 2
    with open(quar / "quarantined_customers.csv", "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 1
    with open(quar / "quarantined_order_events.csv", "r", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 2


def test_quarantine_traceability(isolated_dirs):
    """Verify quarantined records preserve all lineage and diagnostic reasons."""
    inp = isolated_dirs["input"]
    clean = isolated_dirs["clean"]
    quar = isolated_dirs["quarantine"]
    rep = isolated_dirs["reports"]

    create_mock_csv(
        inp / "customers.csv",
        ["customer_id", "customer_name", "city", "customer_segment"],
        [{"customer_id": "C1", "customer_name": "Corp A", "city": "NYC", "customer_segment": "ENTERPRISE"}]
    )

    create_mock_csv(
        inp / "order_events.csv",
        ["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"],
        [
            {"event_id": "E_BAD", "customer_id": "C1", "order_amount": "-50.00", "event_type": "PURCHASE", "event_timestamp": "invalid_date", "source_file": "order_events.csv"}
        ]
    )

    pipeline = DataPipeline(input_dir=inp, batch_id="trace_batch_99", clean_dir=clean, quarantine_dir=quar, reports_dir=rep)
    pipeline.run()

    with open(quar / "quarantined_order_events.csv", "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    row = rows[0]
    assert row["event_id"] == "E_BAD"
    assert row["order_amount"] == "-50.00"
    assert row["batch_id"] == "trace_batch_99"
    assert "negative_or_zero_order_amount" in row["all_rejection_reasons"]
    assert "invalid_or_missing_timestamp" in row["all_rejection_reasons"]


def test_missing_source_file_raises_error(isolated_dirs):
    """Verify missing input file halts pipeline cleanly with clear error report."""
    inp = isolated_dirs["input"]
    # customers.csv is missing
    pipeline = DataPipeline(input_dir=inp, batch_id="b1", clean_dir=isolated_dirs["clean"], quarantine_dir=isolated_dirs["quarantine"], reports_dir=isolated_dirs["reports"])

    with pytest.raises(MissingFileError):
        pipeline.run()

    # Even on failure, run_report should be generated with FAILED status
    run_rep_path = isolated_dirs["reports"] / "run_report.json"
    assert run_rep_path.exists()
    with open(run_rep_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["status"] == "FAILED"
    assert "not found" in data["error_details"]


def test_empty_input_file_raises_error(isolated_dirs):
    """Verify zero-byte or header-only file raises EmptyFileError."""
    inp = isolated_dirs["input"]
    (inp / "customers.csv").touch()  # 0 bytes

    pipeline = DataPipeline(input_dir=inp, batch_id="b1", clean_dir=isolated_dirs["clean"], quarantine_dir=isolated_dirs["quarantine"], reports_dir=isolated_dirs["reports"])
    with pytest.raises(EmptyFileError):
        pipeline.run()


def test_pipeline_idempotency_rerun(isolated_dirs):
    """Verify executing pipeline multiple times on identical input yields deterministic output without duplicate records."""
    inp = isolated_dirs["input"]
    clean = isolated_dirs["clean"]
    quar = isolated_dirs["quarantine"]
    rep = isolated_dirs["reports"]

    create_mock_csv(
        inp / "customers.csv",
        ["customer_id", "customer_name", "city", "customer_segment"],
        [{"customer_id": "C1", "customer_name": "Corp A", "city": "NYC", "customer_segment": "ENTERPRISE"}]
    )
    create_mock_csv(
        inp / "order_events.csv",
        ["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"],
        [{"event_id": "E1", "customer_id": "C1", "order_amount": "100.00", "event_type": "PURCHASE", "event_timestamp": "2026-11-01T10:00:00+00:00", "source_file": "order_events.csv"}]
    )

    # First run
    p1 = DataPipeline(input_dir=inp, batch_id="run1", run_id="fixed_run_01", clean_dir=clean, quarantine_dir=quar, reports_dir=rep)
    res1 = p1.run()

    with open(clean / "clean_order_events.csv", "r", encoding="utf-8") as f:
        content_run1 = f.read()

    # Second run with same parameters (rerun)
    p2 = DataPipeline(input_dir=inp, batch_id="run1", run_id="fixed_run_01", clean_dir=clean, quarantine_dir=quar, reports_dir=rep)
    res2 = p2.run()

    with open(clean / "clean_order_events.csv", "r", encoding="utf-8") as f:
        content_run2 = f.read()

    assert content_run1 == content_run2, "Pipeline must produce identical outputs on rerun without accumulative state"
    assert res1["data_quality_report"]["accepted_events"] == res2["data_quality_report"]["accepted_events"] == 1
