"""Unit tests for SparkSkewLab data generator.

Verifies:
1. Determinism and reproducibility under the same random seed.
2. Uniqueness of customer dimension primary keys.
3. Presence of duplicate event IDs.
4. Correctness of HOT_CUSTOMER skew distribution (~60%).
5. Configurable event count and customer count scaling.
6. Presence and accurate labeling of deliberately injected invalid records.
7. Exact synchronization between generated CSV data and the generation report.
"""

from collections import Counter
import csv
import json
from pathlib import Path
import pytest

from src.config import (
    ALL_ANOMALY_TYPES,
    ANOMALY_DUPLICATE_EVENT_ID,
    ANOMALY_INVALID_CUSTOMER_REF,
    ANOMALY_MISSING_CUSTOMER_ID,
    ANOMALY_MISSING_EVENT_ID,
    ANOMALY_MISSING_TIMESTAMP,
    ANOMALY_NEGATIVE_AMOUNT,
    HOT_CUSTOMER_ID,
)
from src.data_generator import (
    generate_customers,
    run_generation,
    write_customers_csv,
    write_order_events_csv,
)


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Provides an isolated directory for test artifact generation."""
    out_dir = tmp_path / "test_data"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def test_customer_ids_are_unique():
    """Verify customer dimension records have strictly unique primary keys."""
    customers = generate_customers(count=500, seed=42)
    assert len(customers) == 500

    ids = [c["customer_id"] for c in customers]
    assert len(ids) == len(set(ids)), "Customer IDs must be strictly unique"
    assert HOT_CUSTOMER_ID in ids, f"{HOT_CUSTOMER_ID} must be present in customer dimension"
    assert customers[0]["customer_id"] == HOT_CUSTOMER_ID


def test_reproducibility_same_seed(temp_output_dir: Path):
    """Verify identical random seeds produce byte-for-byte identical output."""
    dir_a = temp_output_dir / "run_a"
    dir_b = temp_output_dir / "run_b"

    rep_a = run_generation(events_count=1000, customers_count=50, seed=123, output_dir=dir_a)
    rep_b = run_generation(events_count=1000, customers_count=50, seed=123, output_dir=dir_b)

    with open(dir_a / "customers.csv", "r", encoding="utf-8") as fa, open(dir_b / "customers.csv", "r", encoding="utf-8") as fb:
        assert fa.read() == fb.read(), "customers.csv should be identical across identical seeds"

    with open(dir_a / "order_events.csv", "r", encoding="utf-8") as fa, open(dir_b / "order_events.csv", "r", encoding="utf-8") as fb:
        assert fa.read() == fb.read(), "order_events.csv should be identical across identical seeds"

    assert rep_a["total_generated_events"] == rep_b["total_generated_events"]
    assert rep_a["hot_customer_event_count"] == rep_b["hot_customer_event_count"]


def test_configurable_event_counts(temp_output_dir: Path):
    """Verify generator properly scales to arbitrary requested event counts."""
    for count in [150, 750, 2000]:
        test_dir = temp_output_dir / f"count_{count}"
        report = run_generation(events_count=count, customers_count=20, seed=42, output_dir=test_dir)

        # Count lines in CSV (excluding header)
        with open(test_dir / "order_events.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == count
        assert report["total_generated_events"] == count


def test_hot_customer_skew_percentage(temp_output_dir: Path):
    """Verify HOT_CUSTOMER receives approximately 60% of total event volume."""
    total_events = 5000
    expected_ratio = 0.60
    report = run_generation(
        events_count=total_events,
        customers_count=100,
        seed=42,
        skew_ratio=expected_ratio,
        output_dir=temp_output_dir
    )

    hot_count = report["hot_customer_event_count"]
    actual_ratio = hot_count / total_events

    # Tolerating +/- 3% statistical variance for 5,000 samples
    assert abs(actual_ratio - expected_ratio) < 0.03, (
        f"Expected ~{expected_ratio * 100}% hot events, got {actual_ratio * 100:.2f}%"
    )
    assert report["top_10_customer_frequencies"][0]["customer_id"] == HOT_CUSTOMER_ID


def test_invalid_records_present_and_identifiable(temp_output_dir: Path):
    """Verify deliberately injected invalid record types are present in generated CSV."""
    report = run_generation(
        events_count=1000,
        customers_count=50,
        seed=42,
        invalid_count_per_type=3,
        output_dir=temp_output_dir
    )

    csv_path = temp_output_dir / "order_events.csv"
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    # Anomaly 1: Duplicate event IDs
    all_event_ids = [r["event_id"] for r in reader if r["event_id"]]
    id_counts = Counter(all_event_ids)
    duplicates = [eid for eid, cnt in id_counts.items() if cnt > 1]
    assert len(duplicates) >= 1, "Expected duplicate event IDs to be present"

    # Anomaly 2: Missing customer IDs
    missing_cust = [r for r in reader if not r["customer_id"]]
    assert len(missing_cust) == 3, "Expected exactly 3 records with missing customer_id"

    # Anomaly 3: Missing event IDs
    missing_evt = [r for r in reader if not r["event_id"]]
    assert len(missing_evt) == 3, "Expected exactly 3 records with missing event_id"

    # Anomaly 4: Negative order amounts
    negative_amounts = [r for r in reader if float(r["order_amount"]) < 0]
    assert len(negative_amounts) == 3, "Expected exactly 3 records with negative order_amount"

    # Anomaly 5: Invalid customer references
    nonexistent_cust = [r for r in reader if "CUST_NONEXISTENT" in r["customer_id"]]
    assert len(nonexistent_cust) == 3, "Expected exactly 3 records with nonexistent customer reference"

    # Anomaly 6: Missing timestamps
    missing_ts = [r for r in reader if not r["event_timestamp"]]
    assert len(missing_ts) == 3, "Expected exactly 3 records with missing timestamp"

    # Validate total invalid count
    total_expected_invalid = 3 * len(ALL_ANOMALY_TYPES)
    assert report["invalid_record_count"] == total_expected_invalid


def test_generation_report_consistency(temp_output_dir: Path):
    """Verify that generation_report.json matches the actual physical CSV files."""
    report_file = temp_output_dir / "custom_report.json"
    report = run_generation(
        events_count=800,
        customers_count=40,
        seed=42,
        output_dir=temp_output_dir,
        report_path=report_file
    )

    assert report_file.exists(), "Generation report file must exist on disk"
    with open(report_file, "r", encoding="utf-8") as f:
        loaded_report = json.load(f)

    assert loaded_report["total_generated_events"] == 800
    assert loaded_report["random_seed"] == 42
    assert loaded_report["number_of_customers"] == 40
    assert loaded_report["valid_record_count"] + loaded_report["invalid_record_count"] == 800

    # Verify actual row counts in CSV
    with open(temp_output_dir / "order_events.csv", "r", encoding="utf-8") as f:
        events_rows = list(csv.DictReader(f))
    with open(temp_output_dir / "customers.csv", "r", encoding="utf-8") as f:
        cust_rows = list(csv.DictReader(f))

    assert len(events_rows) == 800
    assert len(cust_rows) == 40
