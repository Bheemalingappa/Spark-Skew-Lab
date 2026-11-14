"""Automated assertion tests for output correctness, source-to-target reconciliation, and mathematical conservation."""

from decimal import Decimal
from pathlib import Path
import pytest

from src.config import (
    DATA_GENERATED_DIR,
    DATA_SAMPLE_DIR,
    OUTPUTS_BASELINE_DIR,
    OUTPUTS_CLEAN_DIR,
    OUTPUTS_OPTIMIZED_DIR,
    OUTPUTS_QUARANTINE_DIR,
    WORKSPACE_ROOT,
)
from src.reconciliation_queries import (
    execute_full_reconciliation_audit,
    reconcile_baseline_vs_optimized,
    reconcile_source_to_staging,
    reconcile_staging_to_target,
)


def test_tier1_event_conservation_full_dataset():
    """Verify Raw event count == Accepted clean event count + Quarantined rejected event count."""
    res = reconcile_source_to_staging(
        raw_customers_path=DATA_GENERATED_DIR / "customers.csv",
        raw_events_path=DATA_GENERATED_DIR / "order_events.csv",
        clean_customers_path=OUTPUTS_CLEAN_DIR / "clean_customers.csv",
        clean_events_path=OUTPUTS_CLEAN_DIR / "clean_order_events.csv",
        quar_customers_path=OUTPUTS_QUARANTINE_DIR / "quarantined_customers.csv",
        quar_events_path=OUTPUTS_QUARANTINE_DIR / "quarantined_order_events.csv",
    )
    assert res["status"] == "PASSED"
    evt = res["events"]
    assert evt["is_balanced"] is True
    assert evt["difference"] == 0
    assert evt["raw_count"] == evt["clean_count"] + evt["quarantined_count"]


def test_tier1_customer_conservation_full_dataset():
    """Verify Raw customer count == Accepted clean customer count + Quarantined customer count."""
    res = reconcile_source_to_staging(
        raw_customers_path=DATA_GENERATED_DIR / "customers.csv",
        raw_events_path=DATA_GENERATED_DIR / "order_events.csv",
        clean_customers_path=OUTPUTS_CLEAN_DIR / "clean_customers.csv",
        clean_events_path=OUTPUTS_CLEAN_DIR / "clean_order_events.csv",
        quar_customers_path=OUTPUTS_QUARANTINE_DIR / "quarantined_customers.csv",
        quar_events_path=OUTPUTS_QUARANTINE_DIR / "quarantined_order_events.csv",
    )
    cust = res["customers"]
    assert cust["is_balanced"] is True
    assert cust["difference"] == 0
    assert cust["raw_count"] == cust["clean_count"] + cust["quarantined_count"]


def test_tier2_order_count_conservation_full_dataset():
    """Verify sum of customer summary order_count == clean input event count."""
    res = reconcile_staging_to_target(
        clean_customers_path=OUTPUTS_CLEAN_DIR / "clean_customers.csv",
        clean_events_path=OUTPUTS_CLEAN_DIR / "clean_order_events.csv",
        target_parquet_dir=OUTPUTS_OPTIMIZED_DIR / "customer_summary",
    )
    assert res["status"] == "PASSED"
    assert res["order_count_conserved"] is True
    assert res["clean_event_count"] == res["target_order_count"]


def test_tier3_revenue_conservation_full_dataset():
    """Verify clean event revenue == summary total_amount within 0.01 Decimal tolerance."""
    res = reconcile_staging_to_target(
        clean_customers_path=OUTPUTS_CLEAN_DIR / "clean_customers.csv",
        clean_events_path=OUTPUTS_CLEAN_DIR / "clean_order_events.csv",
        target_parquet_dir=OUTPUTS_OPTIMIZED_DIR / "customer_summary",
        tolerance=Decimal("0.01")
    )
    assert res["revenue_conserved"] is True
    assert Decimal(res["revenue_difference"]) <= Decimal("0.01")
    assert res["clean_revenue"] == res["target_revenue"]


def test_tier4_referential_and_key_integrity_full_dataset():
    """Verify customer key completeness, no missing keys, no unexpected keys, and referential integrity."""
    res = reconcile_staging_to_target(
        clean_customers_path=OUTPUTS_CLEAN_DIR / "clean_customers.csv",
        clean_events_path=OUTPUTS_CLEAN_DIR / "clean_order_events.csv",
        target_parquet_dir=OUTPUTS_OPTIMIZED_DIR / "customer_summary",
    )
    ref = res["referential_integrity"]
    assert ref["is_valid"] is True
    assert ref["orphan_customer_refs_count"] == 0

    keys = res["target_keys_integrity"]
    assert keys["is_complete"] is True
    assert keys["has_duplicate_keys"] is False
    assert keys["missing_keys_count"] == 0
    assert keys["unexpected_keys_count"] == 0
    assert keys["target_customer_rows"] == keys["target_unique_keys"]


def test_tier5_baseline_vs_optimized_equivalence_full_dataset():
    """Verify baseline and optimized customer summaries match across all fields with zero discrepancies."""
    res = reconcile_baseline_vs_optimized(
        baseline_parquet_dir=OUTPUTS_BASELINE_DIR / "customer_summary",
        optimized_parquet_dir=OUTPUTS_OPTIMIZED_DIR / "customer_summary",
        tolerance=Decimal("0.01")
    )
    assert res["status"] == "PASSED"
    assert res["missing_keys_count"] == 0
    assert res["unexpected_keys_count"] == 0
    assert res["customer_name_mismatch_count"] == 0
    assert res["order_count_mismatch_count"] == 0
    assert res["total_amount_mismatch_count"] == 0
    assert res["average_order_amount_mismatch_count"] == 0
    assert len(res["sample_discrepancies"]) == 0


def test_starter_sample_dataset_full_reconciliation():
    """Verify that the starter sample dataset satisfies all tiers of reconciliation."""
    smp_d = DATA_SAMPLE_DIR
    smp_cln = WORKSPACE_ROOT / "outputs" / "sample_clean"
    smp_quar = WORKSPACE_ROOT / "outputs" / "sample_quarantine"
    smp_base = WORKSPACE_ROOT / "outputs" / "sample_baseline" / "customer_summary"
    smp_opt = WORKSPACE_ROOT / "outputs" / "sample_optimized" / "customer_summary"

    # Tier 1
    t1 = reconcile_source_to_staging(
        raw_customers_path=smp_d / "customers.csv",
        raw_events_path=smp_d / "order_events.csv",
        clean_customers_path=smp_cln / "clean_customers.csv",
        clean_events_path=smp_cln / "clean_order_events.csv",
        quar_customers_path=smp_quar / "quarantined_customers.csv",
        quar_events_path=smp_quar / "quarantined_order_events.csv",
    )
    assert t1["status"] == "PASSED"
    assert t1["events"]["raw_count"] == t1["events"]["clean_count"] + t1["events"]["quarantined_count"]

    # Tier 2 & 3
    t2_3 = reconcile_staging_to_target(
        clean_customers_path=smp_cln / "clean_customers.csv",
        clean_events_path=smp_cln / "clean_order_events.csv",
        target_parquet_dir=smp_opt,
    )
    assert t2_3["status"] == "PASSED"
    assert t2_3["order_count_conserved"] is True
    assert t2_3["revenue_conserved"] is True

    # Tier 5 Equivalence
    t5 = reconcile_baseline_vs_optimized(
        baseline_parquet_dir=smp_base,
        optimized_parquet_dir=smp_opt,
    )
    assert t5["status"] == "PASSED"


def test_execute_full_reconciliation_audit_report_generation():
    """Verify that execute_full_reconciliation_audit runs end-to-end and outputs valid JSON."""
    report = execute_full_reconciliation_audit()
    assert report["overall_status"] == "PASSED"
    assert report["full_dataset_reconciliation"]["status"] == "PASSED"
    assert report["sample_dataset_reconciliation"]["status"] == "PASSED"

    report_path = WORKSPACE_ROOT / "outputs" / "reports" / "step8_correctness_report.json"
    assert report_path.exists()
    assert report_path.stat().st_size > 0
