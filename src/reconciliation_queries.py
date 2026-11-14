"""Executable source-to-target reconciliation queries and invariant assertion engine for SparkSkewLab.

Provides independent, dynamic verification across all pipeline tiers:
- Tier 1: Source Ingestion Invariant (Raw = Accepted Clean + Quarantined Rejects)
- Tier 2: Staging-to-Target Conservation (Clean Event Count = Target Summary Order Count)
- Tier 3: Financial Revenue Conservation (Clean Revenue = Target Revenue within Decimal tolerance)
- Tier 4: Dimensional & Referential Integrity (Customer Key Uniqueness, Completeness, No Orphans)
- Tier 5: Baseline vs. Optimized Analytical Parity (Identical Customer-Level Metrics)
- Multi-dataset execution covering both Full Scalable (100k) and Starter Sample (1k) datasets
"""

import csv
from decimal import Decimal, ROUND_HALF_UP
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

# Ensure workspace root is in sys.path
WORKSPACE_DIR = Path(__file__).resolve().parent.parent
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))

import pyarrow.parquet as pq

from src.config import (
    DATA_GENERATED_DIR,
    DATA_SAMPLE_DIR,
    OUTPUTS_BASELINE_DIR,
    OUTPUTS_CLEAN_DIR,
    OUTPUTS_OPTIMIZED_DIR,
    OUTPUTS_QUARANTINE_DIR,
    OUTPUTS_REPORTS_DIR,
    WORKSPACE_ROOT,
)

logger = logging.getLogger("SparkSkewLab.ReconciliationQueries")
DEFAULT_TOLERANCE = Decimal("0.01")


def count_csv_records(file_path: Path) -> int:
    """Dynamically count non-header data rows in a CSV file."""
    p = Path(file_path)
    if not p.exists() or p.stat().st_size == 0:
        return 0
    with open(p, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return 0
        return sum(1 for _ in reader)


def reconcile_source_to_staging(
    raw_customers_path: Path,
    raw_events_path: Path,
    clean_customers_path: Path,
    clean_events_path: Path,
    quar_customers_path: Path,
    quar_events_path: Path,
) -> Dict[str, Any]:
    """Tier 1: Verify Raw input = Clean accepted + Quarantined rejected rows."""
    raw_cust = count_csv_records(raw_customers_path)
    raw_evt = count_csv_records(raw_events_path)

    clean_cust = count_csv_records(clean_customers_path)
    clean_evt = count_csv_records(clean_events_path)

    quar_cust = count_csv_records(quar_customers_path)
    quar_evt = count_csv_records(quar_events_path)

    customers_balanced = (raw_cust == clean_cust + quar_cust)
    events_balanced = (raw_evt == clean_evt + quar_evt)
    tier_status = "PASSED" if (customers_balanced and events_balanced) else "FAILED"

    return {
        "status": tier_status,
        "customers": {
            "raw_count": raw_cust,
            "clean_count": clean_cust,
            "quarantined_count": quar_cust,
            "sum_clean_plus_quar": clean_cust + quar_cust,
            "is_balanced": customers_balanced,
            "difference": raw_cust - (clean_cust + quar_cust),
        },
        "events": {
            "raw_count": raw_evt,
            "clean_count": clean_evt,
            "quarantined_count": quar_evt,
            "sum_clean_plus_quar": clean_evt + quar_evt,
            "is_balanced": events_balanced,
            "difference": raw_evt - (clean_evt + quar_evt),
        },
    }


def reconcile_staging_to_target(
    clean_customers_path: Path,
    clean_events_path: Path,
    target_parquet_dir: Path,
    tolerance: Decimal = DEFAULT_TOLERANCE,
) -> Dict[str, Any]:
    """Tiers 2, 3, and 4: Verify count conservation, revenue conservation, and referential integrity."""
    clean_cust_p = Path(clean_customers_path)
    clean_evt_p = Path(clean_events_path)
    target_p = Path(target_parquet_dir)

    # 1. Inspect Clean Customers
    clean_customer_ids: Set[str] = set()
    customer_name_map: Dict[str, str] = {}
    with open(clean_cust_p, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row["customer_id"]
            clean_customer_ids.add(cid)
            customer_name_map[cid] = row["customer_name"]

    # 2. Inspect Clean Events (Decimal arithmetic for revenue)
    clean_event_count = 0
    clean_revenue = Decimal("0.00")
    referenced_customer_ids: Set[str] = set()
    events_per_customer: Dict[str, int] = {}
    revenue_per_customer: Dict[str, Decimal] = {}

    with open(clean_evt_p, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_event_count += 1
            amt = Decimal(row["order_amount"])
            clean_revenue += amt

            cid = row["customer_id"]
            referenced_customer_ids.add(cid)
            events_per_customer[cid] = events_per_customer.get(cid, 0) + 1
            revenue_per_customer[cid] = revenue_per_customer.get(cid, Decimal("0.00")) + amt

    # Referential integrity check: are all event customer_ids in clean_customers?
    orphan_customer_refs = referenced_customer_ids - clean_customer_ids
    referential_integrity_valid = (len(orphan_customer_refs) == 0)

    # 3. Inspect Target Parquet Summary
    target_table = pq.read_table(str(target_p))
    target_rows = target_table.to_pylist()

    target_customer_count = len(target_rows)
    target_order_count = sum(r["order_count"] for r in target_rows)
    target_revenue = sum(
        Decimal(str(r["total_amount"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        for r in target_rows
    )

    target_customer_ids = [r["customer_id"] for r in target_rows]
    target_unique_keys = set(target_customer_ids)
    has_duplicate_target_keys = (len(target_customer_ids) != len(target_unique_keys))

    missing_in_target = referenced_customer_ids - target_unique_keys
    unexpected_in_target = target_unique_keys - clean_customer_ids

    # Conservation Invariants
    count_conserved = (clean_event_count == target_order_count)
    revenue_difference = abs(clean_revenue - target_revenue)
    revenue_conserved = (revenue_difference <= tolerance)
    keys_complete = (len(missing_in_target) == 0 and len(unexpected_in_target) == 0 and not has_duplicate_target_keys)

    tier_status = "PASSED" if (
        count_conserved and
        revenue_conserved and
        referential_integrity_valid and
        keys_complete
    ) else "FAILED"

    return {
        "status": tier_status,
        "clean_event_count": clean_event_count,
        "target_order_count": target_order_count,
        "order_count_conserved": count_conserved,
        "clean_revenue": str(clean_revenue),
        "target_revenue": str(target_revenue),
        "revenue_difference": str(revenue_difference),
        "revenue_conserved": revenue_conserved,
        "referential_integrity": {
            "clean_customers_count": len(clean_customer_ids),
            "referenced_in_events_count": len(referenced_customer_ids),
            "orphan_customer_refs_count": len(orphan_customer_refs),
            "is_valid": referential_integrity_valid,
        },
        "target_keys_integrity": {
            "target_customer_rows": target_customer_count,
            "target_unique_keys": len(target_unique_keys),
            "has_duplicate_keys": has_duplicate_target_keys,
            "missing_keys_count": len(missing_in_target),
            "unexpected_keys_count": len(unexpected_in_target),
            "is_complete": keys_complete,
        },
    }


def reconcile_baseline_vs_optimized(
    baseline_parquet_dir: Path,
    optimized_parquet_dir: Path,
    tolerance: Decimal = DEFAULT_TOLERANCE,
) -> Dict[str, Any]:
    """Tier 5: Verify customer-level field equality between baseline and optimized Parquet outputs."""
    b_table = pq.read_table(str(baseline_parquet_dir))
    o_table = pq.read_table(str(optimized_parquet_dir))

    b_rows = {r["customer_id"]: r for r in b_table.to_pylist()}
    o_rows = {r["customer_id"]: r for r in o_table.to_pylist()}

    b_keys = set(b_rows.keys())
    o_keys = set(o_rows.keys())

    missing_in_opt = b_keys - o_keys
    unexpected_in_opt = o_keys - b_keys

    name_mismatches = 0
    count_mismatches = 0
    amount_mismatches = 0
    avg_amount_mismatches = 0
    sample_discrepancies = []

    for cid in b_keys.intersection(o_keys):
        b = b_rows[cid]
        o = o_rows[cid]

        if b["customer_name"] != o["customer_name"]:
            name_mismatches += 1

        if b["order_count"] != o["order_count"]:
            count_mismatches += 1

        b_amt = Decimal(str(b["total_amount"]))
        o_amt = Decimal(str(o["total_amount"]))
        if abs(b_amt - o_amt) > tolerance:
            amount_mismatches += 1

        b_avg = Decimal(str(b["average_order_amount"]))
        o_avg = Decimal(str(o["average_order_amount"]))
        if abs(b_avg - o_avg) > tolerance:
            avg_amount_mismatches += 1

        if (
            b["order_count"] != o["order_count"] or
            abs(b_amt - o_amt) > tolerance or
            abs(b_avg - o_avg) > tolerance
        ):
            if len(sample_discrepancies) < 5:
                sample_discrepancies.append({
                    "customer_id": cid,
                    "baseline": {"count": b["order_count"], "total": float(b_amt), "avg": float(b_avg)},
                    "optimized": {"count": o["order_count"], "total": float(o_amt), "avg": float(o_avg)},
                })

    all_keys_matched = (len(missing_in_opt) == 0 and len(unexpected_in_opt) == 0)
    all_values_matched = (
        name_mismatches == 0 and
        count_mismatches == 0 and
        amount_mismatches == 0 and
        avg_amount_mismatches == 0
    )
    status = "PASSED" if (all_keys_matched and all_values_matched) else "FAILED"

    return {
        "status": status,
        "baseline_customer_count": len(b_keys),
        "optimized_customer_count": len(o_keys),
        "missing_keys_count": len(missing_in_opt),
        "unexpected_keys_count": len(unexpected_in_opt),
        "customer_name_mismatch_count": name_mismatches,
        "order_count_mismatch_count": count_mismatches,
        "total_amount_mismatch_count": amount_mismatches,
        "average_order_amount_mismatch_count": avg_amount_mismatches,
        "sample_discrepancies": sample_discrepancies,
    }


def execute_full_reconciliation_audit(
    generated_dir: Optional[Path] = None,
    clean_dir: Optional[Path] = None,
    quarantine_dir: Optional[Path] = None,
    baseline_dir: Optional[Path] = None,
    optimized_dir: Optional[Path] = None,
    sample_dir: Optional[Path] = None,
    sample_clean_dir: Optional[Path] = None,
    sample_quar_dir: Optional[Path] = None,
    sample_baseline_dir: Optional[Path] = None,
    sample_optimized_dir: Optional[Path] = None,
    report_output_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Execute end-to-end multi-tier reconciliation audit across full and sample datasets."""
    gen_d = Path(generated_dir or DATA_GENERATED_DIR)
    cln_d = Path(clean_dir or OUTPUTS_CLEAN_DIR)
    quar_d = Path(quarantine_dir or OUTPUTS_QUARANTINE_DIR)
    base_d = Path(baseline_dir or (OUTPUTS_BASELINE_DIR / "customer_summary"))
    opt_d = Path(optimized_dir or (OUTPUTS_OPTIMIZED_DIR / "customer_summary"))

    smp_d = Path(sample_dir or DATA_SAMPLE_DIR)
    smp_cln_d = Path(sample_clean_dir or (WORKSPACE_ROOT / "outputs" / "sample_clean"))
    smp_quar_d = Path(sample_quar_dir or (WORKSPACE_ROOT / "outputs" / "sample_quarantine"))
    smp_base_d = Path(sample_baseline_dir or (WORKSPACE_ROOT / "outputs" / "sample_baseline" / "customer_summary"))
    smp_opt_d = Path(sample_optimized_dir or (WORKSPACE_ROOT / "outputs" / "sample_optimized" / "customer_summary"))

    rep_out = Path(report_output_path or (OUTPUTS_REPORTS_DIR / "step8_correctness_report.json"))

    audit_report: Dict[str, Any] = {
        "project": "SparkSkewLab",
        "step": "Step 8 — Output Correctness Reconciliation & Assertion Suite",
        "tolerance": str(DEFAULT_TOLERANCE),
        "full_dataset_reconciliation": {},
        "sample_dataset_reconciliation": {},
        "overall_status": "PENDING",
    }

    # =========================================================================
    # PART A: Full Scalable Dataset Reconciliation (100,000 Events)
    # =========================================================================
    logger.info("Auditing Full Scalable Dataset...")
    full_t1 = reconcile_source_to_staging(
        raw_customers_path=gen_d / "customers.csv",
        raw_events_path=gen_d / "order_events.csv",
        clean_customers_path=cln_d / "clean_customers.csv",
        clean_events_path=cln_d / "clean_order_events.csv",
        quar_customers_path=quar_d / "quarantined_customers.csv",
        quar_events_path=quar_d / "quarantined_order_events.csv",
    )

    full_t2_3_4_base = reconcile_staging_to_target(
        clean_customers_path=cln_d / "clean_customers.csv",
        clean_events_path=cln_d / "clean_order_events.csv",
        target_parquet_dir=base_d,
    )

    full_t2_3_4_opt = reconcile_staging_to_target(
        clean_customers_path=cln_d / "clean_customers.csv",
        clean_events_path=cln_d / "clean_order_events.csv",
        target_parquet_dir=opt_d,
    )

    full_t5_equiv = reconcile_baseline_vs_optimized(
        baseline_parquet_dir=base_d,
        optimized_parquet_dir=opt_d,
    )

    full_passed = (
        full_t1["status"] == "PASSED" and
        full_t2_3_4_base["status"] == "PASSED" and
        full_t2_3_4_opt["status"] == "PASSED" and
        full_t5_equiv["status"] == "PASSED"
    )

    audit_report["full_dataset_reconciliation"] = {
        "status": "PASSED" if full_passed else "FAILED",
        "tier1_source_to_staging": full_t1,
        "tier2_staging_to_baseline_target": full_t2_3_4_base,
        "tier2_staging_to_optimized_target": full_t2_3_4_opt,
        "tier5_baseline_vs_optimized_equivalence": full_t5_equiv,
    }

    # =========================================================================
    # PART B: Starter Sample Dataset Reconciliation (1,000 Events)
    # =========================================================================
    logger.info("Auditing Starter Sample Dataset...")
    sample_passed = False
    if smp_cln_d.exists() and smp_base_d.exists() and smp_opt_d.exists():
        sample_t1 = reconcile_source_to_staging(
            raw_customers_path=smp_d / "customers.csv",
            raw_events_path=smp_d / "order_events.csv",
            clean_customers_path=smp_cln_d / "clean_customers.csv",
            clean_events_path=smp_cln_d / "clean_order_events.csv",
            quar_customers_path=smp_quar_d / "quarantined_customers.csv",
            quar_events_path=smp_quar_d / "quarantined_order_events.csv",
        )

        sample_t2_3_4_base = reconcile_staging_to_target(
            clean_customers_path=smp_cln_d / "clean_customers.csv",
            clean_events_path=smp_cln_d / "clean_order_events.csv",
            target_parquet_dir=smp_base_d,
        )

        sample_t2_3_4_opt = reconcile_staging_to_target(
            clean_customers_path=smp_cln_d / "clean_customers.csv",
            clean_events_path=smp_cln_d / "clean_order_events.csv",
            target_parquet_dir=smp_opt_d,
        )

        sample_t5_equiv = reconcile_baseline_vs_optimized(
            baseline_parquet_dir=smp_base_d,
            optimized_parquet_dir=smp_opt_d,
        )

        sample_passed = (
            sample_t1["status"] == "PASSED" and
            sample_t2_3_4_base["status"] == "PASSED" and
            sample_t2_3_4_opt["status"] == "PASSED" and
            sample_t5_equiv["status"] == "PASSED"
        )

        audit_report["sample_dataset_reconciliation"] = {
            "status": "PASSED" if sample_passed else "FAILED",
            "tier1_source_to_staging": sample_t1,
            "tier2_staging_to_baseline_target": sample_t2_3_4_base,
            "tier2_staging_to_optimized_target": sample_t2_3_4_opt,
            "tier5_baseline_vs_optimized_equivalence": sample_t5_equiv,
        }
    else:
        audit_report["sample_dataset_reconciliation"] = {
            "status": "SKIPPED_NOT_STAGED",
            "message": "Sample target directories not yet compiled.",
        }

    overall_status = "PASSED" if (full_passed and sample_passed) else "FAILED"
    audit_report["overall_status"] = overall_status

    # Persist report
    rep_out.parent.mkdir(parents=True, exist_ok=True)
    with open(rep_out, mode="w", encoding="utf-8") as f:
        json.dump(audit_report, f, indent=2)

    logger.info(f"Comprehensive reconciliation report written to {rep_out}. Overall Status: {overall_status}")
    return audit_report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    res = execute_full_reconciliation_audit()
    print("RECONCILIATION AUDIT COMPLETED with status:", res["overall_status"])
