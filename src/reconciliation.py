"""Correctness Reconciliation Engine for SparkSkewLab.

Performs customer-level full outer comparison between baseline and optimized datasets:
- customer_id and customer_name integrity
- order_count exact match
- total_amount exact match (within decimal tolerance 0.01)
- average_order_amount match (within decimal tolerance 0.01)
- Detection of missing or unexpected customer keys
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import abs as spark_abs, col, coalesce, count, lit, when

from src.config import (
    OUTPUTS_BASELINE_DIR,
    OUTPUTS_OPTIMIZED_DIR,
    OUTPUTS_REPORTS_DIR,
)

logger = logging.getLogger(__name__)


def reconcile_customer_summaries(
    baseline_df: DataFrame,
    optimized_df: DataFrame,
    amount_tolerance: float = 0.01
) -> Dict[str, Any]:
    """Perform customer-by-customer full outer comparison.
    
    Args:
        baseline_df: Baseline analytical DataFrame.
        optimized_df: Salted optimized analytical DataFrame.
        amount_tolerance: Allowed decimal tolerance for rounding variances (default 0.01).
        
    Returns:
        Structured dictionary audit of reconciliation results.
    """
    b_count = baseline_df.count()
    o_count = optimized_df.count()

    # Rename columns to avoid collision during join
    b_renamed = (
        baseline_df
        .withColumnRenamed("customer_name", "b_customer_name")
        .withColumnRenamed("order_count", "b_order_count")
        .withColumnRenamed("total_amount", "b_total_amount")
        .withColumnRenamed("average_order_amount", "b_average_order_amount")
    )

    o_renamed = (
        optimized_df
        .withColumnRenamed("customer_name", "o_customer_name")
        .withColumnRenamed("order_count", "o_order_count")
        .withColumnRenamed("total_amount", "o_total_amount")
        .withColumnRenamed("average_order_amount", "o_average_order_amount")
    )

    # Full outer join on customer_id
    compared = b_renamed.join(o_renamed, on="customer_id", how="full_outer")

    # Perform a single-pass aggregation to compute all discrepancy counts simultaneously
    metrics_row = compared.select(
        count(when(col("o_customer_name").isNull(), 1)).alias("missing_count"),
        count(when(col("b_customer_name").isNull(), 1)).alias("unexpected_count"),
        count(when((col("b_customer_name").isNotNull() & col("o_customer_name").isNotNull()) & (col("b_customer_name") != col("o_customer_name")), 1)).alias("name_mismatch_count"),
        count(when((col("b_customer_name").isNotNull() & col("o_customer_name").isNotNull()) & (col("b_order_count") != col("o_order_count")), 1)).alias("order_count_mismatch_count"),
        count(when((col("b_customer_name").isNotNull() & col("o_customer_name").isNotNull()) & (spark_abs(col("b_total_amount") - col("o_total_amount")) > amount_tolerance), 1)).alias("amount_mismatch_count"),
        count(when((col("b_customer_name").isNotNull() & col("o_customer_name").isNotNull()) & (spark_abs(col("b_average_order_amount") - col("o_average_order_amount")) > amount_tolerance), 1)).alias("avg_mismatch_count"),
    ).collect()[0]

    missing_count = metrics_row["missing_count"]
    unexpected_count = metrics_row["unexpected_count"]
    name_mismatch_count = metrics_row["name_mismatch_count"]
    order_count_mismatch_count = metrics_row["order_count_mismatch_count"]
    amount_mismatch_count = metrics_row["amount_mismatch_count"]
    avg_mismatch_count = metrics_row["avg_mismatch_count"]

    missing_keys: List[str] = []
    if missing_count > 0:
        missing_in_opt = compared.filter(col("o_customer_name").isNull()).select("customer_id").limit(50).collect()
        missing_keys = [r["customer_id"] for r in missing_in_opt]

    unexpected_keys: List[str] = []
    if unexpected_count > 0:
        unexpected_in_opt = compared.filter(col("b_customer_name").isNull()).select("customer_id").limit(50).collect()
        unexpected_keys = [r["customer_id"] for r in unexpected_in_opt]

    is_correct = (
        b_count == o_count and
        missing_count == 0 and
        unexpected_count == 0 and
        name_mismatch_count == 0 and
        order_count_mismatch_count == 0 and
        amount_mismatch_count == 0 and
        avg_mismatch_count == 0
    )

    status = "PASSED" if is_correct else "FAILED"

    # Sample mismatch records if any
    sample_mismatches: List[Dict[str, Any]] = []
    if not is_correct:
        problem_rows = (
            compared.filter(
                (col("b_customer_name").isNotNull() & col("o_customer_name").isNotNull()) & (
                    (col("b_order_count") != col("o_order_count")) |
                    (spark_abs(col("b_total_amount") - col("o_total_amount")) > amount_tolerance) |
                    (spark_abs(col("b_average_order_amount") - col("o_average_order_amount")) > amount_tolerance)
                )
            )
            .limit(10)
            .collect()
        )
        for r in problem_rows:
            sample_mismatches.append({
                "customer_id": r["customer_id"],
                "baseline_order_count": r["b_order_count"],
                "optimized_order_count": r["o_order_count"],
                "baseline_total_amount": float(r["b_total_amount"]),
                "optimized_total_amount": float(r["o_total_amount"]),
            })

    return {
        "overall_correctness_status": status,
        "baseline_row_count": b_count,
        "optimized_row_count": o_count,
        "missing_customer_keys_count": len(missing_keys),
        "missing_customer_keys": missing_keys,
        "unexpected_customer_keys_count": len(unexpected_keys),
        "unexpected_customer_keys": unexpected_keys,
        "customer_name_mismatch_count": name_mismatch_count,
        "order_count_mismatches": order_count_mismatch_count,
        "amount_mismatches": amount_mismatch_count,
        "average_amount_mismatches": avg_mismatch_count,
        "allowed_amount_tolerance": amount_tolerance,
        "mismatched_sample_records": sample_mismatches,
    }


def run_reconciliation(
    spark: SparkSession,
    baseline_dir: Optional[Path] = None,
    optimized_dir: Optional[Path] = None,
    report_path: Optional[Path] = None
) -> Dict[str, Any]:
    """Load baseline and optimized Parquet datasets, reconcile, and persist report."""
    b_dir = baseline_dir or (OUTPUTS_BASELINE_DIR / "customer_summary")
    o_dir = optimized_dir or (OUTPUTS_OPTIMIZED_DIR / "customer_summary")
    target_report = report_path or (OUTPUTS_REPORTS_DIR / "reconciliation_report.json")

    logger.info(f"Reconciling baseline ({b_dir}) against optimized ({o_dir})...")

    baseline_df = spark.read.parquet(str(b_dir))
    optimized_df = spark.read.parquet(str(o_dir))

    report = reconcile_customer_summaries(baseline_df, optimized_df)
    report["baseline_path"] = str(b_dir.resolve())
    report["optimized_path"] = str(o_dir.resolve())

    target_report.parent.mkdir(parents=True, exist_ok=True)
    with open(target_report, mode="w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Reconciliation completed with status: {report['overall_correctness_status']}. Written to {target_report}")
    return report
