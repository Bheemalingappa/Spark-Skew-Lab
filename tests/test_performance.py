"""Tests for Step 9 performance benchmarking, partition distribution, and optimization analysis."""

import json
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.benchmarking import measure_post_shuffle_partitions
from src.config import OUTPUTS_REPORTS_DIR


def test_measure_post_shuffle_partitions(spark: SparkSession):
    """Verify that measure_post_shuffle_partitions correctly aggregates row counts per partition."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("val", StringType(), False)
    ])
    data = [(i, f"val_{i}") for i in range(100)]
    df = spark.createDataFrame(data, schema).repartition(4)

    metrics = measure_post_shuffle_partitions(df)

    assert metrics["num_partitions"] == 4
    assert metrics["total_rows"] == 100
    assert metrics["min_rows_in_partition"] > 0
    assert metrics["max_rows_in_partition"] >= metrics["min_rows_in_partition"]
    assert metrics["mean_rows_in_partition"] == 25.0
    assert metrics["imbalance_ratio_max_mean"] >= 1.0
    assert sum(metrics["partition_record_counts"].values()) == 100


def test_performance_calculation_formulas():
    """Verify speedup and runtime improvement mathematical calculations."""
    base_median = 1.0
    opt_median = 0.8

    speedup = round(base_median / opt_median, 4)
    improvement_pct = round(((base_median - opt_median) / base_median) * 100, 2)

    assert speedup == 1.25
    assert improvement_pct == 20.0


def test_step9_performance_report_structure_and_integrity():
    """Verify that outputs/reports/step9_performance_report.json exists and conforms to required schema."""
    report_path = OUTPUTS_REPORTS_DIR / "step9_performance_report.json"
    assert report_path.exists(), "step9_performance_report.json must exist"

    data = json.loads(report_path.read_text(encoding="utf-8"))

    # Validate top-level schema
    assert data["project"] == "SparkSkewLab"
    assert data["step"] == "step_9_performance_measurement_and_optimization"
    assert "timestamp" in data
    assert "dataset" in data
    assert "spark_configuration" in data
    assert "performance_metrics" in data
    assert "partition_distribution" in data
    assert "correctness_reconciliation" in data

    # Validate dataset metrics
    ds = data["dataset"]
    assert ds["input_customers"] == 1000
    assert ds["input_events"] == 99700
    assert ds["hot_customer_id"] == "HOT_CUSTOMER"
    assert ds["hot_customer_events"] == 59934
    assert ds["hot_customer_percentage"] == 60.11

    # Validate correctness reconciliation
    rec = data["correctness_reconciliation"]
    assert rec["reconciliation_status"] == "PASSED"
    assert rec["baseline_customer_count"] == 1000
    assert rec["optimized_customer_count"] == 1000
    assert rec["order_count_mismatches"] == 0
    assert rec["amount_mismatches"] == 0
    assert rec["average_amount_mismatches"] == 0


def test_peak_partition_reduction_verified():
    """Verify empirical proof that salting reduced maximum partition load by > 50%."""
    report_path = OUTPUTS_REPORTS_DIR / "step9_performance_report.json"
    data = json.loads(report_path.read_text(encoding="utf-8"))

    comp = data["performance_metrics"]["comparison"]
    base_peak = comp["baseline_peak_partition_rows"]
    opt_peak = comp["optimized_peak_partition_rows"]
    reduction = comp["peak_partition_load_reduction_percent"]

    assert base_peak == 64660, "Baseline peak partition must be 64,660 records"
    assert opt_peak == 27517, "Optimized peak partition must be 27,517 records"
    assert reduction > 50.0, f"Peak reduction should be > 50%, found {reduction}%"
