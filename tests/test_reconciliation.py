"""Unit tests for correctness reconciliation between baseline and optimized datasets."""

from decimal import Decimal
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.config import OUTPUTS_BASELINE_DIR, OUTPUTS_OPTIMIZED_DIR
from src.reconciliation import reconcile_customer_summaries, run_reconciliation
from src.spark_session import get_spark_session, stop_spark_session

SUMMARY_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("order_count", LongType(), False),
    StructField("total_amount", DecimalType(12, 2), False),
    StructField("average_order_amount", DecimalType(12, 2), False),
])


@pytest.fixture(scope="module")
def spark_recon(spark: SparkSession):
    """Shared SparkSession for reconciliation tests."""
    return spark


def test_reconciliation_identical_datasets_pass(spark_recon: SparkSession):
    """Verify identical datasets evaluate to PASSED with 0 mismatches."""
    data = [
        ("C1", "Corp One", 10, Decimal("1000.00"), Decimal("100.00")),
        ("C2", "Corp Two", 5, Decimal("500.00"), Decimal("100.00")),
    ]
    df1 = spark_recon.createDataFrame(data, schema=SUMMARY_SCHEMA)
    df2 = spark_recon.createDataFrame(data, schema=SUMMARY_SCHEMA)

    report = reconcile_customer_summaries(df1, df2)
    assert report["overall_correctness_status"] == "PASSED"
    assert report["missing_customer_keys_count"] == 0
    assert report["unexpected_customer_keys_count"] == 0
    assert report["order_count_mismatches"] == 0
    assert report["amount_mismatches"] == 0
    assert report["average_amount_mismatches"] == 0


def test_reconciliation_detects_missing_and_unexpected_keys(spark_recon: SparkSession):
    """Verify full outer comparison detects asymmetric keys."""
    data_base = [
        ("C1", "Corp One", 10, Decimal("1000.00"), Decimal("100.00")),
        ("C2", "Corp Two", 5, Decimal("500.00"), Decimal("100.00")),
    ]
    data_opt = [
        ("C1", "Corp One", 10, Decimal("1000.00"), Decimal("100.00")),
        ("C3_EXTRA", "Corp Three", 2, Decimal("200.00"), Decimal("100.00")),
    ]
    df_base = spark_recon.createDataFrame(data_base, schema=SUMMARY_SCHEMA)
    df_opt = spark_recon.createDataFrame(data_opt, schema=SUMMARY_SCHEMA)

    report = reconcile_customer_summaries(df_base, df_opt)
    assert report["overall_correctness_status"] == "FAILED"
    assert "C2" in report["missing_customer_keys"]
    assert "C3_EXTRA" in report["unexpected_customer_keys"]


def test_reconciliation_detects_value_mismatches(spark_recon: SparkSession):
    """Verify mismatched order counts and totals are flagged."""
    data_base = [("C1", "Corp One", 10, Decimal("1000.00"), Decimal("100.00"))]
    data_opt = [("C1", "Corp One", 9, Decimal("900.00"), Decimal("100.00"))]

    df_base = spark_recon.createDataFrame(data_base, schema=SUMMARY_SCHEMA)
    df_opt = spark_recon.createDataFrame(data_opt, schema=SUMMARY_SCHEMA)

    report = reconcile_customer_summaries(df_base, df_opt)
    assert report["overall_correctness_status"] == "FAILED"
    assert report["order_count_mismatches"] == 1
    assert report["amount_mismatches"] == 1
    assert len(report["mismatched_sample_records"]) == 1
