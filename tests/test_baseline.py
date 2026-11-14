"""Unit and integration tests for baseline Spark join and aggregation workload."""

from decimal import Decimal
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

from src.baseline import (
    CLEAN_CUSTOMER_SPARK_SCHEMA,
    CLEAN_ORDER_EVENT_SPARK_SCHEMA,
    diagnose_key_distribution,
    diagnose_partition_distribution,
    execute_baseline_workload,
    load_clean_datasets,
    save_baseline_results,
)
from src.config import HOT_CUSTOMER_ID, OUTPUTS_CLEAN_DIR
from src.spark_session import get_spark_session, stop_spark_session


@pytest.fixture(scope="module")
def spark_baseline(spark: SparkSession):
    """Provides a shared SparkSession for baseline tests."""
    return spark


def test_schema_definitions():
    """Verify explicit schemas match the cleaned column specifications."""
    assert "customer_id" in CLEAN_CUSTOMER_SPARK_SCHEMA.fieldNames()
    assert "customer_name" in CLEAN_CUSTOMER_SPARK_SCHEMA.fieldNames()
    assert "order_amount" in CLEAN_ORDER_EVENT_SPARK_SCHEMA.fieldNames()
    assert "event_id" in CLEAN_ORDER_EVENT_SPARK_SCHEMA.fieldNames()


def test_baseline_aggregation_logic_fixture(spark_baseline: SparkSession):
    """Verify aggregation math (count, sum, avg) on a controlled small fixture."""
    # Create test customers
    cust_data = [
        ("C1", "Alpha Corp", "NYC", "ENTERPRISE", "file.csv", "b1", "r1"),
        ("C2", "Beta Ltd", "Chicago", "RETAIL", "file.csv", "b1", "r1"),
    ]
    cust_df = spark_baseline.createDataFrame(cust_data, schema=CLEAN_CUSTOMER_SPARK_SCHEMA)

    # Create test events
    # C1 has 3 orders: 100.00, 200.00, 300.00 -> count=3, total=600.00, avg=200.00
    # C2 has 1 order: 50.00 -> count=1, total=50.00, avg=50.00
    event_data = [
        ("E1", "C1", Decimal("100.00"), "PURCHASE", None, "file.csv", "b1", "r1"),
        ("E2", "C1", Decimal("200.00"), "PURCHASE", None, "file.csv", "b1", "r1"),
        ("E3", "C1", Decimal("300.00"), "PURCHASE", None, "file.csv", "b1", "r1"),
        ("E4", "C2", Decimal("50.00"), "PURCHASE", None, "file.csv", "b1", "r1"),
    ]
    events_df = spark_baseline.createDataFrame(event_data, schema=CLEAN_ORDER_EVENT_SPARK_SCHEMA)

    summary_df = execute_baseline_workload(cust_df, events_df)
    results = {r["customer_id"]: r for r in summary_df.collect()}

    assert len(results) == 2
    assert results["C1"]["order_count"] == 3
    assert float(results["C1"]["total_amount"]) == 600.00
    assert float(results["C1"]["average_order_amount"]) == 200.00

    assert results["C2"]["order_count"] == 1
    assert float(results["C2"]["total_amount"]) == 50.00
    assert float(results["C2"]["average_order_amount"]) == 50.00


def test_baseline_workload_on_cleaned_dataset(spark_baseline: SparkSession, tmp_path: Path):
    """Verify baseline workload executes against the validated datasets in outputs/clean."""
    cust_df, events_df = load_clean_datasets(spark_baseline, OUTPUTS_CLEAN_DIR)
    total_clean_events = events_df.count()

    # 1. Execute baseline join and aggregation
    summary_df = execute_baseline_workload(cust_df, events_df)
    output_rows = summary_df.collect()

    assert len(output_rows) > 0

    # 2. Reconcile total order counts with input events
    total_output_orders = summary_df.select(spark_sum("order_count")).collect()[0][0]
    assert total_output_orders == total_clean_events, (
        f"Sum of output order_count ({total_output_orders}) must match clean input events ({total_clean_events})"
    )

    # 3. Verify HOT_CUSTOMER presence and skew
    hot_row = next((r for r in output_rows if r["customer_id"] == HOT_CUSTOMER_ID), None)
    assert hot_row is not None, f"{HOT_CUSTOMER_ID} must be present in customer summary"
    assert hot_row["order_count"] > 1000, "HOT_CUSTOMER should have high event count"

    # 4. Key distribution diagnosis
    diag = diagnose_key_distribution(events_df, hot_customer_id=HOT_CUSTOMER_ID)
    assert diag["top_10_customer_frequencies"][0]["customer_id"] == HOT_CUSTOMER_ID
    assert diag["hot_customer_percentage"] > 55.0

    # 5. Persist to Parquet and verify read-back
    out_dir = tmp_path / "test_summary_parquet"
    save_baseline_results(summary_df, out_dir)
    read_back = spark_baseline.read.parquet(str(out_dir))
    assert read_back.count() == len(output_rows)
