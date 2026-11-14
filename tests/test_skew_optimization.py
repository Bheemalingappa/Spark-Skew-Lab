"""Unit and integration tests for data skew mitigation via deterministic key salting."""

from decimal import Decimal
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

from src.baseline import (
    CLEAN_CUSTOMER_SPARK_SCHEMA,
    CLEAN_ORDER_EVENT_SPARK_SCHEMA,
    load_clean_datasets,
)
from src.config import HOT_CUSTOMER_ID, OUTPUTS_CLEAN_DIR
from src.skew_optimization import (
    apply_event_salting,
    diagnose_salt_distribution,
    execute_salted_workload,
    identify_hot_keys,
    replicate_customer_dimension,
    save_optimized_results,
)
from src.spark_session import get_spark_session, stop_spark_session


@pytest.fixture(scope="module")
def spark_opt(spark: SparkSession):
    """Shared SparkSession for skew optimization tests."""
    return spark


def test_salt_values_are_deterministic_and_bounded(spark_opt: SparkSession):
    """Verify salt assignment is strictly deterministic and within [0, buckets - 1]."""
    event_data = [
        ("EVT_01", HOT_CUSTOMER_ID, Decimal("100.00"), "PURCHASE", None, "f.csv", "b1", "r1"),
        ("EVT_02", HOT_CUSTOMER_ID, Decimal("150.00"), "PURCHASE", None, "f.csv", "b1", "r1"),
        ("EVT_03", "NORMAL_CUST_1", Decimal("50.00"), "PURCHASE", None, "f.csv", "b1", "r1"),
    ]
    events_df = spark_opt.createDataFrame(event_data, schema=CLEAN_ORDER_EVENT_SPARK_SCHEMA)

    salted_df1 = apply_event_salting(events_df, [HOT_CUSTOMER_ID], num_salt_buckets=8)
    salted_df2 = apply_event_salting(events_df, [HOT_CUSTOMER_ID], num_salt_buckets=8)

    rows1 = salted_df1.collect()
    rows2 = salted_df2.collect()

    assert [(r["event_id"], r["salt"]) for r in rows1] == [(r["event_id"], r["salt"]) for r in rows2]

    # Verify salts stay within [0, 7]
    for r in rows1:
        assert 0 <= r["salt"] < 8
        if r["customer_id"] != HOT_CUSTOMER_ID:
            assert r["salt"] == 0, "Normal customers must have salt = 0"


def test_dimension_replication(spark_opt: SparkSession):
    """Verify hot customers are replicated across buckets while normal customers stay at 1 row."""
    cust_data = [
        (HOT_CUSTOMER_ID, "Hot Corp", "NYC", "ENTERPRISE", "f.csv", "b1", "r1"),
        ("NORMAL_1", "Regular Store", "LA", "RETAIL", "f.csv", "b1", "r1"),
        ("NORMAL_2", "Local Shop", "Austin", "RETAIL", "f.csv", "b1", "r1"),
    ]
    cust_df = spark_opt.createDataFrame(cust_data, schema=CLEAN_CUSTOMER_SPARK_SCHEMA)

    replicated = replicate_customer_dimension(cust_df, [HOT_CUSTOMER_ID], num_salt_buckets=8)
    rows = replicated.collect()

    # Hot customer should have 8 rows with salts 0..7
    hot_rows = [r for r in rows if r["customer_id"] == HOT_CUSTOMER_ID]
    assert len(hot_rows) == 8
    assert sorted([r["salt"] for r in hot_rows]) == list(range(8))

    # Normal customers should each have 1 row with salt 0
    norm1_rows = [r for r in rows if r["customer_id"] == "NORMAL_1"]
    assert len(norm1_rows) == 1
    assert norm1_rows[0]["salt"] == 0

    norm2_rows = [r for r in rows if r["customer_id"] == "NORMAL_2"]
    assert len(norm2_rows) == 1
    assert norm2_rows[0]["salt"] == 0


def test_empty_hot_keys_works(spark_opt: SparkSession):
    """Verify workload executes seamlessly without error when hot_keys list is empty."""
    cust_data = [("C1", "Corp One", "NYC", "RETAIL", "f.csv", "b1", "r1")]
    event_data = [("E1", "C1", Decimal("75.00"), "PURCHASE", None, "f.csv", "b1", "r1")]

    cust_df = spark_opt.createDataFrame(cust_data, schema=CLEAN_CUSTOMER_SPARK_SCHEMA)
    event_df = spark_opt.createDataFrame(event_data, schema=CLEAN_ORDER_EVENT_SPARK_SCHEMA)

    summary_df = execute_salted_workload(cust_df, event_df, hot_keys=[], num_salt_buckets=8)
    rows = summary_df.collect()

    assert len(rows) == 1
    assert rows[0]["customer_id"] == "C1"
    assert rows[0]["order_count"] == 1
    assert float(rows[0]["total_amount"]) == 75.00


def test_salted_join_correctness_and_no_event_loss(spark_opt: SparkSession, tmp_path: Path):
    """Verify that salted workload preserves exact event counts, totals, and Parquet persistence."""
    cust_df, events_df = load_clean_datasets(spark_opt, OUTPUTS_CLEAN_DIR)
    total_input_events = events_df.count()

    summary_df = execute_salted_workload(
        cust_df,
        events_df,
        hot_keys=[HOT_CUSTOMER_ID],
        num_salt_buckets=8
    )
    rows = summary_df.collect()

    assert len(rows) == cust_df.count()

    # Verify no events lost or duplicated
    total_salted_orders = summary_df.select(spark_sum("order_count")).collect()[0][0]
    assert total_salted_orders == total_input_events, (
        f"Salted orders {total_salted_orders} must equal input events {total_input_events}"
    )

    # Verify Parquet export
    out_dir = tmp_path / "test_opt_parquet"
    save_optimized_results(summary_df, out_dir)
    read_back = spark_opt.read.parquet(str(out_dir))
    assert read_back.count() == len(rows)


def test_different_salt_bucket_counts(spark_opt: SparkSession):
    """Verify mathematical correctness across various salt bucket counts (4, 8, 16)."""
    cust_df, events_df = load_clean_datasets(spark_opt, OUTPUTS_CLEAN_DIR)
    expected_event_count = events_df.count()

    for buckets in [4, 8, 16]:
        summary_df = execute_salted_workload(
            cust_df,
            events_df,
            hot_keys=[HOT_CUSTOMER_ID],
            num_salt_buckets=buckets
        )
        total_orders = summary_df.select(spark_sum("order_count")).collect()[0][0]
        assert total_orders == expected_event_count, f"Failed at {buckets} salt buckets"
