"""Baseline PySpark Workload and Skew Diagnostics for SparkSkewLab.

Implements:
1. Schema-enforced ingestion of validated datasets.
2. Standard, unmitigated SortMerge shuffle join and customer summary aggregation.
3. Controlled execution environment with 8 shuffle partitions, AQE disabled, and broadcast disabled.
4. Mathematical and operational skew diagnosis (key frequency, input partitions, post-shuffle distribution).
5. Explain plan capture and Parquet analytical result publishing.
"""

from collections import Counter
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    desc,
    round as spark_round,
    spark_partition_id,
    sum as spark_sum,
)
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import (
    HOT_CUSTOMER_ID,
    OUTPUTS_BASELINE_DIR,
    OUTPUTS_CLEAN_DIR,
    OUTPUTS_REPORTS_DIR,
)

logger = logging.getLogger(__name__)

# Explicit Spark Schemas matching cleaned output CSVs
CLEAN_CUSTOMER_SPARK_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_name", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("customer_segment", StringType(), nullable=True),
    StructField("source_file", StringType(), nullable=True),
    StructField("batch_id", StringType(), nullable=True),
    StructField("run_id", StringType(), nullable=True),
])

CLEAN_ORDER_EVENT_SPARK_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("order_amount", DecimalType(12, 2), nullable=False),
    StructField("event_type", StringType(), nullable=True),
    StructField("event_timestamp", TimestampType(), nullable=True),
    StructField("source_file", StringType(), nullable=True),
    StructField("batch_id", StringType(), nullable=True),
    StructField("run_id", StringType(), nullable=True),
])


def load_clean_datasets(
    spark: SparkSession,
    clean_dir: Optional[Path] = None
) -> Tuple[DataFrame, DataFrame]:
    """Load cleaned customer and order event datasets with explicit schemas.
    
    Args:
        spark: Active SparkSession.
        clean_dir: Path to directory containing clean CSVs (defaults to outputs/clean).
        
    Returns:
        Tuple of (customers_df, events_df).
    """
    target_clean_dir = clean_dir or OUTPUTS_CLEAN_DIR
    cust_path = target_clean_dir / "clean_customers.csv"
    events_path = target_clean_dir / "clean_order_events.csv"

    if not cust_path.exists():
        raise FileNotFoundError(f"Clean customers CSV not found at: {cust_path}")
    if not events_path.exists():
        raise FileNotFoundError(f"Clean events CSV not found at: {events_path}")

    # Read CSV with header and explicit schema
    customers_df = (
        spark.read
        .option("header", "true")
        .schema(CLEAN_CUSTOMER_SPARK_SCHEMA)
        .csv(str(cust_path))
    )

    events_df = (
        spark.read
        .option("header", "true")
        .schema(CLEAN_ORDER_EVENT_SPARK_SCHEMA)
        .csv(str(events_path))
    )

    logger.info(f"Loaded clean datasets: {customers_df.count():,} customers, {events_df.count():,} events.")
    return customers_df, events_df


def execute_baseline_workload(
    customers_df: DataFrame,
    events_df: DataFrame
) -> DataFrame:
    """Execute unoptimized shuffle join and customer summary aggregations.
    
    Operations:
    1. Inner join on customer_id (enforcing shuffle join via disabled broadcast).
    2. Group by customer_id and customer_name.
    3. Aggregate: order_count, total_amount, average_order_amount.
    """
    # Join on key
    joined_df = events_df.join(customers_df, on="customer_id", how="inner")

    # Group and aggregate
    summary_df = (
        joined_df
        .groupBy("customer_id", "customer_name")
        .agg(
            count("event_id").alias("order_count"),
            spark_round(spark_sum("order_amount"), 2).alias("total_amount"),
            spark_round(avg("order_amount"), 2).alias("average_order_amount")
        )
    )

    return summary_df


def diagnose_key_distribution(
    events_df: DataFrame,
    hot_customer_id: str = HOT_CUSTOMER_ID
) -> Dict[str, Any]:
    """Quantify domain key skew within the transactional dataset."""
    total_events = events_df.count()

    # Calculate top keys
    key_counts_df = (
        events_df.groupBy("customer_id")
        .agg(count("event_id").alias("event_count"))
        .orderBy(desc("event_count"))
    )

    top_rows = key_counts_df.limit(10).collect()

    top_10 = [
        {
            "customer_id": r["customer_id"],
            "event_count": r["event_count"],
            "percentage": round((r["event_count"] / total_events) * 100, 2) if total_events > 0 else 0.0
        }
        for r in top_rows
    ]

    hot_entry = next((item for item in top_10 if item["customer_id"] == hot_customer_id), None)
    hot_count = hot_entry["event_count"] if hot_entry else 0
    hot_percentage = hot_entry["percentage"] if hot_entry else 0.0

    return {
        "total_events": total_events,
        "hot_customer_id": hot_customer_id,
        "hot_customer_event_count": hot_count,
        "hot_customer_percentage": hot_percentage,
        "top_10_customer_frequencies": top_10,
    }


def diagnose_partition_distribution(df: DataFrame) -> Dict[str, Any]:
    """Measure physical record distribution across Spark partition IDs."""
    partition_counts_df = (
        df.withColumn("partition_id", spark_partition_id())
        .groupBy("partition_id")
        .agg(count("*").alias("row_count"))
        .orderBy("partition_id")
    )

    rows = partition_counts_df.collect()
    per_partition = {int(r["partition_id"]): int(r["row_count"]) for r in rows}
    counts = list(per_partition.values())

    return {
        "num_partitions": len(counts),
        "min_rows_in_partition": min(counts) if counts else 0,
        "max_rows_in_partition": max(counts) if counts else 0,
        "partition_record_counts": per_partition,
    }


def capture_execution_plan(df: DataFrame, output_path: Optional[Path] = None) -> str:
    """Capture formatted physical Spark execution plan to string and text file."""
    # Obtain formatted explain plan string using ExplainMode
    explain_mode = df._sc._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
    plan_str = df._jdf.queryExecution().explainString(explain_mode)

    target_file = output_path or (OUTPUTS_REPORTS_DIR / "baseline_execution_plan.txt")
    target_file.parent.mkdir(parents=True, exist_ok=True)
    with open(target_file, mode="w", encoding="utf-8") as f:
        f.write("============================================================\n")
        f.write(" SparkSkewLab Baseline Query Execution Plan (Formatted)\n")
        f.write(" Settings: AQE=False, autoBroadcastJoinThreshold=-1\n")
        f.write("============================================================\n\n")
        f.write(plan_str)

    logger.info(f"Execution plan captured to: {target_file}")
    return plan_str


def save_baseline_results(
    summary_df: DataFrame,
    output_dir: Optional[Path] = None
) -> Path:
    """Persist aggregated customer summary DataFrame to Parquet."""
    target_dir = output_dir or (OUTPUTS_BASELINE_DIR / "customer_summary")
    target_dir.mkdir(parents=True, exist_ok=True)

    # Save to Parquet format (clean overwrite)
    summary_df.write.mode("overwrite").parquet(str(target_dir))
    logger.info(f"Baseline customer summary persisted to: {target_dir}")
    return target_dir
