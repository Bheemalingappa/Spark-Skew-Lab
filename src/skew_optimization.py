"""Spark Data Skew Mitigation using Deterministic Key Salting for SparkSkewLab.

Implements:
1. Dynamic or threshold-based identification of hot customer keys.
2. Deterministic hashing of event_id to assign salt values [0, num_salt_buckets - 1] to hot-key events.
3. Dimension replication for hot keys across all salt buckets, with normal customers pinned to salt 0.
4. Two-phase aggregation:
   - Partial aggregation by (customer_id, customer_name, salt)
   - Final aggregation by (customer_id, customer_name)
5. Physical plan capture and salt bucket distribution diagnostics.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    col,
    count,
    desc,
    explode,
    hash as spark_hash,
    lit,
    pmod,
    round as spark_round,
    spark_partition_id,
    sum as spark_sum,
    when,
)

from src.baseline import (
    CLEAN_CUSTOMER_SPARK_SCHEMA,
    CLEAN_ORDER_EVENT_SPARK_SCHEMA,
    load_clean_datasets,
)
from src.config import (
    HOT_CUSTOMER_ID,
    OUTPUTS_CLEAN_DIR,
    OUTPUTS_OPTIMIZED_DIR,
    OUTPUTS_REPORTS_DIR,
)

logger = logging.getLogger(__name__)

DEFAULT_SALT_BUCKETS: int = 8


def identify_hot_keys(
    events_df: DataFrame,
    threshold_ratio: float = 0.10,
    explicit_hot_keys: Optional[List[str]] = None
) -> List[str]:
    """Identify customer keys exceeding the skew threshold or return explicit keys."""
    if explicit_hot_keys is not None:
        return explicit_hot_keys

    total_events = events_df.count()
    if total_events == 0:
        return []

    threshold_count = int(total_events * threshold_ratio)
    hot_df = (
        events_df.groupBy("customer_id")
        .agg(count("event_id").alias("evt_cnt"))
        .filter(col("evt_cnt") >= threshold_count)
    )

    hot_keys = [r["customer_id"] for r in hot_df.collect()]
    logger.info(f"Identified {len(hot_keys)} hot key(s) exceeding {threshold_ratio * 100}% threshold: {hot_keys}")
    return hot_keys


def apply_event_salting(
    events_df: DataFrame,
    hot_keys: List[str],
    num_salt_buckets: int = DEFAULT_SALT_BUCKETS
) -> DataFrame:
    """Assign deterministic salt values to event records.
    
    Rules:
    - Events with customer_id in hot_keys are salted: pmod(hash(event_id), num_salt_buckets)
    - Events with normal customer_id receive salt 0.
    - If hot_keys is empty, all events receive salt 0.
    """
    if num_salt_buckets < 1:
        raise ValueError("num_salt_buckets must be >= 1")

    if not hot_keys or num_salt_buckets == 1:
        return events_df.withColumn("salt", lit(0))

    # pmod ensures non-negative salt values strictly in [0, num_salt_buckets - 1]
    salt_expr = when(
        col("customer_id").isin(hot_keys),
        pmod(spark_hash(col("event_id")), lit(num_salt_buckets))
    ).otherwise(lit(0))

    return events_df.withColumn("salt", salt_expr)


def replicate_customer_dimension(
    customers_df: DataFrame,
    hot_keys: List[str],
    num_salt_buckets: int = DEFAULT_SALT_BUCKETS
) -> DataFrame:
    """Replicate hot customer dimension records across all salt buckets.
    
    Rules:
    - Hot customers are replicated into num_salt_buckets rows (salts 0 through num_salt_buckets - 1).
    - Normal customers remain exactly 1 row with salt 0.
    - If hot_keys is empty, all customers receive salt 0 without replication.
    """
    if num_salt_buckets < 1:
        raise ValueError("num_salt_buckets must be >= 1")

    if not hot_keys or num_salt_buckets == 1:
        return customers_df.withColumn("salt", lit(0))

    bucket_literals = [lit(i) for i in range(num_salt_buckets)]
    salt_array_expr = when(
        col("customer_id").isin(hot_keys),
        array(bucket_literals)
    ).otherwise(array([lit(0)]))

    replicated_df = (
        customers_df
        .withColumn("salt_array", salt_array_expr)
        .withColumn("salt", explode("salt_array"))
        .drop("salt_array")
    )

    return replicated_df


def execute_salted_workload(
    customers_df: DataFrame,
    events_df: DataFrame,
    hot_keys: Optional[List[str]] = None,
    num_salt_buckets: int = DEFAULT_SALT_BUCKETS
) -> DataFrame:
    """Execute the end-to-end salted join and two-phase aggregation workflow.
    
    Steps:
    1. Assign deterministic salts to hot-key events.
    2. Replicate hot customers across all salt buckets.
    3. Inner join on composite key [customer_id, salt].
    4. Partial aggregation by [customer_id, customer_name, salt].
    5. Final aggregation by [customer_id, customer_name].
    """
    active_hot_keys = [HOT_CUSTOMER_ID] if hot_keys is None else hot_keys

    # 1. Salt events and replicate dimension
    salted_events = apply_event_salting(events_df, active_hot_keys, num_salt_buckets)
    replicated_cust = replicate_customer_dimension(customers_df, active_hot_keys, num_salt_buckets)

    # 2. Join on composite key (customer_id, salt)
    salted_joined = salted_events.join(
        replicated_cust,
        on=["customer_id", "salt"],
        how="inner"
    )

    # 3. Phase 1: Partial Aggregation (distributing hot key across salt buckets)
    partial_agg = (
        salted_joined
        .groupBy("customer_id", "customer_name", "salt")
        .agg(
            count("event_id").alias("partial_order_count"),
            spark_sum("order_amount").alias("partial_total_amount")
        )
    )

    # 4. Phase 2: Final Aggregation (rolling up partial buckets per customer)
    final_summary = (
        partial_agg
        .groupBy("customer_id", "customer_name")
        .agg(
            spark_sum("partial_order_count").alias("order_count"),
            spark_round(spark_sum("partial_total_amount"), 2).alias("total_amount"),
            spark_round(spark_sum("partial_total_amount") / spark_sum("partial_order_count"), 2).alias("average_order_amount")
        )
    )

    return final_summary


def diagnose_salt_distribution(
    salted_events_df: DataFrame,
    hot_keys: List[str]
) -> Dict[str, Any]:
    """Inspect and quantify how effectively events are dispersed across salt buckets."""
    # Filter hot customer events
    hot_events = salted_events_df.filter(col("customer_id").isin(hot_keys))
    bucket_counts_df = (
        hot_events
        .groupBy("salt")
        .agg(count("event_id").alias("event_count"))
        .orderBy("salt")
    )

    bucket_rows = bucket_counts_df.collect()
    per_bucket = {int(r["salt"]): int(r["event_count"]) for r in bucket_rows}
    counts = list(per_bucket.values())

    # Check normal keys
    normal_events = salted_events_df.filter(~col("customer_id").isin(hot_keys))
    normal_salt_count = normal_events.filter(col("salt") != 0).count()

    return {
        "hot_keys": hot_keys,
        "total_hot_events": sum(counts),
        "salt_bucket_counts": per_bucket,
        "min_bucket_events": min(counts) if counts else 0,
        "max_bucket_events": max(counts) if counts else 0,
        "bucket_variance_ratio": round(max(counts) / min(counts), 4) if (counts and min(counts) > 0) else 1.0,
        "normal_events_non_zero_salt_count": normal_salt_count,
    }


def capture_optimized_execution_plan(
    df: DataFrame,
    output_path: Optional[Path] = None
) -> str:
    """Capture formatted physical execution plan of the optimized workload."""
    explain_mode = df._sc._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
    plan_str = df._jdf.queryExecution().explainString(explain_mode)

    target_file = output_path or (OUTPUTS_REPORTS_DIR / "optimized_execution_plan.txt")
    target_file.parent.mkdir(parents=True, exist_ok=True)
    with open(target_file, mode="w", encoding="utf-8") as f:
        f.write("============================================================\n")
        f.write(" SparkSkewLab Optimized Query Execution Plan (Key Salting)\n")
        f.write(" Settings: AQE=False, autoBroadcastJoinThreshold=-1\n")
        f.write(" Composite Join Key: [customer_id, salt]\n")
        f.write("============================================================\n\n")
        f.write(plan_str)

    logger.info(f"Optimized execution plan captured to: {target_file}")
    return plan_str


def save_optimized_results(
    summary_df: DataFrame,
    output_dir: Optional[Path] = None
) -> Path:
    """Persist optimized customer summary DataFrame to Parquet."""
    target_dir = output_dir or (OUTPUTS_OPTIMIZED_DIR / "customer_summary")
    target_dir.mkdir(parents=True, exist_ok=True)

    summary_df.write.mode("overwrite").parquet(str(target_dir))
    logger.info(f"Optimized customer summary persisted to: {target_dir}")
    return target_dir
