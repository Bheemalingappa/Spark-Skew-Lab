"""Benchmarking framework for baseline and optimized Spark workloads.

Measures:
- Warm-up execution latency
- Multi-iteration execution times (3 measured runs)
- Median runtime calculation
- Operational skew metrics and environment metadata
- Rigorous performance comparison between baseline and salted workloads
- Generation of baseline_report.json, optimized_report.json, and performance_comparison.json
"""

import json
import logging
from pathlib import Path
import statistics
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

from src.baseline import (
    capture_execution_plan,
    diagnose_key_distribution,
    diagnose_partition_distribution,
    execute_baseline_workload,
    load_clean_datasets,
    save_baseline_results,
)
from src.config import (
    HOT_CUSTOMER_ID,
    OUTPUTS_BASELINE_DIR,
    OUTPUTS_CLEAN_DIR,
    OUTPUTS_OPTIMIZED_DIR,
    OUTPUTS_REPORTS_DIR,
)
from src.reconciliation import run_reconciliation
from src.skew_optimization import (
    DEFAULT_SALT_BUCKETS,
    apply_event_salting,
    capture_optimized_execution_plan,
    diagnose_salt_distribution,
    execute_salted_workload,
    identify_hot_keys,
    replicate_customer_dimension,
    save_optimized_results,
)
from src.spark_session import check_spark_environment, get_spark_session, stop_spark_session

logger = logging.getLogger(__name__)


def time_action(action_fn: Callable[[], Any]) -> Tuple[float, Any]:
    """Execute an action, measure elapsed wall-clock time in seconds, and return result."""
    start = time.perf_counter()
    result = action_fn()
    duration = time.perf_counter() - start
    return round(duration, 4), result


def measure_post_shuffle_partitions(joined_df: DataFrame) -> Dict[str, Any]:
    """Measure physical record distribution across Spark partition IDs."""
    from pyspark.sql.functions import count, spark_partition_id

    partition_counts_df = (
        joined_df.withColumn("partition_id", spark_partition_id())
        .groupBy("partition_id")
        .agg(count("*").alias("row_count"))
        .orderBy("partition_id")
    )
    rows = partition_counts_df.collect()
    per_partition = {int(r["partition_id"]): int(r["row_count"]) for r in rows}
    counts = list(per_partition.values())
    total_rows = sum(counts)
    num_parts = len(counts)
    mean_rows = round(total_rows / num_parts, 2) if num_parts > 0 else 0.0
    max_rows = max(counts) if counts else 0
    min_rows = min(counts) if counts else 0
    med_rows = round(statistics.median(counts), 2) if counts else 0.0
    imbalance_mean = round(max_rows / mean_rows, 4) if mean_rows > 0 else 1.0
    imbalance_min = round(max_rows / min_rows, 4) if min_rows > 0 else 1.0
    peak_pct = round((max_rows / total_rows) * 100, 2) if total_rows > 0 else 0.0

    return {
        "num_partitions": num_parts,
        "partition_record_counts": per_partition,
        "total_rows": total_rows,
        "min_rows_in_partition": min_rows,
        "max_rows_in_partition": max_rows,
        "mean_rows_in_partition": mean_rows,
        "median_rows_in_partition": med_rows,
        "imbalance_ratio_max_mean": imbalance_mean,
        "imbalance_ratio_max_min": imbalance_min,
        "peak_load_percentage": peak_pct,
    }


def run_baseline_benchmark(
    spark: Optional[SparkSession] = None,
    clean_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    reports_dir: Optional[Path] = None,
    run_id: str = "baseline_run_001",
    batch_id: str = "batch_prod_001",
    shuffle_partitions: int = 8,
    measured_runs: int = 3
) -> Dict[str, Any]:
    """Execute reproducible benchmarking of the baseline shuffle join workload."""
    clean_path = clean_dir or OUTPUTS_CLEAN_DIR
    out_path = output_dir or (OUTPUTS_BASELINE_DIR / "customer_summary")
    rep_path = reports_dir or OUTPUTS_REPORTS_DIR
    plan_file = rep_path / "baseline_execution_plan.txt"
    report_file = rep_path / "baseline_report.json"

    session = spark or get_spark_session(
        app_name="SparkSkewLab-BaselineBenchmark",
        shuffle_partitions=shuffle_partitions,
        enable_aqe=False,
        auto_broadcast_threshold=-1
    )

    env_info = check_spark_environment()
    customers_df, events_df = load_clean_datasets(session, clean_path)
    input_cust_count = customers_df.count()
    input_events_count = events_df.count()

    key_diag = diagnose_key_distribution(events_df, hot_customer_id=HOT_CUSTOMER_ID)
    summary_df = execute_baseline_workload(customers_df, events_df)
    capture_execution_plan(summary_df, output_path=plan_file)

    logger.info("Executing Baseline Warm-up Run...")
    warmup_time, _ = time_action(lambda: save_baseline_results(summary_df, out_path))
    logger.info(f"Baseline warm-up completed in {warmup_time:.4f}s")

    measured_runtimes: List[float] = []
    for iteration in range(1, measured_runs + 1):
        dur, _ = time_action(lambda: save_baseline_results(summary_df, out_path))
        measured_runtimes.append(dur)
        logger.info(f"Baseline run {iteration} completed in {dur:.4f}s")

    median_runtime = round(statistics.median(measured_runtimes), 4)
    min_runtime = round(min(measured_runtimes), 4) if measured_runtimes else 0.0
    max_runtime = round(max(measured_runtimes), 4) if measured_runtimes else 0.0
    mean_runtime = round(statistics.mean(measured_runtimes), 4) if measured_runtimes else 0.0

    # Capture post-shuffle join partition distribution
    base_joined = events_df.join(customers_df, on="customer_id", how="inner")
    partition_diag = measure_post_shuffle_partitions(base_joined)

    persisted_summary = session.read.parquet(str(out_path))
    output_cust_count = persisted_summary.count()

    report: Dict[str, Any] = {
        "project": "SparkSkewLab",
        "workload_type": "baseline_unmitigated",
        "run_id": run_id,
        "batch_id": batch_id,
        "execution_status": "SUCCESS",
        "dataset_metrics": {
            "input_customer_count": input_cust_count,
            "input_event_count": input_events_count,
            "output_customer_summary_count": output_cust_count,
            "hot_customer_id": HOT_CUSTOMER_ID,
            "hot_customer_event_count": key_diag["hot_customer_event_count"],
            "hot_customer_percentage": key_diag["hot_customer_percentage"],
            "top_10_customer_frequencies": key_diag["top_10_customer_frequencies"]
        },
        "spark_configuration": {
            "shuffle_partitions": shuffle_partitions,
            "adaptive_query_execution_enabled": False,
            "auto_broadcast_join_threshold": -1,
            "sql_timezone": "UTC",
            "master": "local[*]"
        },
        "partition_distribution": partition_diag,
        "benchmark_results": {
            "warmup_runtime_seconds": warmup_time,
            "measured_runtimes_seconds": measured_runtimes,
            "min_runtime_seconds": min_runtime,
            "max_runtime_seconds": max_runtime,
            "mean_runtime_seconds": mean_runtime,
            "median_runtime_seconds": median_runtime,
            "number_of_measured_runs": measured_runs
        },
        "artifact_paths": {
            "customer_summary_parquet": str(out_path.resolve()),
            "execution_plan_txt": str(plan_file.resolve()),
            "baseline_report_json": str(report_file.resolve())
        },
        "environment": {
            "python_version": env_info["python_version"],
            "java_home": env_info["java_home"],
            "pyspark_version": env_info["pyspark_version"],
            "host_os": env_info["os"]
        }
    }

    report_file.parent.mkdir(parents=True, exist_ok=True)
    with open(report_file, mode="w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report


def run_optimized_benchmark(
    spark: Optional[SparkSession] = None,
    clean_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    reports_dir: Optional[Path] = None,
    run_id: str = "optimized_run_001",
    batch_id: str = "batch_prod_001",
    shuffle_partitions: int = 8,
    num_salt_buckets: int = DEFAULT_SALT_BUCKETS,
    measured_runs: int = 3
) -> Dict[str, Any]:
    """Execute reproducible benchmarking of the salted join workload."""
    clean_path = clean_dir or OUTPUTS_CLEAN_DIR
    out_path = output_dir or (OUTPUTS_OPTIMIZED_DIR / "customer_summary")
    rep_path = reports_dir or OUTPUTS_REPORTS_DIR
    plan_file = rep_path / "optimized_execution_plan.txt"
    report_file = rep_path / "optimized_report.json"

    session = spark or get_spark_session(
        app_name="SparkSkewLab-OptimizedBenchmark",
        shuffle_partitions=shuffle_partitions,
        enable_aqe=False,
        auto_broadcast_threshold=-1
    )

    env_info = check_spark_environment()
    customers_df, events_df = load_clean_datasets(session, clean_path)
    input_cust_count = customers_df.count()
    input_events_count = events_df.count()

    hot_keys = identify_hot_keys(events_df, explicit_hot_keys=[HOT_CUSTOMER_ID])
    salted_events = apply_event_salting(events_df, hot_keys, num_salt_buckets)
    salt_diag = diagnose_salt_distribution(salted_events, hot_keys)

    summary_df = execute_salted_workload(
        customers_df,
        events_df,
        hot_keys=hot_keys,
        num_salt_buckets=num_salt_buckets
    )
    capture_optimized_execution_plan(summary_df, output_path=plan_file)

    logger.info("Executing Optimized Warm-up Run...")
    warmup_time, _ = time_action(lambda: save_optimized_results(summary_df, out_path))
    logger.info(f"Optimized warm-up completed in {warmup_time:.4f}s")

    measured_runtimes: List[float] = []
    for iteration in range(1, measured_runs + 1):
        dur, _ = time_action(lambda: save_optimized_results(summary_df, out_path))
        measured_runtimes.append(dur)
        logger.info(f"Optimized run {iteration} completed in {dur:.4f}s")

    median_runtime = round(statistics.median(measured_runtimes), 4)
    min_runtime = round(min(measured_runtimes), 4) if measured_runtimes else 0.0
    max_runtime = round(max(measured_runtimes), 4) if measured_runtimes else 0.0
    mean_runtime = round(statistics.mean(measured_runtimes), 4) if measured_runtimes else 0.0

    # Capture post-shuffle salted join partition distribution
    replicated_cust = replicate_customer_dimension(customers_df, hot_keys, num_salt_buckets)
    salted_joined = salted_events.join(replicated_cust, on=["customer_id", "salt"], how="inner")
    partition_diag = measure_post_shuffle_partitions(salted_joined)

    persisted_summary = session.read.parquet(str(out_path))
    output_cust_count = persisted_summary.count()

    report: Dict[str, Any] = {
        "project": "SparkSkewLab",
        "workload_type": "optimized_salting",
        "run_id": run_id,
        "batch_id": batch_id,
        "execution_status": "SUCCESS",
        "dataset_metrics": {
            "input_customer_count": input_cust_count,
            "input_event_count": input_events_count,
            "output_customer_summary_count": output_cust_count,
            "salt_bucket_count": num_salt_buckets,
            "hot_keys": hot_keys,
            "salt_distribution": salt_diag
        },
        "spark_configuration": {
            "shuffle_partitions": shuffle_partitions,
            "adaptive_query_execution_enabled": False,
            "auto_broadcast_join_threshold": -1,
            "sql_timezone": "UTC",
            "master": "local[*]"
        },
        "partition_distribution": partition_diag,
        "benchmark_results": {
            "warmup_runtime_seconds": warmup_time,
            "measured_runtimes_seconds": measured_runtimes,
            "min_runtime_seconds": min_runtime,
            "max_runtime_seconds": max_runtime,
            "mean_runtime_seconds": mean_runtime,
            "median_runtime_seconds": median_runtime,
            "number_of_measured_runs": measured_runs
        },
        "artifact_paths": {
            "customer_summary_parquet": str(out_path.resolve()),
            "execution_plan_txt": str(plan_file.resolve()),
            "optimized_report_json": str(report_file.resolve())
        },
        "environment": {
            "python_version": env_info["python_version"],
            "java_home": env_info["java_home"],
            "pyspark_version": env_info["pyspark_version"],
            "host_os": env_info["os"]
        }
    }

    report_file.parent.mkdir(parents=True, exist_ok=True)
    with open(report_file, mode="w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report


def run_performance_comparison(
    spark: Optional[SparkSession] = None,
    clean_dir: Optional[Path] = None,
    reports_dir: Optional[Path] = None,
    shuffle_partitions: int = 8,
    num_salt_buckets: int = DEFAULT_SALT_BUCKETS,
    measured_runs: int = 3
) -> Dict[str, Any]:
    """Execute fair, standardized benchmark comparison between baseline and salted workloads."""
    rep_dir = reports_dir or OUTPUTS_REPORTS_DIR
    session = spark or get_spark_session(
        app_name="SparkSkewLab-BenchmarkComparison",
        shuffle_partitions=shuffle_partitions,
        enable_aqe=False,
        auto_broadcast_threshold=-1
    )

    logger.info("Running Baseline Workload Benchmark...")
    baseline_rep = run_baseline_benchmark(
        spark=session,
        clean_dir=clean_dir,
        reports_dir=rep_dir,
        shuffle_partitions=shuffle_partitions,
        measured_runs=measured_runs
    )

    logger.info("Running Optimized (Salted) Workload Benchmark...")
    optimized_rep = run_optimized_benchmark(
        spark=session,
        clean_dir=clean_dir,
        reports_dir=rep_dir,
        shuffle_partitions=shuffle_partitions,
        num_salt_buckets=num_salt_buckets,
        measured_runs=measured_runs
    )

    logger.info("Running Full Customer-Level Correctness Reconciliation...")
    reconciliation_rep = run_reconciliation(spark=session, report_path=rep_dir / "reconciliation_report.json")

    base_median = baseline_rep["benchmark_results"]["median_runtime_seconds"]
    opt_median = optimized_rep["benchmark_results"]["median_runtime_seconds"]

    speedup = round(base_median / opt_median, 4) if opt_median > 0 else 1.0
    runtime_improvement_percent = round(((base_median - opt_median) / base_median) * 100, 2) if base_median > 0 else 0.0

    comparison_report = {
        "project": "SparkSkewLab",
        "benchmark_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "comparison_summary": {
            "baseline_median_runtime_seconds": base_median,
            "optimized_median_runtime_seconds": opt_median,
            "speedup_factor": speedup,
            "runtime_improvement_percent": runtime_improvement_percent,
            "is_faster": opt_median < base_median,
            "reconciliation_status": reconciliation_rep["overall_correctness_status"],
        },
        "baseline_details": baseline_rep["benchmark_results"],
        "optimized_details": optimized_rep["benchmark_results"],
        "baseline_partition_distribution": baseline_rep.get("partition_distribution", {}),
        "optimized_partition_distribution": optimized_rep.get("partition_distribution", {}),
        "salt_distribution": optimized_rep["dataset_metrics"]["salt_distribution"],
        "environment_and_spark_config": {
            "shuffle_partitions": shuffle_partitions,
            "adaptive_query_execution_enabled": False,
            "auto_broadcast_join_threshold": -1,
            "master": "local[*]",
            "num_salt_buckets": num_salt_buckets,
            "pyspark_version": baseline_rep["environment"]["pyspark_version"],
            "python_version": baseline_rep["environment"]["python_version"],
            "host_os": baseline_rep["environment"]["host_os"]
        },
        "reconciliation_summary": {
            "baseline_row_count": reconciliation_rep["baseline_row_count"],
            "optimized_row_count": reconciliation_rep["optimized_row_count"],
            "order_count_mismatches": reconciliation_rep["order_count_mismatches"],
            "amount_mismatches": reconciliation_rep["amount_mismatches"],
            "average_amount_mismatches": reconciliation_rep["average_amount_mismatches"],
        }
    }

    comp_file = rep_dir / "performance_comparison.json"
    with open(comp_file, mode="w", encoding="utf-8") as f:
        json.dump(comparison_report, f, indent=2)

    # Step 9 dedicated comprehensive performance report
    base_part = baseline_rep.get("partition_distribution", {})
    opt_part = optimized_rep.get("partition_distribution", {})
    base_peak = base_part.get("max_rows_in_partition", 0)
    opt_peak = opt_part.get("max_rows_in_partition", 0)
    peak_reduction = round(((base_peak - opt_peak) / base_peak) * 100, 2) if base_peak > 0 else 0.0

    step9_report = {
        "project": "SparkSkewLab",
        "step": "step_9_performance_measurement_and_optimization",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset": {
            "input_customers": baseline_rep["dataset_metrics"]["input_customer_count"],
            "input_events": baseline_rep["dataset_metrics"]["input_event_count"],
            "hot_customer_id": HOT_CUSTOMER_ID,
            "hot_customer_events": baseline_rep["dataset_metrics"]["hot_customer_event_count"],
            "hot_customer_percentage": baseline_rep["dataset_metrics"]["hot_customer_percentage"]
        },
        "spark_configuration": {
            "master": "local[*]",
            "shuffle_partitions": shuffle_partitions,
            "adaptive_query_execution_enabled": False,
            "auto_broadcast_join_threshold": -1,
            "sql_timezone": "UTC"
        },
        "performance_metrics": {
            "baseline": {
                "warmup_seconds": baseline_rep["benchmark_results"]["warmup_runtime_seconds"],
                "measured_runs_seconds": baseline_rep["benchmark_results"]["measured_runtimes_seconds"],
                "min_seconds": baseline_rep["benchmark_results"]["min_runtime_seconds"],
                "max_seconds": baseline_rep["benchmark_results"]["max_runtime_seconds"],
                "mean_seconds": baseline_rep["benchmark_results"]["mean_runtime_seconds"],
                "median_seconds": base_median
            },
            "optimized": {
                "warmup_seconds": optimized_rep["benchmark_results"]["warmup_runtime_seconds"],
                "measured_runs_seconds": optimized_rep["benchmark_results"]["measured_runtimes_seconds"],
                "min_seconds": optimized_rep["benchmark_results"]["min_runtime_seconds"],
                "max_seconds": optimized_rep["benchmark_results"]["max_runtime_seconds"],
                "mean_seconds": optimized_rep["benchmark_results"]["mean_runtime_seconds"],
                "median_seconds": opt_median,
                "salt_bucket_count": num_salt_buckets
            },
            "comparison": {
                "speedup_factor": speedup,
                "runtime_improvement_percent": runtime_improvement_percent,
                "baseline_peak_partition_rows": base_peak,
                "optimized_peak_partition_rows": opt_peak,
                "peak_partition_load_reduction_percent": peak_reduction,
                "is_optimized_faster": opt_median < base_median
            }
        },
        "partition_distribution": {
            "baseline": base_part,
            "optimized": opt_part
        },
        "correctness_reconciliation": {
            "reconciliation_status": reconciliation_rep["overall_correctness_status"],
            "baseline_customer_count": reconciliation_rep["baseline_row_count"],
            "optimized_customer_count": reconciliation_rep["optimized_row_count"],
            "order_count_mismatches": reconciliation_rep["order_count_mismatches"],
            "amount_mismatches": reconciliation_rep["amount_mismatches"],
            "average_amount_mismatches": reconciliation_rep["average_amount_mismatches"]
        }
    }

    step9_file = rep_dir / "step9_performance_report.json"
    with open(step9_file, mode="w", encoding="utf-8") as f:
        json.dump(step9_report, f, indent=2)

    logger.info(f"Performance comparison report written to: {comp_file}")
    logger.info(f"Step 9 performance report written to: {step9_file}")
    return comparison_report


if __name__ == "__main__":
    print("Executing SparkSkewLab Full Baseline vs. Optimized Performance Comparison...")
    comp = run_performance_comparison()
    print("\n================== BENCHMARK COMPARISON RESULTS ==================")
    print(f"Baseline Median Runtime:     {comp['comparison_summary']['baseline_median_runtime_seconds']}s")
    print(f"Optimized Median Runtime:    {comp['comparison_summary']['optimized_median_runtime_seconds']}s")
    print(f"Speedup Factor:              {comp['comparison_summary']['speedup_factor']}x")
    print(f"Runtime Improvement:         {comp['comparison_summary']['runtime_improvement_percent']}%")
    print(f"Reconciliation Status:       {comp['comparison_summary']['reconciliation_status']}")
    print(f"Comparison Report:           {OUTPUTS_REPORTS_DIR / 'performance_comparison.json'}")
    print("==================================================================\n")
    stop_spark_session()
