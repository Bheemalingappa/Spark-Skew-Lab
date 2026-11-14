"""Reproducible synthetic data generator for SparkSkewLab.

Generates:
1. customers.csv - Customer dimension with unique primary keys and a designated HOT_CUSTOMER.
2. order_events.csv - High-volume transactional event dataset featuring controlled key skew,
   deterministic ISO-8601 timestamps, and deliberately injected data-quality anomalies.
3. generation_report.json - Operational and statistical audit metadata for verification.
"""

from collections import Counter
from datetime import datetime, timezone, timedelta
import argparse
import csv
import json
import os
from pathlib import Path
import random
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

from src.config import (
    ALL_ANOMALY_TYPES,
    ANOMALY_DUPLICATE_EVENT_ID,
    ANOMALY_INVALID_CUSTOMER_REF,
    ANOMALY_MISSING_CUSTOMER_ID,
    ANOMALY_MISSING_EVENT_ID,
    ANOMALY_MISSING_TIMESTAMP,
    ANOMALY_NEGATIVE_AMOUNT,
    CITIES,
    CUSTOMER_SEGMENTS,
    DATA_GENERATED_DIR,
    DATA_SAMPLE_DIR,
    DEFAULT_CUSTOMERS_COUNT,
    DEFAULT_EVENTS_COUNT,
    DEFAULT_RANDOM_SEED,
    DEFAULT_SKEW_RATIO,
    EVENT_TYPES,
    HOT_CUSTOMER_ID,
    OUTPUTS_REPORTS_DIR,
)

# Deterministic name components for synthetic generation
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker"
]


def generate_customers(
    count: int = DEFAULT_CUSTOMERS_COUNT,
    seed: int = DEFAULT_RANDOM_SEED,
    hot_customer_id: str = HOT_CUSTOMER_ID
) -> List[Dict[str, str]]:
    """Generate deterministic customer dimension records.
    
    Ensures:
    - Exactly 1 customer has id == hot_customer_id
    - All customer_id values are strictly unique
    - No invalid or null records in dimension table
    """
    if count < 1:
        raise ValueError("Customer count must be at least 1")

    rng = random.Random(seed)
    customers: List[Dict[str, str]] = []

    # First entry: the anchor hot customer
    customers.append({
        "customer_id": hot_customer_id,
        "customer_name": "MegaRetail Global Enterprises",
        "city": "New York",
        "customer_segment": "ENTERPRISE",
    })

    used_ids: Set[str] = {hot_customer_id}

    for i in range(1, count):
        cust_id = f"CUST_{i:06d}"
        if cust_id in used_ids:
            raise ValueError(f"Duplicate customer ID generated: {cust_id}")
        used_ids.add(cust_id)

        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)
        city = rng.choice(CITIES)
        segment = rng.choice(CUSTOMER_SEGMENTS)

        customers.append({
            "customer_id": cust_id,
            "customer_name": f"{first_name} {last_name}",
            "city": city,
            "customer_segment": segment,
        })

    return customers


def write_customers_csv(customers: List[Dict[str, str]], filepath: Path) -> int:
    """Write customer dimension records to CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["customer_id", "customer_name", "city", "customer_segment"]

    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)

    return len(customers)


def compute_anomaly_schedule(
    total_events: int,
    invalid_count_per_type: Optional[int] = None,
    seed: int = DEFAULT_RANDOM_SEED
) -> Dict[int, str]:
    """Deterministically assign row indices (0-indexed) to specific anomaly types.
    
    Ensures:
    - Anomalies do not overlap on the same row.
    - Anomalies start after row index 20 so duplicate event IDs have prior IDs to reference.
    - Count per anomaly type is predictable and strictly recorded.
    """
    if invalid_count_per_type is None:
        # Default ~0.05% per anomaly type (approx 50 per type in 100k events, min 2)
        invalid_count_per_type = max(2, int(total_events * 0.0005))

    total_anomalies_needed = invalid_count_per_type * len(ALL_ANOMALY_TYPES)
    if total_anomalies_needed >= total_events:
        raise ValueError(
            f"Requested {total_anomalies_needed} anomalies but total events is {total_events}."
        )

    rng = random.Random(seed + 999)
    # Available indices after warm-up row 20
    candidate_indices = list(range(20, total_events))
    selected_indices = rng.sample(candidate_indices, total_anomalies_needed)

    schedule: Dict[int, str] = {}
    idx_pointer = 0
    for anomaly_type in ALL_ANOMALY_TYPES:
        for _ in range(invalid_count_per_type):
            target_row = selected_indices[idx_pointer]
            schedule[target_row] = anomaly_type
            idx_pointer += 1

    return schedule


def generate_order_events(
    total_events: int = DEFAULT_EVENTS_COUNT,
    customer_ids: Optional[List[str]] = None,
    hot_customer_id: str = HOT_CUSTOMER_ID,
    skew_ratio: float = DEFAULT_SKEW_RATIO,
    invalid_count_per_type: Optional[int] = None,
    seed: int = DEFAULT_RANDOM_SEED,
    source_filename: str = "order_events.csv"
) -> Generator[Tuple[Dict[str, Any], Optional[str]], None, None]:
    """Streaming generator yielding (record_dict, anomaly_type_or_none) tuples.
    
    Allows memory-efficient streaming of 100,000+ records directly to disk.
    """
    if customer_ids is None:
        customer_ids = [hot_customer_id] + [f"CUST_{i:06d}" for i in range(1, DEFAULT_CUSTOMERS_COUNT)]

    normal_customer_ids = [cid for cid in customer_ids if cid != hot_customer_id]
    if not normal_customer_ids:
        raise ValueError("Must have at least one normal customer in addition to HOT_CUSTOMER")

    rng = random.Random(seed)
    anomaly_schedule = compute_anomaly_schedule(
        total_events=total_events,
        invalid_count_per_type=invalid_count_per_type,
        seed=seed
    )

    base_timestamp = datetime(2026, 11, 1, 8, 0, 0, tzinfo=timezone.utc)
    recent_valid_event_ids: List[str] = []

    for row_idx in range(total_events):
        standard_event_id = f"EVT_{row_idx + 1:08d}"
        anomaly = anomaly_schedule.get(row_idx)

        # Baseline timestamp progression
        event_time = base_timestamp + timedelta(
            seconds=row_idx * 2 + rng.randint(0, 10)
        )
        timestamp_str = event_time.isoformat()

        # Decide customer (with skew)
        if rng.random() < skew_ratio:
            assigned_customer = hot_customer_id
        else:
            assigned_customer = rng.choice(normal_customer_ids)

        order_amount = f"{round(rng.uniform(10.0, 1250.0), 2):.2f}"
        event_type = rng.choices(EVENT_TYPES, weights=[0.70, 0.15, 0.10, 0.05])[0]

        event_id = standard_event_id

        # Apply anomaly if scheduled for this row
        if anomaly == ANOMALY_DUPLICATE_EVENT_ID:
            # Pick a previously generated valid event ID
            event_id = rng.choice(recent_valid_event_ids) if recent_valid_event_ids else "EVT_DUPLICATE_00000001"
        elif anomaly == ANOMALY_MISSING_CUSTOMER_ID:
            assigned_customer = ""
        elif anomaly == ANOMALY_MISSING_EVENT_ID:
            event_id = ""
        elif anomaly == ANOMALY_NEGATIVE_AMOUNT:
            order_amount = f"-{round(rng.uniform(15.0, 450.0), 2):.2f}"
        elif anomaly == ANOMALY_INVALID_CUSTOMER_REF:
            assigned_customer = f"CUST_NONEXISTENT_{rng.randint(900000, 999999)}"
        elif anomaly == ANOMALY_MISSING_TIMESTAMP:
            timestamp_str = ""

        # Keep a buffer of recent valid IDs for duplicate creation
        if not anomaly and len(recent_valid_event_ids) < 500:
            recent_valid_event_ids.append(standard_event_id)

        record = {
            "event_id": event_id,
            "customer_id": assigned_customer,
            "order_amount": order_amount,
            "event_type": event_type,
            "event_timestamp": timestamp_str,
            "source_file": source_filename,
        }

        yield record, anomaly


def write_order_events_csv(
    total_events: int,
    output_filepath: Path,
    customer_ids: List[str],
    hot_customer_id: str = HOT_CUSTOMER_ID,
    skew_ratio: float = DEFAULT_SKEW_RATIO,
    invalid_count_per_type: Optional[int] = None,
    seed: int = DEFAULT_RANDOM_SEED
) -> Dict[str, Any]:
    """Stream order events to CSV and collect generation statistics."""
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"]

    customer_counter: Counter = Counter()
    anomaly_counter: Counter = Counter()
    total_written = 0
    valid_count = 0

    with open(output_filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for record, anomaly in generate_order_events(
            total_events=total_events,
            customer_ids=customer_ids,
            hot_customer_id=hot_customer_id,
            skew_ratio=skew_ratio,
            invalid_count_per_type=invalid_count_per_type,
            seed=seed,
            source_filename=output_filepath.name
        ):
            writer.writerow(record)
            total_written += 1
            if record["customer_id"]:
                customer_counter[record["customer_id"]] += 1

            if anomaly:
                anomaly_counter[anomaly] += 1
            else:
                valid_count += 1

    hot_count = customer_counter.get(hot_customer_id, 0)
    hot_percentage = round((hot_count / total_written) * 100, 2) if total_written > 0 else 0.0

    # Top 10 customer key frequencies
    top_10 = [
        {
            "customer_id": cid,
            "event_count": count,
            "percentage": round((count / total_written) * 100, 2)
        }
        for cid, count in customer_counter.most_common(10)
    ]

    return {
        "total_events": total_written,
        "valid_record_count": valid_count,
        "invalid_record_count": sum(anomaly_counter.values()),
        "invalid_records_breakdown": dict(anomaly_counter),
        "hot_customer_event_count": hot_count,
        "hot_customer_percentage": hot_percentage,
        "top_10_customer_frequencies": top_10,
    }


def run_generation(
    events_count: int = DEFAULT_EVENTS_COUNT,
    customers_count: int = DEFAULT_CUSTOMERS_COUNT,
    seed: int = DEFAULT_RANDOM_SEED,
    skew_ratio: float = DEFAULT_SKEW_RATIO,
    invalid_count_per_type: Optional[int] = None,
    output_dir: Optional[Path] = None,
    report_path: Optional[Path] = None
) -> Dict[str, Any]:
    """Execute complete dataset generation and write audit report."""
    target_output_dir = output_dir or (
        DATA_SAMPLE_DIR if events_count <= 5000 else DATA_GENERATED_DIR
    )
    target_report_path = report_path or (OUTPUTS_REPORTS_DIR / "generation_report.json")

    customers_file = target_output_dir / "customers.csv"
    order_events_file = target_output_dir / "order_events.csv"

    # Step 1: Generate & write customer dimension
    customers = generate_customers(count=customers_count, seed=seed)
    write_customers_csv(customers, customers_file)
    customer_ids = [c["customer_id"] for c in customers]

    # Step 2: Stream order events
    stats = write_order_events_csv(
        total_events=events_count,
        output_filepath=order_events_file,
        customer_ids=customer_ids,
        hot_customer_id=HOT_CUSTOMER_ID,
        skew_ratio=skew_ratio,
        invalid_count_per_type=invalid_count_per_type,
        seed=seed
    )

    # Step 3: Assemble generation report
    report: Dict[str, Any] = {
        "project": "SparkSkewLab",
        "random_seed": seed,
        "number_of_customers": customers_count,
        "total_generated_events": stats["total_events"],
        "valid_record_count": stats["valid_record_count"],
        "invalid_record_count": stats["invalid_record_count"],
        "invalid_records_breakdown": stats["invalid_records_breakdown"],
        "configured_skew_ratio": skew_ratio,
        "hot_customer_id": HOT_CUSTOMER_ID,
        "hot_customer_event_count": stats["hot_customer_event_count"],
        "hot_customer_percentage": stats["hot_customer_percentage"],
        "top_10_customer_frequencies": stats["top_10_customer_frequencies"],
        "output_file_paths": {
            "customers_csv": str(customers_file.resolve()),
            "order_events_csv": str(order_events_file.resolve()),
            "generation_report_json": str(target_report_path.resolve())
        },
        "generation_timestamp": datetime.now(timezone.utc).isoformat()
    }

    target_report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_report_path, mode="w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report


def main() -> None:
    """CLI entry point for data generation."""
    parser = argparse.ArgumentParser(
        description="SparkSkewLab Reproducible Synthetic Data Generator"
    )
    parser.add_argument(
        "--events",
        type=int,
        default=DEFAULT_EVENTS_COUNT,
        help=f"Total order events to generate (default: {DEFAULT_EVENTS_COUNT})"
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=DEFAULT_CUSTOMERS_COUNT,
        help=f"Number of customer dimension records (default: {DEFAULT_CUSTOMERS_COUNT})"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_RANDOM_SEED,
        help=f"Random seed for deterministic generation (default: {DEFAULT_RANDOM_SEED})"
    )
    parser.add_argument(
        "--skew-ratio",
        type=float,
        default=DEFAULT_SKEW_RATIO,
        help=f"Fraction of events assigned to HOT_CUSTOMER (default: {DEFAULT_SKEW_RATIO})"
    )
    parser.add_argument(
        "--invalid-count-per-type",
        type=int,
        default=None,
        help="Explicit number of invalid records to inject per anomaly type (default: auto proportional)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Target directory for generated CSV files (default: data/sample if <= 5000 else data/generated)"
    )
    parser.add_argument(
        "--report-path",
        type=str,
        default=None,
        help="Path for generation_report.json (default: outputs/reports/generation_report.json)"
    )

    args = parser.parse_args()

    out_dir = Path(args.output_dir) if args.output_dir else None
    rep_path = Path(args.report_path) if args.report_path else None

    print(f"Generating synthetic dataset: {args.events:,} events, {args.customers:,} customers (seed={args.seed})...")
    report = run_generation(
        events_count=args.events,
        customers_count=args.customers,
        seed=args.seed,
        skew_ratio=args.skew_ratio,
        invalid_count_per_type=args.invalid_count_per_type,
        output_dir=out_dir,
        report_path=rep_path
    )

    print("\nGeneration Completed Successfully!")
    print(f"Total Events: {report['total_generated_events']:,}")
    print(f"Valid Records: {report['valid_record_count']:,}")
    print(f"Invalid Records: {report['invalid_record_count']:,}")
    print(f"HOT_CUSTOMER Events: {report['hot_customer_event_count']:,} ({report['hot_customer_percentage']}%)")
    print("\nTop 5 Customer Frequencies:")
    for rank, entry in enumerate(report["top_10_customer_frequencies"][:5], start=1):
        print(f"  {rank}. {entry['customer_id']}: {entry['event_count']:,} ({entry['percentage']}%)")
    print(f"\nReport written to: {report['output_file_paths']['generation_report_json']}")


if __name__ == "__main__":
    main()
