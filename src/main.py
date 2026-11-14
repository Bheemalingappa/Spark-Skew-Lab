"""CLI entry point for SparkSkewLab Ingestion and Data Quality Pipeline.

Usage:
    python -m src.main --input-dir data/generated --batch-id batch_001
    python -m src.main --input-dir data/sample --batch-id sample_batch
"""

import argparse
import json
from pathlib import Path
import sys

from src.config import DATA_GENERATED_DIR, DATA_SAMPLE_DIR
from src.pipeline import DataPipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SparkSkewLab Ingestion and Data Quality Pipeline"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(DATA_GENERATED_DIR),
        help=f"Directory containing input customers.csv and order_events.csv (default: {DATA_GENERATED_DIR})"
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default="batch_001",
        help="Batch identifier string for lineage tracking (default: batch_001)"
    )
    parser.add_argument(
        "--clean-dir",
        type=str,
        default=None,
        help="Directory for valid clean output CSVs (default: outputs/clean)"
    )
    parser.add_argument(
        "--quarantine-dir",
        type=str,
        default=None,
        help="Directory for quarantined CSVs (default: outputs/quarantine)"
    )
    parser.add_argument(
        "--reports-dir",
        type=str,
        default=None,
        help="Directory for audit and run reports (default: outputs/reports)"
    )

    args = parser.parse_args()

    input_path = Path(args.input_dir)
    clean_path = Path(args.clean_dir) if args.clean_dir else None
    quarantine_path = Path(args.quarantine_dir) if args.quarantine_dir else None
    reports_path = Path(args.reports_dir) if args.reports_dir else None

    print(f"============================================================")
    print(f" SparkSkewLab Ingestion & Data Quality Validation Pipeline")
    print(f" Input:    {input_path}")
    print(f" Batch ID: {args.batch_id}")
    print(f"============================================================\n")

    pipeline = DataPipeline(
        input_dir=input_path,
        batch_id=args.batch_id,
        clean_dir=clean_path,
        quarantine_dir=quarantine_path,
        reports_dir=reports_path
    )

    try:
        result = pipeline.run()
    except Exception as exc:
        print(f"\n[FATAL] Pipeline run failed: {exc}", file=sys.stderr)
        sys.exit(1)

    dq = result["data_quality_report"]
    run_rep = result["run_report"]

    print("\n----------------- EXECUTION SUMMARY -----------------")
    print(f"Status:             {result['status']}")
    print(f"Run ID:             {result['run_id']}")
    print(f"Batch ID:           {result['batch_id']}")
    print(f"Duration:           {run_rep['duration_seconds']}s")
    print(f"\n----------------- ROW RECONCILIATION -----------------")
    print(f"Customers Input:    {dq['total_input_customers']:,}")
    print(f"  - Accepted:       {dq['accepted_customers']:,}")
    print(f"  - Quarantined:    {dq['rejected_customers']:,}")
    print(f"Events Input:       {dq['total_input_events']:,}")
    print(f"  - Accepted:       {dq['accepted_events']:,}")
    print(f"  - Quarantined:    {dq['rejected_events']:,}")
    print(f"Reconciliation:     Customers Balanced={dq['reconciliation']['customers_balanced']}, Events Balanced={dq['reconciliation']['events_balanced']}")

    print("\n----------------- REJECTIONS BY REASON -----------------")
    if dq["rejection_counts_grouped_by_reason"]:
        for reason, count in dq["rejection_counts_grouped_by_reason"].items():
            print(f"  • {reason}: {count:,}")
    else:
        print("  None (all records valid)")

    print(f"\n----------------- REPORT ARTIFACTS -----------------")
    print(f"Data Quality Report: {pipeline.reports_dir / 'data_quality_report.json'}")
    print(f"Run Execution Report: {pipeline.reports_dir / 'run_report.json'}")
    print(f"Clean Customers:     {pipeline.clean_customers_path}")
    print(f"Clean Order Events:  {pipeline.clean_events_path}")
    print(f"Quarantined Events:  {pipeline.quarantine_dir / 'quarantined_order_events.csv'}")
    print(f"============================================================\n")


if __name__ == "__main__":
    main()
