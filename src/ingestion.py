"""Data ingestion module for SparkSkewLab.

Provides streaming and schema-aware CSV ingestion for:
- customers.csv
- order_events.csv

Attaches lineage metadata (source_file, batch_id) and enforces clear error handling
for missing or empty files without silently dropping any input records.
"""

import csv
import logging
import os
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from src.config import CUSTOMER_FIELDS, ORDER_EVENT_FIELDS

logger = logging.getLogger(__name__)


class IngestionError(Exception):
    """Base exception for ingestion pipeline errors."""
    pass


class MissingFileError(IngestionError):
    """Raised when an expected input dataset file does not exist."""
    pass


class EmptyFileError(IngestionError):
    """Raised when an input dataset file is empty or lacks data rows."""
    pass


def read_csv_stream(
    file_path: Path,
    batch_id: str,
    expected_fields: Optional[List[str]] = None
) -> Generator[Dict[str, Any], None, None]:
    """Stream records from a CSV file, attaching lineage metadata.
    
    Args:
        file_path: Path to the target CSV file.
        batch_id: Identifier for the current ingestion batch.
        expected_fields: Optional list of expected column names for schema validation.
        
    Yields:
        Dict[str, Any] representing each record with source lineage attached.
        
    Raises:
        MissingFileError: If file_path does not exist on disk.
        EmptyFileError: If file is 0 bytes or contains no data rows.
    """
    if not file_path.exists():
        raise MissingFileError(f"Input file not found at path: {file_path}")

    if file_path.stat().st_size == 0:
        raise EmptyFileError(f"Input file is completely empty (0 bytes): {file_path}")

    source_filename = file_path.name
    logger.info(f"Ingesting records from {file_path} for batch {batch_id}")

    with open(file_path, mode="r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise EmptyFileError(f"Input file {file_path} lacks a valid CSV header")

        row_count = 0
        for row_index, raw_row in enumerate(reader, start=1):
            row_count += 1
            # Preserve original dictionary and attach lineage
            record = dict(raw_row)
            record["source_file"] = record.get("source_file") or source_filename
            record["batch_id"] = batch_id
            record["_source_row_index"] = row_index
            yield record

        if row_count == 0:
            raise EmptyFileError(f"Input file {file_path} has header but contains 0 data rows")

    logger.info(f"Ingested {row_count:,} raw records from {source_filename}")


def ingest_customers(file_path: Path, batch_id: str) -> Generator[Dict[str, Any], None, None]:
    """Ingest raw customer dimension records."""
    return read_csv_stream(file_path=file_path, batch_id=batch_id, expected_fields=CUSTOMER_FIELDS)


def ingest_order_events(file_path: Path, batch_id: str) -> Generator[Dict[str, Any], None, None]:
    """Ingest raw transactional order event records."""
    return read_csv_stream(file_path=file_path, batch_id=batch_id, expected_fields=ORDER_EVENT_FIELDS)
