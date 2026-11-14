"""Configuration constants and default parameters for SparkSkewLab."""

from pathlib import Path
from typing import Dict, List

# Workspace Root & Standard Paths
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = WORKSPACE_ROOT / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_GENERATED_DIR = DATA_DIR / "generated"
DATA_SAMPLE_DIR = DATA_DIR / "sample"

OUTPUTS_DIR = WORKSPACE_ROOT / "outputs"
OUTPUTS_CLEAN_DIR = WORKSPACE_ROOT / "outputs" / "clean"
OUTPUTS_QUARANTINE_DIR = WORKSPACE_ROOT / "outputs" / "quarantine"
OUTPUTS_BASELINE_DIR = WORKSPACE_ROOT / "outputs" / "baseline"
OUTPUTS_OPTIMIZED_DIR = WORKSPACE_ROOT / "outputs" / "optimized"
OUTPUTS_REPORTS_DIR = WORKSPACE_ROOT / "outputs" / "reports"
OUTPUTS_LOGS_DIR = WORKSPACE_ROOT / "outputs" / "logs"

# Default Generator Configuration
DEFAULT_RANDOM_SEED: int = 42
DEFAULT_CUSTOMERS_COUNT: int = 1000
DEFAULT_EVENTS_COUNT: int = 100000
DEFAULT_SKEW_RATIO: float = 0.60
HOT_CUSTOMER_ID: str = "HOT_CUSTOMER"

# Synthetic Dimension Attributes
CUSTOMER_SEGMENTS: List[str] = ["RETAIL", "WHOLESALE", "ENTERPRISE", "VIP"]
CITIES: List[str] = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Seattle", "Denver", "Boston", "San Francisco"
]
EVENT_TYPES: List[str] = ["PURCHASE", "REFUND", "PAYMENT", "CHECKOUT"]

# Field Schemas
CUSTOMER_FIELDS: List[str] = ["customer_id", "customer_name", "city", "customer_segment"]
ORDER_EVENT_FIELDS: List[str] = ["event_id", "customer_id", "order_amount", "event_type", "event_timestamp", "source_file"]

# Anomaly / Validation Rejection Reason Keys
ANOMALY_DUPLICATE_EVENT_ID: str = "duplicate_event_id"
ANOMALY_MISSING_CUSTOMER_ID: str = "missing_customer_id"
ANOMALY_MISSING_EVENT_ID: str = "missing_event_id"
ANOMALY_NEGATIVE_AMOUNT: str = "negative_order_amount"
ANOMALY_INVALID_CUSTOMER_REF: str = "invalid_customer_reference"
ANOMALY_MISSING_TIMESTAMP: str = "missing_timestamp"

ALL_ANOMALY_TYPES: List[str] = [
    ANOMALY_DUPLICATE_EVENT_ID,
    ANOMALY_MISSING_CUSTOMER_ID,
    ANOMALY_MISSING_EVENT_ID,
    ANOMALY_NEGATIVE_AMOUNT,
    ANOMALY_INVALID_CUSTOMER_REF,
    ANOMALY_MISSING_TIMESTAMP,
]

# Validation Failure Reasons
REASON_MISSING_CUSTOMER_ID: str = "missing_customer_id"
REASON_DUPLICATE_CUSTOMER_ID: str = "duplicate_customer_id"
REASON_MISSING_CUSTOMER_NAME: str = "missing_customer_name"
REASON_MISSING_CITY: str = "missing_city"
REASON_INVALID_CUSTOMER_SEGMENT: str = "invalid_customer_segment"

REASON_MISSING_EVENT_ID: str = "missing_event_id"
REASON_DUPLICATE_EVENT_ID: str = "duplicate_event_id"
REASON_INVALID_CUSTOMER_REF: str = "invalid_customer_reference"
REASON_NON_NUMERIC_AMOUNT: str = "non_numeric_order_amount"
REASON_NON_POSITIVE_AMOUNT: str = "negative_or_zero_order_amount"
REASON_INVALID_EVENT_TYPE: str = "invalid_event_type"
REASON_INVALID_TIMESTAMP: str = "invalid_or_missing_timestamp"
REASON_MISSING_TIMESTAMP: str = "invalid_or_missing_timestamp"

