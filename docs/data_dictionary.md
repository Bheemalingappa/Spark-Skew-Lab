# SparkSkewLab — Data Dictionary & Schema Specification

This document provides the definitive data dictionary, physical storage formats, field constraints, nullability rules, and business definitions for all datasets across the SparkSkewLab pipeline.

---

## 1. Customer Dimension Dataset

### 1.1 Ingestion & Clean Storage
- **Raw / Scalable Path**: `data/generated/customers.csv`
- **Sample Path**: `data/sample/customers.csv`
- **Clean Published Path**: `outputs/clean/clean_customers.csv`
- **Storage Format**: CSV (UTF-8, comma-delimited, header row present)

### 1.2 Schema Definition

| Field Name | Physical Type | Logical Type | Nullable | Primary Key | Description & Validation Rules |
| :--- | :--- | :--- | :---: | :---: | :--- |
| `customer_id` | `StringType` | `VARCHAR(32)` | **No** | **Yes** | Unique customer identifier (e.g., `CUST_000001`..`CUST_001000` or `HOT_CUSTOMER`). Must match regex `^[A-Za-z0-9_]{2,32}$`. Duplicate keys are rejected. |
| `customer_name` | `StringType` | `VARCHAR(128)` | **No** | No | Formal enterprise or commercial account name (e.g., `Global Logistics Corp`). Must not be null or blank. |
| `city` | `StringType` | `VARCHAR(64)` | **No** | No | Registered corporate billing city (e.g., `New York`, `Dallas`, `Seattle`). |
| `customer_segment` | `StringType` | `ENUM` | **No** | No | Customer classification tier. Valid values: `ENTERPRISE`, `VIP`, `RETAIL`. |

---

## 2. Order Events Transactional Dataset

### 2.1 Ingestion & Clean Storage
- **Raw / Scalable Path**: `data/generated/order_events.csv`
- **Sample Path**: `data/sample/order_events.csv`
- **Clean Published Path**: `outputs/clean/clean_order_events.csv`
- **Storage Format**: CSV (UTF-8, comma-delimited, header row present)

### 2.2 Schema Definition

| Field Name | Physical Type | Logical Type | Nullable | Primary Key | Description & Validation Rules |
| :--- | :--- | :--- | :---: | :---: | :--- |
| `event_id` | `StringType` | `VARCHAR(32)` | **No** | **Yes** | Globally unique transaction event identifier (e.g., `EVT_00000001`). Re-occurrences within the same batch are quarantined as duplicates. |
| `customer_id` | `StringType` | `VARCHAR(32)` | **No** | No (FK) | Foreign key referencing `customer_id` in the Customer Dimension. Must exist in validated customer set; otherwise quarantined as orphaned event. Highly skewed key (`HOT_CUSTOMER` represents ~60% of all events). |
| `order_amount` | `StringType` (CSV) / `Decimal(10,2)` | `DECIMAL(10,2)` | **No** | No | Monetary order value in USD. Must be strictly positive ($> 0.00$) and valid numeric format with up to two decimal places. |
| `event_type` | `StringType` | `ENUM` | **No** | No | Event operational type. Valid values: `PURCHASE`, `PAYMENT`, `RENEWAL`. |
| `event_timestamp` | `StringType` (CSV) / `TimestampType` | `TIMESTAMP` | **No** | No | UTC transaction occurrence timestamp in ISO 8601 format (`YYYY-MM-DDTHH:MM:SSZ` or with timezone offset). |
| `source_file` | `StringType` | `VARCHAR(128)` | **No** | No | Lineage metadata indicating the source file from which the record was ingested (e.g., `order_events.csv`). |

---

## 3. Quarantine (Dead-Letter) Datasets

When records fail schema, integrity, or referential constraints during the ingestion validation stage, they are routed to the quarantine storage layer while preserving the original record and diagnostic lineage.

- **Customer Quarantine Path**: `outputs/quarantine/quarantined_customers.csv`
- **Event Quarantine Path**: `outputs/quarantine/quarantined_order_events.csv`
- **Storage Format**: CSV (UTF-8, comma-delimited, header row present)

### 3.1 Lineage Fields Appended

| Appended Field Name | Type | Description |
| :--- | :--- | :--- |
| `quarantine_reason` | `StringType` | Machine-readable error code explaining the exact rejection criteria (e.g., `DUPLICATE_EVENT_ID`, `ORPHAN_CUSTOMER_ID`, `NEGATIVE_OR_ZERO_AMOUNT`, `INVALID_TIMESTAMP`, `MISSING_REQUIRED_FIELD`). |
| `quarantine_timestamp`| `StringType` | ISO 8601 UTC timestamp indicating when the record was rejected. |
| `batch_id` | `StringType` | Batch processing identifier associated with the ingestion run. |

---

## 4. Analytical Summary Output Datasets

- **Baseline Output Path**: `outputs/baseline/customer_summary`
- **Optimized Output Path**: `outputs/optimized/customer_summary`
- **Storage Format**: Apache Parquet (Snappy-compressed, columnar format)
- **Partitioning**: 8 post-shuffle partitions

### 4.1 Schema Definition

| Field Name | Physical Type | Logical Type | Nullable | Aggregation Formula / Description |
| :--- | :--- | :--- | :---: | :--- |
| `customer_id` | `StringType` | `VARCHAR(32)` | **No** | Grouping key matching customer dimension. Exactly 1,000 unique records. |
| `customer_name` | `StringType` | `VARCHAR(128)` | **No** | Preserved account name from customer dimension. |
| `order_count` | `LongType` | `BIGINT` | **No** | Total count of validated purchase events attributed to this customer: `COUNT(event_id)`. |
| `total_amount` | `DecimalType(12,2)` | `DECIMAL(12,2)` | **No** | Total aggregated transaction revenue: `ROUND(SUM(order_amount), 2)`. |
| `average_order_amount` | `DecimalType(12,2)` | `DECIMAL(12,2)` | **No** | Mean order value: `ROUND(SUM(order_amount) / COUNT(event_id), 2)`. |

---

## 5. Metadata and Operational State Schemas

### 5.1 Batch Ledger (`outputs/logs/batch_ledger.json`)
Persistent transactional state file used by `src/recovery.py` to enforce idempotency and safe recovery:

```json
{
  "batches": {
    "batch_prod_001": {
      "batch_id": "batch_prod_001",
      "latest_run_id": "run_20261114_084627_c73a11",
      "input_fingerprint": "704812ae...",
      "processing_status": "SUCCESS",
      "attempt_type": "FIRST_ATTEMPT",
      "created_at": "2026-11-14T08:46:27.123456Z",
      "updated_at": "2026-11-14T08:46:31.654321Z",
      "published_artifacts": {
        "clean_customers": "outputs/clean/clean_customers.csv",
        "clean_order_events": "outputs/clean/clean_order_events.csv"
      }
    }
  }
}
```
