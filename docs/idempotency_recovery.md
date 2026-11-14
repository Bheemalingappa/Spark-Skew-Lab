# SparkSkewLab — Idempotency, Retry, Restart, and Backfill Architecture

**Document Version**: 1.0.0  
**Project**: SparkSkewLab  
**Module Reference**: `src/recovery.py`, `src/pipeline.py`  
**Ledger Path**: `outputs/logs/batch_ledger.json`

---

## 1. Architectural Overview & Design Principles

In production data engineering pipelines, operational failures, duplicate data transfers, process crashes, and the need to backfill historical partitions are inevitable. The SparkSkewLab data pipeline implements **idempotent publishing semantics**, **prevents duplicate record accumulation across tested retries**, and maintains **full run traceability** through an integrated state ledger and two-phase staging architecture.

```
                          Incoming Batch Request
                        [batch_id, input_dir, run_id]
                                      |
                                      v
                        +----------------------------+
                        | Cryptographic Fingerprint  |
                        | (SHA-256 Composite Hash)   |
                        +--------------+-------------+
                                      |
                                      v
                        +----------------------------+
                        |   Ledger State Check       |
                        | (outputs/logs/batch_ledger)|
                        +--------------+-------------+
                                      |
                +---------------------+---------------------+
                |                                           |
      [Status == SUCCESS &                        [Status == FAILED,
       Not Backfill & Not Force]                   INTERRUPTED, or New]
                |                                           |
                v                                           v
     +----------------------+                     +--------------------+
     | SKIPPED_IDEMPOTENT   |                     | Register IN_PROGRESS
     | Return existing paths|                     | Create .staging_dir|
     +----------------------+                     +---------+----------+
                                                            |
                                                            v
                                                  +--------------------+
                                                  | Ingest & Validate  |
                                                  | into Staging Dir   |
                                                  +---------+----------+
                                                            |
                                                            v
                                                  +--------------------+
                                                  | Reconciliation     |
                                                  | Invariant Check    |
                                                  +---------+----------+
                                                            |
                                           +----------------+----------------+
                                           |                                 |
                                      [SUCCESS]                          [FAILURE]
                                           |                                 |
                                           v                                 v
                                 +--------------------+            +--------------------+
                                 | Atomic Commit via  |            | Wipe Staging Dir   |
                                 | os.replace()       |            | Ledger = FAILED    |
                                 +---------+----------+            +--------------------+
                                           |
                                           v
                                 +--------------------+
                                 | Ledger = SUCCESS   |
                                 | Emit Run Report    |
                                 +--------------------+
```

---

## 2. Batch Identity vs. Run Identity

To achieve strict reproducibility and observability, the pipeline decouples **business batch identity** from **physical execution run identity**:

| Dimension | Concept | Format | Responsibility |
| :--- | :--- | :--- | :--- |
| **Batch Identity** (`batch_id`) | Business slice / partition | `batch_YYYYMMDD` or `batch_001` | Identifies the logical dataset partition. Stable across retries and restarts. |
| **Run Identity** (`run_id`) | Physical execution attempt | `run_YYYYMMDD_HHMMSS_<uuid8>` | Unique identifier for a single process attempt. Never reused. |
| **Input Fingerprint** (`input_hash`) | Cryptographic checksum | SHA-256 (64 hex characters) | Computed across all input CSV files (`customers.csv`, `order_events.csv`). Detects identical vs. changed payloads. |

### How the Same Input is Recognized:
`compute_input_fingerprint()` scans the input directory, computes individual SHA-256 hashes of every CSV file in sorted alphabetical order, and digests them into a single `composite_hash`.
- **Same Input + Same `batch_id`**: Detected as an idempotent duplicate; processing is skipped.
- **Same Input + Different `batch_id`**: Processed as a distinct logical batch partition with independent state tracking.
- **Different Input + Same `batch_id`**: Recognized as a payload modification, permitted only when explicitly flagged as a `BACKFILL` or `FORCE` run.

---

## 3. Idempotency Strategy

Idempotency ensures that executing a workload multiple times on the same input produces the exact same published state as a single execution, without row duplication or state corruption.

### Rules of Idempotent Execution:
1. **Prior Success Check**: Before opening any input stream or creating staging folders, `IdempotentPipeline` inspects `BatchLedger`.
2. **Idempotent Skip**: If `current_status == "SUCCESS"` and neither `is_backfill` nor `force` is set:
   - Ingestion and validation are skipped completely.
   - Published target files are not modified (preserving original timestamps and contents).
   - An `IDEMPOTENT_CHECK` audit record is appended to the batch's run history.
   - The returned report reflects `status: "SKIPPED_IDEMPOTENT"`.
3. **Deterministic Row Balance**: Re-running the pipeline $N$ times in tested rerun scenarios ensures that published row counts equal exactly the accepted counts from the initial run ($1\times$), not $N\times$.

---

## 4. Failure Handling & Staging Isolation (Two-Phase Commit)

A critical vulnerability in naive data pipelines is the partial write: when a pipeline writes directly to the destination folder and crashes halfway through, leaving downstream consumers with corrupted or truncated datasets.

### Staging Isolation Architecture:
1. **Isolated Stage Workspace**: All customer and event validation outputs are written into dedicated, run-scoped hidden folders:
   - Clean Stage: `outputs/clean/.staging_<run_id>/`
   - Quarantine Stage: `outputs/quarantine/.staging_<run_id>/`
2. **Reconciliation Invariant Barrier**: Before committing, the pipeline verifies strict mathematical invariants:
   $$\text{Total Input Rows} = \text{Accepted Rows} + \text{Rejected (Quarantined) Rows}$$
   $$\text{Published Rows} = \text{Accepted Rows}$$
   If any discrepancy is detected, an exception is raised and commit is aborted.
3. **Atomic Commit**: Staged files are swapped into final published paths using `os.replace()`:
   - `clean_customers.csv.tmp` $\to$ `clean_customers.csv`
   - `clean_order_events.csv.tmp` $\to$ `clean_order_events.csv`
   `os.replace()` is an atomic filesystem operation on Windows and POSIX systems.
4. **Rollback on Error**: If any failure occurs during ingestion, validation, or commit:
   - The pipeline catches the exception and attempts to clean up the `.staging_<run_id>` directory.
   - If publication has begun, previous files backed up to `.backup_<run_id>/` are restored.
   - The batch is marked `FAILED` in the ledger with error messages and stack traces.

---

## 5. Retry Behavior

When an execution fails due to transient environment errors, disk locks, or input transfer delays:
1. **Retry Detection**: Submitting the same `batch_id` whose latest status in the ledger is `FAILED` automatically triggers a **Retry**.
2. **Attempt Counter**: `retry_count` is incremented in the batch ledger.
3. **Clean Slate Staging**: Retries execute from raw input files into a fresh staging directory (`.staging_<new_run_id>`), aiming to isolate the new attempt from prior failed runs.
4. **Status Transition**: Upon successful completion, the batch transitions from `FAILED` $\to$ `SUCCESS`.

---

## 6. Restart & Recovery Procedure for Interrupted Runs

If a host machine powers off or an executor process is killed (`SIGKILL` / task cancellation), the ledger will hold an uncompleted run with `current_status: "IN_PROGRESS"`:

### Recovery Workflow:
1. **Interruption Detection**: On pipeline initialization, `BatchLedger.mark_interrupted_runs()` scans all batches. Any run with `IN_PROGRESS` is automatically updated to `INTERRUPTED`.
2. **Recovery Execution**: Running the pipeline with the interrupted `batch_id` recognizes the `INTERRUPTED` state:
   - Sets attempt type to `RESTART`.
   - Increments `recovery_attempt_count`.
   - Attempts to clean up orphaned staging directories.
   - Resumes processing from input stage.
3. **Zero Output Duplication in Tested Scenarios**: Because partial outputs are isolated to staging, restarts in tested scenarios complete cleanly with exact record counts.

---

## 7. Backfill Procedure

Backfilling is required when historical data must be reprocessed due to upstream corrections, late-arriving dimensions, or modified business rules.

### Executing a Backfill:
```python
pipeline = IdempotentPipeline(
    input_dir=Path("data/raw/batch_2026_10_corrected"),
    batch_id="batch_2026_10",
    is_backfill=True  # Enables backfill replacement mode
)
result = pipeline.run()
```

### Backfill Invariants & Behavior:
1. **Atomic Replacement**: Backfilling replaces the published dataset for `batch_2026_10` atomically via staging swap.
2. **Historical Lineage Preservation**: Unlike an unrecorded overwrite, the ledger retains every historical run (`FIRST_RUN`, `IDEMPOTENT_CHECK`, `BACKFILL`), preserving audit trails.
3. **Batch Isolation**: Re-publishing `batch_2026_10` touches only files and partitions belonging to that batch; all unrelated batches remain untouched.

---

## 8. Observability & Reporting Schema

Every run produces an audit report at `outputs/reports/run_report_<run_id>.json` and updates `outputs/logs/batch_ledger.json`.

```json
{
  "project": "SparkSkewLab",
  "batch_id": "batch_demo_01",
  "run_id": "run_20261114_101943_45baa106",
  "processing_status": "SUCCESS",
  "attempt_type": "BACKFILL",
  "is_backfill": true,
  "retry_count": 0,
  "recovery_attempt_count": 0,
  "start_timestamp": "2026-11-14T10:19:43.307000+00:00",
  "end_timestamp": "2026-11-14T10:19:43.346000+00:00",
  "duration_seconds": 0.039,
  "reconciliation_status": "BALANCED",
  "error_details": null,
  "counts": {
    "input_counts": { "customers": 3, "events": 6, "total": 9 },
    "accepted_counts": { "customers": 3, "events": 6, "total": 9 },
    "rejected_counts": { "customers": 0, "events": 0, "total": 0 },
    "published_counts": { "customers": 3, "events": 6, "total": 9 }
  },
  "input_fingerprint": {
    "composite_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "files": { ... }
  }
}
```

---

## 9. Known Limitations & Trade-offs

1. **Local File-Based Ledger**:
   - *Current Implementation*: Uses `outputs/logs/batch_ledger.json` with temporary file atomic replacement (`os.replace`).
   - *Trade-off*: Highly lightweight and zero-dependency for local execution.
   - *Production Evolution*: In a multi-worker distributed environment, replace the local JSON ledger with a centralized ACID metadata store (e.g., PostgreSQL table with row-level locks or DynamoDB with conditional writes).
2. **Target File Granularity**:
   - In single-batch mode, published files are `clean_customers.csv` and `clean_order_events.csv` to seamlessly feed the downstream Step 4 & 5 Spark baseline and optimized workloads.
   - In multi-batch partitions, partitioned directory structures (e.g. `clean/batch_id=batch_001/`) can be enabled by setting `partition_by_batch=True`.
