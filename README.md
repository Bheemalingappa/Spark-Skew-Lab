# SparkSkewLab

**Reproducible PySpark Workload with Skew Diagnosis, Mitigation, and Correctness Verification**

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.14-blue.svg)](https://www.python.org/)
[![Spark Version](https://img.shields.io/badge/pyspark-4.3.0-orange.svg)](https://spark.apache.org/)
[![Test Suite](https://img.shields.io/badge/tests-pytest-green.svg)](https://docs.pytest.org/)

---

## 1. Project Objective & Context

In distributed data processing with Apache Spark, data skew (where a disproportionate volume of records share the same partitioning or join key) is one of the most common causes of pipeline degradation, memory spills, executor Out-Of-Memory (OOM) failures, and the dreaded "straggler task" syndrome.

**SparkSkewLab** is designed to provide:
1. **Reproducible Workload Generation**: Configurable synthetic dataset generation with controlled key skew (e.g., Pareto/Zipfian distribution representing high-volume sellers, merchants, or hot partitions) alongside balanced lookup/dimension datasets.
2. **Data Quality & Quarantine Layer**: Robust schema validation, primary key integrity checks, null rejection, and duplicate handling with an explicit quarantine path preserving lineage.
3. **Bottleneck Diagnosis & Baseline Execution**: Instrumentation of baseline shuffle joins that demonstrate skew symptoms (task duration variance, spill to disk/memory, partition record imbalances).
4. **Targeted Skew Mitigation**: Implementation of deterministic skew mitigation strategies (such as key salting with two-phase aggregation or broadcast join where applicable).
5. **Correctness Reconciliation & Benchmarking**: Deterministic validation proving that the optimized workload produces identical analytical outputs to the baseline, accompanied by transparent operational metrics (runtime, input/output counts, quarantine counts).

---

## 2. Planned Architecture & End-to-End Workflow

```
                       +------------------------+
                       | Synthetic Data Gen     |
                       | (Configurable Seed)    |
                       +-----------+------------+
                                   |
                                   v
                         [ data/raw / generated ]
                                   |
                                   v
                    +------------------------------+
                    | Ingestion & Schema Validator |
                    +--------------+---------------+
                                   |
                    +--------------+--------------+
                    |                             |
             [Valid Records]               [Invalid / Malformed]
                    |                             |
                    v                             v
           [ outputs/clean ]            [ outputs/quarantine ]
                    |
       +------------+------------+
       |                         |
       v                         v
+--------------+          +---------------+
| Baseline Run |          | Optimized Run |
| (Skewed Join)|          | (Salting /    |
+-------+------+          |  Broadcast)   |
        |                 +-------+-------+
        v                         v
[outputs/baseline]        [outputs/optimized]
        |                         |
        +------------+------------+
                     |
                     v
       +----------------------------+
       | Reconciliation & Auditor   |
       | - Row-count reconciliation |
       | - Key hash / sum check     |
       | - Runtime & metrics report |
       +-------------+--------------+
                     |
                     v
             [outputs/reports]
             [ outputs/logs  ]
```

### Implemented System Modules (`src/`)

| Module | Responsibility | Status |
| :--- | :--- | :--- |
| `src.config` | Centralized path definitions, schemas, anomaly keys, and default parameters. | **Complete** |
| `src.generator` | Deterministic synthetic generator for customers and order events with controlled skew. | **Complete** |
| `src.ingestion` | Schema-enforced streaming readers for customers and transactional events with traceability. | **Complete** |
| `src.validation` | Business rule validation, anomaly identification, and duplicate resolution. | **Complete** |
| `src.quarantine` | Segregated quarantine writer preserving root cause error lineage. | **Complete** |
| `src.pipeline` | Ingestion, validation, quarantine routing, and data quality reporting orchestrator. | **Complete** |
| `src.spark_session`| Controlled PySpark session factory with explicit shuffle partition and AQE controls. | **Complete** |
| `src.baseline` | Unmitigated baseline sort-merge join exposing single-partition bottleneck. | **Complete** |
| `src.skew_optimization` | Salted join and two-phase aggregation eliminating partition straggler. | **Complete** |
| `src.reconciliation` | High-efficiency full outer join correctness reconciliation and report generator. | **Complete** |
| `src.recovery` | Two-phase staging commit, persistent batch ledger, retry, restart, and backfills. | **Complete** |
| `src.reconciliation_queries` | Multi-tier source-to-target reconciliation assertions and query engine. | **Complete** |
| `src.benchmarking` | Controlled baseline vs. salted benchmark suite, timing engine, and partition introspection. | **Complete** |

---

## 3. Directory Layout

```
C:\CandidateWorkspace\
├── data/
│   ├── raw/                 # Raw incoming extracts
│   ├── generated/           # Synthetic scalable datasets (skewed, 100k events)
│   └── sample/              # Minimal starter sample for rapid smoke testing (1k events)
├── outputs/
│   ├── clean/               # Validated staging data
│   ├── quarantine/          # Rejected records with error lineage
│   ├── baseline/            # Analytical outputs from unmitigated run
│   ├── optimized/           # Analytical outputs from mitigated run
│   ├── reports/             # Multi-tier reconciliation, performance audits, and recovery reports
│   └── logs/                # Persistent batch ledger and execution run reports
├── src/                     # Core PySpark pipeline, recovery, and reconciliation modules
├── tests/                   # Pytest automated test suite (60 tests across 10 modules)
├── docs/                    # Architecture diagrams, data dictionary, runbooks
├── .gitignore               # Ignored environments, caches, and transient outputs
├── requirements.txt         # Project dependencies
└── README.md                # Project documentation and roadmap
```

---

## 4. Environment & Prerequisites

- **Python**: 3.10+ (Current verified runtime: Python 3.14.7)
- **Java**: Java 17+ (Current verified runtime: OpenJDK 17 Temurin)
- **Apache Spark / PySpark**: 4.3.0
- **PyTest**: 9.1.1+

### Installation & Environment Setup

```bash
# Optional: Create and activate a virtual environment
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# Windows Command Prompt
.venv\Scripts\activate.bat

# Install dependencies
pip install -r requirements.txt
```

---

## 5. Implementation Roadmap

- [x] **Step 1: Workspace Initialization** — Directory structure, README, `.gitignore`, `requirements.txt`, environment verification.
- [x] **Step 2: Synthetic Skewed Data Generator** — Modular generator with configurable key skew (~60% on `HOT_CUSTOMER`) and traceable edge anomalies.
- [x] **Step 3: Ingestion & Validation Pipeline** — Ingestion, field validation, deterministic deduplication, dead-letter quarantine, and data quality reporting.
- [x] **Step 4: Spark Session & Baseline Workload** — Controlled Spark environment (`local[*]`, AQE=False, broadcast=-1) diagnosing the shuffle skew straggler.
- [x] **Step 5: Key Salting Skew Mitigation & Audit** — Salted join with two-phase aggregation (-57.44% peak partition concentration) and full outer reconciliation.
- [x] **Step 6: Idempotency, Retry, Restart & Backfill** — Two-phase staging commit, persistent batch ledger, idempotent skip, crash restart, and historical backfills.
- [x] **Step 7: Reproducible Sample Data & Gold Standard Bundles** — Compact starter sample (`data/sample/`, 1k events) and full scalable dataset (`data/generated/`, 100k events).
- [x] **Step 8: Output Correctness Reconciliation & Assertion Suite** — Multi-tier mathematical conservation proofs ($\text{Raw}=\text{Clean}+\text{Quar}$, $\sum\text{Orders}=\text{Clean}$, $\Delta\text{Revenue}=\$0.00$), referential integrity, and automated assertion suite.
- [x] **Step 9: Performance Benchmarking & Optimization Analysis** — Multi-run benchmark execution (warm-up + 3 measured runs), physical partition distribution introspection (-57.44% peak reduction), trade-off analysis, and persistent reports.
- [x] **Step 10: Submission Packaging & Trade-off Notes** — Deliverable completeness audit, documentation bundle, clean-environment verification, GitHub readiness, and compliance matrix.

---

## 6. Step 6 Execution Workflows: Idempotency, Retry, & Backfill

Detailed architectural documentation is available in [docs/idempotency_recovery.md](docs/idempotency_recovery.md).

### 6.1 Executing an Idempotent Batch Run
```python
from pathlib import Path
from src.recovery import IdempotentPipeline

# First execution: ingests, validates, and atomically publishes datasets
pipeline = IdempotentPipeline(
    input_dir=Path("data/generated"),
    batch_id="batch_2026_11_14"
)
result = pipeline.run()
print("Run Status:", result["processing_status"])  # SUCCESS

# Second execution (same batch & input): detects prior success and skips publishing
pipeline_rerun = IdempotentPipeline(
    input_dir=Path("data/generated"),
    batch_id="batch_2026_11_14"
)
result_rerun = pipeline_rerun.run()
print("Rerun Status:", result_rerun["processing_status"])  # SKIPPED_IDEMPOTENT
```

### 6.2 Executing a Historical Backfill
```python
# Atomically replaces target outputs for batch_2026_11_14 while preserving run audit history
backfill_pipeline = IdempotentPipeline(
    input_dir=Path("data/raw/backfill_inputs"),
    batch_id="batch_2026_11_14",
    is_backfill=True
)
backfill_result = backfill_pipeline.run()
print("Backfill Status:", backfill_result["processing_status"])  # SUCCESS (attempt_type: BACKFILL)
```

---

## 7. Step 8 Execution Workflows: Correctness Reconciliation Queries

Detailed evidence is persisted in [outputs/reports/step8_correctness_report.json](outputs/reports/step8_correctness_report.json) and [outputs/reports/step8_final_audit.md](outputs/reports/step8_final_audit.md).

### 7.1 Executing Reconciliation Queries
```bash
# Execute standalone multi-tier source-to-target reconciliation audit across full and sample datasets
python src/reconciliation_queries.py

# Run automated correctness assertion tests (8 tests)
python -m pytest tests/test_correctness_queries.py -v
```

### 7.2 Running Full Repository Test Suite
```bash
# Run complete test suite across all 10 modules
python -m pytest tests/ -v
```

---

## 8. Step 9 Execution Workflows: Performance Benchmarking & Optimization

Detailed evidence is persisted in [outputs/reports/step9_performance_report.json](outputs/reports/step9_performance_report.json) and [outputs/reports/step9_final_audit.md](outputs/reports/step9_final_audit.md).

### 8.1 Executing the Standardized Benchmark
```bash
# Execute standardized baseline vs. optimized benchmark (1 warm-up + 3 measured runs)
python -m src.benchmarking
```

### 8.2 Key Benchmark Findings
- **Baseline Median Runtime**: 1.4767 seconds
- **Optimized Median Runtime**: 1.5903 seconds
- **Speedup Factor**: 0.9286x
- **Runtime Improvement**: -7.69%
- **Baseline Mean Runtime**: 1.3766 seconds
- **Optimized Mean Runtime**: 1.8518 seconds
- **Peak Partition Record Reduction**: Peak load slashed from **64,660 rows down to 27,517 rows (-57.44%)**.
- **Imbalance Ratio (Max / Mean)**: Reduced from **5.1884 down to 2.2080**.
- **Hot-Key Events**: 59,934 `HOT_CUSTOMER` events distributed across 8 salt buckets.
- **Correctness**: 1,000 rows on both sides and zero mismatches ($\Delta = \$0.00$).

> [!NOTE]
> **Performance Trade-Off Analysis**:
> While key salting successfully eliminated the extreme shuffle skew and reduced peak partition concentration by **57.44%**, this local benchmark did not demonstrate a runtime improvement (-7.69% runtime delta). On a single-node local JVM (`local[*]`) with 8 partitions sharing local CPU cores and memory, the two-phase aggregation coordination overhead (`HashAggregate -> Exchange -> HashAggregate`) and hot-key dimension replication slightly exceed the parallelization benefits for a ~100k-row dataset. However, on distributed production clusters, eliminating the single-partition straggler is crucial to prevent executor disk spill and Out-Of-Memory (OOM) failures.

---

## 9. Step 10 Submission Readiness & Documentation Index

Comprehensive audit evidence, architectural design notes, and data dictionaries are available in:
- **Final Submission Audit**: [outputs/reports/step10_final_submission_audit.md](outputs/reports/step10_final_submission_audit.md)
- **Data Dictionary**: [docs/data_dictionary.md](docs/data_dictionary.md)
- **Architecture & Technical Trade-offs**: [docs/architecture_and_tradeoffs.md](docs/architecture_and_tradeoffs.md)
- **Idempotency & Failure Recovery Runbook**: [docs/idempotency_recovery.md](docs/idempotency_recovery.md)

### 9.1 Reviewer Quickstart (Clean Environment)
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run fast smoke assertions on starter sample & correctness engine (12 tests, ~38s)
python -m pytest tests/test_correctness_queries.py tests/test_performance.py -v

# 3. Execute standalone source-to-target reconciliation audit
python src/reconciliation_queries.py

# 4. (Optional) Run complete test suite (60 tests across 10 modules)
python -m pytest tests/ -v
```


