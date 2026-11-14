# SparkSkewLab — Architecture, Design Decisions, and Technical Trade-Offs

## 1. Architectural Overview

SparkSkewLab is an enterprise-grade PySpark reference pipeline designed to demonstrate:
1. Systematic diagnosis of shuffle skew bottlenecks in distributed joins.
2. Deterministic mitigation using key salting and two-phase aggregation.
3. Multi-tier mathematical source-to-target conservation audits.
4. Production-grade operational resilience (idempotency, crash restart, atomic rollback, and historical backfills).

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             END-TO-END DATA PIPELINE                             │
└──────────────────────────────────────────────────────────────────────────────────┘

        [ data/raw or data/generated ]
                      │
                      ▼
     ┌──────────────────────────────────┐
     │ Ingestion & Quality Engine       │  (src/ingestion.py, src/validation.py)
     │ - Schema enforcement             │
     │ - Referential integrity checks   │
     │ - Deterministic deduplication    │
     └────────────────┬─────────────────┘
                      │
         ┌────────────┴────────────┐
         ▼                         ▼
  [ outputs/clean ]        [ outputs/quarantine ]
  - 1,000 customers        - 300 rejected events
  - 99,700 order events    - Preserves root-cause lineage
         │
         ├───────────────────────────────────┐
         ▼                                   ▼
┌───────────────────────────────┐   ┌───────────────────────────────┐
│ Workload A: Baseline Join     │   │ Workload B: Salted Join       │
│ (src/baseline.py)             │   │ (src/skew_optimization.py)    │
│ - Unmitigated SortMergeJoin   │   │ - Deterministic Event Salting │
│ - Fixed 8 shuffle partitions  │   │ - Customer Dim Replication    │
│ - Single-task straggler       │   │ - Two-Phase Aggregation       │
│   (Partition 3: 64,660 rows)  │   │ - Leveled Partitions          │
│                               │   │   (Peak: 27,517 rows, -57.4%) │
└───────────────┬───────────────┘   └───────────────┬───────────────┘
                │                                   │
                ▼                                   ▼
    [ outputs/baseline/summary ]        [ outputs/optimized/summary ]
                │                                   │
                └─────────────────┬─────────────────┘
                                  ▼
                 ┌─────────────────────────────────┐
                 │ Correctness Reconciliation      │
                 │ (src/reconciliation.py &        │
                 │  src/reconciliation_queries.py) │
                 │ - Full Outer Join Equivalence   │
                 │ - Tier 1-5 Mathematical Proofs  │
                 │ - 0 Key Mismatch, $0.00 Variance│
                 └────────────────┬────────────────┘
                                  │
                                  ▼
                 [ outputs/reports/*.json, *.md ]
```

---

## 2. The Data Skew Bottleneck

### 2.1 The Problem: Shuffle Imbalance in SortMergeJoin
In standard distributed joins, PySpark hashes join keys using `Murmur3Hash(join_key) % num_partitions` to colocate matching records on the same executor partition.
When a single key accounts for a disproportionate fraction of the dataset:
- In our dataset, `HOT_CUSTOMER` accounts for **59,934 events (60.11% of all clean events)**.
- Normal customers (999 accounts) average only **~39.8 events per account**.
- During the baseline join with 8 shuffle partitions, Spark maps `HOT_CUSTOMER` and 4,726 hash-colliding normal records into **Partition 3**, creating an extreme straggler of **64,660 records (64.85% of total dataset volume)**.
- 7 executor cores finish their tasks in milliseconds and sit completely idle while Task 3 executes alone, serializing the pipeline at the shuffle barrier.

---

## 3. Targeted Skew Mitigation: Key Salting with Two-Phase Aggregation

### 3.1 Mechanics
1. **Hot Key Isolation**: Identify keys exceeding skew thresholds (`HOT_CUSTOMER`).
2. **Deterministic Salt Assignment**:
   For events where `customer_id == 'HOT_CUSTOMER'`, assign salt $s \in [0, 7]$ deterministically:
   $$\text{salt} = \text{pmod}(\text{hash}(\text{event\_id}, 42), 8)$$
   Normal customer events receive $\text{salt} = 0$.
3. **Dimension Replication**:
   The customer dimension row for `HOT_CUSTOMER` is exploded across all 8 salt buckets ($0 \dots 7$), while normal customer rows retain $\text{salt} = 0$. This ensures referential integrity across all salt partitions with minimal memory footprint (only 7 extra dimension rows).
4. **Composite Join**:
   Join on composite key `[customer_id, salt]`. The hot key's load is split across 8 distinct hash buckets.
5. **Two-Phase Aggregation**:
   - *Phase 1 (Partial)*: Aggregate `groupBy(customer_id, customer_name, salt)`.
   - *Phase 2 (Final Rollup)*: Aggregate `groupBy(customer_id, customer_name)` summing the partial counts and amounts.

---

## 4. Architectural Trade-Off Analysis

| Architectural Dimension | Baseline (Unmitigated) | Key Salting with Two-Phase Aggregation | Production Trade-off Evaluation |
| :--- | :--- | :--- | :--- |
| **Peak Partition Load** | 64,660 records (Partition 3) | **27,517 records (Partition 1)** | **-57.44% peak concentration**. Drastically reduces executor memory pressure and spill to disk. |
| **Partition Imbalance Ratio** | **5.1884** (Max / Mean) | **2.2080** (Max / Mean) | **57.44% lower load imbalance**. Eliminates extreme task duration variance. |
| **Dimension Memory Footprint** | 1,000 dimension rows | 1,007 dimension rows (+7 rows) | Negligible overhead for isolated hot keys. (Trade-off: if millions of keys were salted, replication would require bounded memory controls). |
| **Shuffle Coordination Stages** | Single join + aggregation | Two aggregation phases + composite shuffle | Adds an extra HashAggregate stage and shuffle boundary (`HashAggregate -> Exchange -> HashAggregate`). |
| **Small vs Large Partition Spaces** | Inevitable straggler | Murmur3 hash collisions on 8 partitions (Partition 1 received 3 salt buckets) | With 8 partitions, Murmur3 mapped 3 buckets to partition 1. On standard cluster settings (200 partitions), salt buckets distribute across separate executors with virtually zero collision. |
| **Local Host vs Cluster Runtime** | ~0.908s median | ~0.933s median (near-parity) | On a single-node CPU with unified RAM and cache, the coordination overhead of stage 2 balances parallel gains for 99.7k rows. On distributed clusters with network transfers, salting prevents catastrophic executor OOMs. |

---

## 5. Idempotency & Failure Recovery Architecture

In `src/recovery.py`, operational resilience is guaranteed through transactional staging and state ledger controls:

1. **Deterministic Fingerprinting**:
   Incoming data directories are hashed using SHA-256 over canonical file names, byte sizes, and timestamps.
2. **Idempotent Skipping**:
   If a batch with the same `batch_id` and identical fingerprint was previously marked `SUCCESS`, processing is skipped (`SKIPPED_IDEMPOTENT`), preventing duplicate published records.
3. **Modified Input Policy**:
   If the same `batch_id` is submitted with modified inputs without `--backfill`, execution fails immediately to prevent silent partial overwrite.
4. **Two-Phase Atomic Publication & Rollback**:
   Outputs are generated in temporary staging directories (`.staging_<run_id>/`). Target directories are backed up to `.backup_<run_id>/` prior to replacement. If any replacement fails, the entire transaction is rolled back from backup, ensuring zero partial-write corruption.
5. **Backfill Mode**:
   Explicit backfills (`is_backfill=True`) replace existing outputs atomically while appending complete run history to `outputs/logs/batch_ledger.json`.
