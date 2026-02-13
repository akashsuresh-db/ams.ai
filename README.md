# Unified Alert Pipeline — Databricks MVP

**Ingestion → Deduplication → Correlation → Context Building → LLM Summarization**

A production-style streaming pipeline built on Databricks that ingests operational alerts, deduplicates them, correlates with surrounding telemetry, builds structured context bundles, and generates LLM-powered incident summaries.

---

## Architecture Overview

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  JSONL       │    │   BRONZE     │    │   SILVER     │    │    GOLD      │    │  LLM BATCH   │
│  Landing     │───▶│  Raw Events  │───▶│  Deduped     │───▶│  Correlated  │───▶│  Summarized  │
│  Zone        │    │  (Delta)     │    │  Alerts      │    │  Incidents   │    │  Incidents   │
│              │    │              │    │  (Delta)     │    │  (Delta)     │    │              │
│ Auto Loader  │    │ Append-only  │    │ Stateful     │    │ foreachBatch │    │ ai_summarize │
│ cloudFiles   │    │ No transform │    │ Aggregation  │    │ 60-min join  │    │ Batch UPDATE │
└─────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
     Stream              Stream              Stream              Stream              Batch
```

---

## File Structure

```
databricks-alert-pipeline/
├── config.py                  # Shared configuration (paths, params, table names)
├── 00_setup.py                # Create catalog, schema, and Delta tables
├── 01_data_generator.py       # Synthetic JSONL event generator (3 scenarios)
├── 02_bronze_ingestion.py     # Auto Loader → Bronze Delta table
├── 03_silver_dedup.py         # Fingerprint-based alert deduplication
├── 04_gold_correlation.py     # 60-min lookback correlation + context builder
├── 05_llm_summarization.sql   # Batch ai_summarize() + root cause extraction
├── 06_run_pipeline.py         # End-to-end orchestrator notebook
└── README.md                  # This file
```

---

## Quick Start

### 1. Upload to Databricks

Import all `.py` and `.sql` files into a Databricks workspace folder (e.g., `/Repos/alert-pipeline/`).

### 2. Run Setup

```
Run: 00_setup.py
```

This creates the `alert_ops.pipeline` schema and all three Delta tables.

### 3. Generate Synthetic Data

```
Run: 01_data_generator.py
```

Generates 400–600 interleaved JSONL events and writes to the landing zone.

### 4. Run the Pipeline

Execute notebooks in order (or use `06_run_pipeline.py`):

```
02_bronze_ingestion.py   →  Ingest raw events
03_silver_dedup.py       →  Deduplicate alerts
04_gold_correlation.py   →  Correlate + build context
05_llm_summarization.sql →  LLM batch summarization
```

---

## Simulated Scenarios

### Scenario 1 — Deployment-Induced Incident
| Phase | Time | Events |
|-------|------|--------|
| Deploy | T+0 | `payments-api` v2.4.1 rolling update |
| Ramp | T+20–30 min | error_rate climbs, CPU increases |
| Peak | T+30 min | CPU 92.5%, error_rate 0.18 |
| Repeated | T+31–35 min | 5 CPU alerts (same fingerprint) |
| Errors | T+30–35 min | TimeoutException, DBConnectionError |

### Scenario 2 — Memory Leak (No Deployment)
| Phase | Time | Events |
|-------|------|--------|
| Ramp | T+0–40 min | memory 55% → 92% |
| Alert | T+35 min | memory_high threshold 85% |
| Repeated | T+36–40 min | 5 memory alerts (same fingerprint) |
| Symptoms | T+35–40 min | GC pauses, heap pressure warnings |

### Scenario 3 — Infra Restart (Noise)
| Phase | Time | Events |
|-------|------|--------|
| Restart | T+0 | Host maintenance restart |
| Spike | T+1–2 min | Brief CPU spike |
| Alert | T+2 min | Single cpu_high (MEDIUM) |
| Resolve | T+3–5 min | Metrics normalize |

---

## Architectural Decisions

### Why Deduplication Must Be Deterministic

Alert systems frequently re-fire the same alert while a condition persists (e.g., "CPU > 80%" every 60 seconds). Without deduplication, downstream consumers receive N identical signals instead of one.

Dedup **must** use a deterministic algorithm (fingerprint hash + time window) rather than probabilistic or LLM-based approaches because:

1. **Reproducibility**: Re-running the pipeline on the same data must produce identical results. This is essential for debugging, auditing, and compliance.
2. **Auditability**: An on-call engineer must be able to trace exactly which alerts were suppressed and why. A SHA-256 fingerprint and a 5-minute window are transparent and explainable.
3. **Performance**: Dedup runs in the streaming hot path. Each micro-batch must complete in seconds. LLM calls (2–10s latency) would bottleneck the entire pipeline.
4. **Testability**: Deterministic logic can be unit-tested with fixed inputs and expected outputs. LLM outputs vary between runs.

### Why Correlation Must Not Rely on LLM

Correlation is the process of gathering all related telemetry for an incident: deployments, error logs, metrics, and prior alerts within a 60-minute window. This is a **data engineering** operation, not a **reasoning** operation.

Using Spark SQL for correlation ensures:

- **Determinism**: The same alert always correlates with the same set of Bronze events.
- **Cost efficiency**: Scanning thousands of Bronze rows with Spark is nearly free. Sending the same data to an LLM would cost significant tokens.
- **Latency**: Spark processes the 60-minute lookback join in sub-second time. LLM round-trips would add seconds per incident.
- **Correctness**: SQL joins are exact. LLMs might miss events, hallucinate connections, or inconsistently interpret time windows.

The LLM's role is to **interpret** the pre-built context, not to **gather** it.

### Why LLM Runs as Micro-Batch (Not Inside Streaming DAG)

The `ai_summarize()` function is deliberately separated from the streaming pipeline and runs as a batch SQL UPDATE:

1. **Cost control**: In streaming, every micro-batch would fire LLM calls — potentially hundreds per hour during an incident storm. Batch mode processes only `WHERE summary IS NULL` rows, naturally deduplicating work.
2. **Latency isolation**: LLM calls take 2–10 seconds. Inside the streaming DAG, they would block correlation processing, increasing end-to-end latency for dashboards and PagerDuty integrations.
3. **Retry safety**: LLM calls can fail (rate limits, model timeouts). In streaming, a failed micro-batch retries ALL records. In batch mode, the idempotent `WHERE summary IS NULL` clause means only unfilled rows are retried.
4. **Deterministic/stochastic separation**: Bronze → Silver → Gold is deterministic (same input → same output). LLM summarization is stochastic (different runs may produce different text). Keeping them in separate execution contexts means the deterministic pipeline can be independently validated and tested.

### Why Bronze / Silver / Gold Separation Matters

The medallion architecture is not just organizational — it provides critical operational guarantees:

| Layer | Purpose | Key Property |
|-------|---------|-------------|
| **Bronze** | Raw, immutable event log | **Reprocessable**: If Silver or Gold logic has a bug, we can recompute from Bronze without re-ingesting |
| **Silver** | Cleaned, deduplicated alerts | **Noise-free**: Downstream consumers see only meaningful signals, not repeated firings |
| **Gold** | Business-level incidents | **Enriched**: Each incident has full context, ready for dashboards and LLM summarization |

Without this separation:
- A bug in dedup logic could corrupt raw data (no Bronze isolation)
- Alert storms would flood LLM summarization (no Silver filtering)
- Re-processing would require re-ingesting from source systems (no immutable Bronze)

### Why `ai_summarize()` Should Not Be Inside the Streaming DAG

Beyond the cost and latency reasons above, there is an architectural principle: **streaming DAGs should be side-effect-free and deterministic**.

`ai_summarize()` has external side effects (API call to a model endpoint) and is non-deterministic (same prompt may produce different responses). Placing it inside a streaming DAG means:

- **Checkpoint recovery** might re-invoke the LLM for already-summarized incidents
- **Micro-batch retries** would generate different summaries for the same incident
- **Backpressure** from LLM rate limits would stall the entire pipeline
- **Testing** becomes impossible without mocking the LLM endpoint

By running it as a separate batch SQL job, we get natural idempotency (`WHERE summary IS NULL`), independent scheduling, and clean separation of concerns.

---

## Configuration

All configuration lives in `config.py`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BASE_PATH` | `/mnt/alert_pipeline` | Root storage path |
| `DEDUP_WINDOW_MINUTES` | 5 | Alert suppression window |
| `CORRELATION_LOOKBACK_MINUTES` | 60 | Correlation context window |
| `WATERMARK_DELAY` | 10 minutes | Late-arrival tolerance |
| `AUTOLOADER_MAX_FILES_PER_TRIGGER` | 10 | Files per micro-batch |

---

## Production Deployment

For production, convert the orchestrator to a **Databricks Workflow**:

```
Task 1: 02_bronze_ingestion.py  (continuous streaming)
Task 2: 03_silver_dedup.py      (continuous streaming)
Task 3: 04_gold_correlation.py  (continuous streaming)
Task 4: 05_llm_summarization.sql (scheduled every 5 min)
```

Tasks 1–3 run as long-running streaming jobs. Task 4 runs on a schedule to backfill summaries.

---

## Technology Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Storage | Delta Lake | ACID transactions, time travel, schema enforcement |
| Ingestion | Auto Loader (cloudFiles) | Exactly-once file discovery, scalable to millions of files |
| Processing | PySpark Structured Streaming | Unified batch/stream, stateful aggregation, watermarking |
| Deduplication | Stateful window aggregation | Deterministic, auditable, sub-second per micro-batch |
| Correlation | foreachBatch + batch join | Flexible lookback window, full SQL expressiveness |
| Summarization | Databricks SQL ai_summarize() | Managed LLM endpoint, SQL-native, no infra to manage |
