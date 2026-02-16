# Unified Alert Pipeline — Databricks MVP

**Ingestion → Deduplication → Correlation → Context Building → LLM Analysis (Platinum)**

A production-style streaming pipeline built on Databricks that ingests operational alerts, deduplicates them, correlates with surrounding telemetry, builds structured context bundles, and generates LLM-powered incident summaries.

---

## Architecture Overview

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  JSONL       │    │   BRONZE     │    │   SILVER     │    │    GOLD      │    │  PLATINUM    │
│  Landing     │───▶│  Raw Events  │───▶│  Deduped     │───▶│  Correlated  │───▶│  LLM-Enriched│
│  Zone        │    │  (Delta)     │    │  Alerts      │    │  Incidents   │    │  Incidents   │
│              │    │              │    │  (Delta)     │    │  (Delta)     │    │  (Delta)     │
│ Auto Loader  │    │ Append-only  │    │ Stateful     │    │ foreachBatch │    │  ai_query()  │
│ cloudFiles   │    │ No transform │    │ Aggregation  │    │ 60-min join  │    │ CTAS batch   │
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
├── 05_llm_analysis.py         # Platinum layer — ai_query() CTAS with structured JSON
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

This creates the `akash_s_demo.ams` schema and all three Delta tables.

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

## Known Behavior: Dedup Window vs. Data Generator Timing

### Why Silver dedup windows appear shorter than 5 minutes

When looking at `silver_alerts`, you may notice that `first_seen_timestamp` to `last_seen_timestamp` spans only **~25–30 seconds** even though the dedup window is configured to 5 minutes. This is **expected behavior** given how the data generator works — it is not a bug in the deduplication logic.

**Root cause: the data generator emits scenario alerts in a compressed burst.**

The continuous data generator (`01_data_generator.py`) injects a full incident scenario every ~2 minutes. During a scenario (e.g., Deployment Incident), it generates all repeated alerts in a tight loop:

```
T+0s   : first CPU alert
T+5s   : repeated CPU alert      ← SCENARIO_EVENT_DELAY = 2s between file writes
T+10s  : repeated CPU alert         + timedelta offsets of ~5s between events
T+15s  : repeated CPU alert
T+20s  : repeated CPU alert
T+25s  : repeated CPU alert
```

All 6 alerts land within a **~25-second window**, not spread over 5 real minutes. The 5-minute tumbling window in Silver correctly groups them (they all fall in the same window), resulting in:

```
fingerprint: ff29103a26f1...
first_seen:  13:26:59
last_seen:   13:27:24    ← only ~25 seconds span
suppressed:  5           ← correct: 6 alerts - 1 kept = 5 suppressed
```

### What would happen with real production data

In a real production environment, monitoring systems like Prometheus, Datadog, or CloudWatch fire alerts at their configured evaluation interval (typically every 30–60 seconds). A CPU > 80% alert would fire like:

```
T+0 min  : CPU alert fires
T+1 min  : CPU alert fires again (same fingerprint)
T+2 min  : CPU alert fires again
T+3 min  : CPU alert fires again
T+4 min  : CPU alert fires again
```

With this spacing, the 5-minute dedup window would show:
```
first_seen:  14:00:00
last_seen:   14:04:00    ← full 4-minute span
suppressed:  4
```

### Why we kept the generator this way

The compressed timing in the generator is a deliberate trade-off for demo purposes:

1. **Fast feedback loop**: A scenario completes in ~30 seconds instead of waiting 5+ real minutes. You can see data flow through Bronze → Silver → Gold → Platinum within a few minutes of starting the pipeline.
2. **Dedup logic is correct regardless of timing**: Whether alerts arrive 5 seconds apart or 60 seconds apart, the fingerprint-based tumbling window groups them identically. The `suppressed_count` is accurate.
3. **Testing scenarios cycle faster**: With `SCENARIO_INTERVAL = 120s`, you see all three scenarios (deployment, memory leak, infra restart) within 6 minutes. At real-time spacing, a single deployment incident would take 35+ minutes to complete.

### How to verify dedup is working correctly

Even with compressed timing, you can confirm correct behavior:

| Check | Expected |
|-------|----------|
| Same fingerprint appears once per 5-min window | Yes — `ff29103a26f1...` never duplicates within one window |
| `suppressed_count` matches (total alerts - 1) | Yes — 6 repeated CPU alerts → suppressed_count = 5 |
| Different fingerprints are NOT merged | Yes — `cpu_high` and `error_rate_high` remain separate rows |
| Alerts crossing a window boundary split into two rows | Yes — visible in the historic data where one fingerprint has two rows |

### If you want real-time spacing

To make the generator spread alerts over actual minutes (matching production behavior), you would need to change the scenario functions to use real `time.sleep(60)` delays between repeated alerts instead of `timedelta(seconds=5)` offsets. This means a single scenario would take ~5 minutes to complete instead of ~25 seconds, and `SCENARIO_INTERVAL` would need to increase to ~10 minutes to avoid overlap.

---

## How Silver Dedup Streaming Works (append mode + watermark)

The Silver dedup stream uses `outputMode("append")` with a tumbling window and watermark. This is the most important streaming concept in the pipeline — understanding it is essential for debugging and tuning.

### The three components

| Component | Current Value | Role |
|-----------|--------------|------|
| **Trigger interval** | `processingTime="30 seconds"` | How often Spark checks Bronze for new alert rows |
| **Tumbling window** | `5 minutes` | Groups alerts by fingerprint into fixed 5-minute buckets |
| **Watermark** | `1 minute` | How long Spark waits for late-arriving data before finalizing a window |

### Step-by-step: what happens each trigger cycle

```
Every 30 seconds (trigger fires):
  1. Read new alert rows from Bronze
  2. For each alert, determine which 5-min window it belongs to
     (e.g., timestamp 13:21:05 → window [13:20:00, 13:25:00))
  3. Update the aggregation in Spark's INTERNAL STATE STORE (in memory):
     - increment count
     - update min/max timestamps
  4. Check: has the watermark closed any window?
     → NO  → Write NOTHING to Silver. State keeps accumulating silently.
     → YES → Emit the finalized window as ONE row to silver_alerts.
             That window's state is then discarded.
```

### When does a window close?

A window is finalized when Spark sees an event with a timestamp **past the window end + watermark delay**. This is the only moment a row appears in the Silver table.

**Example** (5-min window, 1-min watermark):

```
Window: [13:20:00, 13:25:00)
Closes when Spark sees an event with timestamp >= 13:26:00
         (window end 13:25:00 + watermark 1 min)

Timeline:
  Trigger at T+30s:  alert at 13:20:54 → internal state: count=1   → Silver: (nothing)
  Trigger at T+60s:  alert at 13:21:19 → internal state: count=6   → Silver: (nothing)
  Trigger at T+90s:  noise only         → state unchanged           → Silver: (nothing)
  ...
  Trigger at T+Ns:   event at 13:26:01 arrives
                     → watermark passes window end
                     → Window [13:20, 13:25) is FINALIZED
                     → ONE row written: fingerprint=ff29..., suppressed_count=5
                     → Internal state for this window is discarded
```

### Key behaviors to understand

| Question | Answer |
|----------|--------|
| Does Silver read from Bronze every 30 seconds? | **Yes** — the trigger interval controls how often Spark polls for new data |
| Does Silver write to the Delta table every 30 seconds? | **No** — in `append` mode, nothing is written until a window is finalized |
| Are intermediate aggregations visible in the table? | **No** — they exist only in Spark's internal state store (in memory). This is the key difference from `update` mode |
| When does a row appear in Silver? | Only after the watermark passes the window's end boundary |
| What is the actual write latency? | It depends on when the **next event** arrives after the window ends. The minimum latency is the watermark delay (1 minute past window end) |
| What if no new events arrive after the window? | The window stays open in state indefinitely. With `availableNow=True`, Spark closes all windows at the end of the batch |
| Can the same window produce multiple rows? | **No** — in `append` mode, each window emits exactly once |

### Why `append` mode, not `update` mode

The original code used `outputMode("update")`, which caused incorrect `suppressed_count` values:

- In `update` mode, Spark emits **intermediate results** every time the aggregation changes
- Delta Lake's streaming sink treats each "update" as an **append** (Delta doesn't support in-place streaming updates)
- Result: multiple rows per window, with escalating counts (e.g., suppressed_count = 2, 5, 8, 14)
- The final row had the correct count, but all intermediate rows also persisted in the table

With `append` mode:
- Spark accumulates silently in internal state
- Emits **one row per window, one time, with the correct final count**
- Trade-off: slightly higher latency (must wait for watermark to pass)

### Why the watermark matters for `availableNow=True`

When using `trigger(availableNow=True)` (one-shot backfill mode), Spark processes all available Bronze data, then signals end-of-stream. At that point, all remaining open windows are finalized and flushed — even if the watermark hasn't technically passed them yet. This is why `availableNow=True` produces complete results for historic data.

With `trigger(processingTime="30 seconds")` (continuous mode), there is no end-of-stream signal. Windows can only close via the watermark — so you **must** have new events arriving with later timestamps to push the watermark forward and close older windows.

---

## How Gold Correlation & Context Building Works

The Gold layer (`04_gold_correlation.py`) takes each deduplicated alert from Silver and enriches it with surrounding telemetry from Bronze, then assembles a structured text bundle (`incident_context_text`) for the LLM.

### Overview

```
Silver alert (new deduplicated alert)
    │
    ├── 1. Join against Bronze (60-min lookback window)
    │      └── same application_id + timestamp in [alert_time - 60 min, alert_time]
    │
    ├── 2. Aggregate from the joined data:
    │      ├── Deployments  →  JSON array
    │      ├── Error Logs   →  JSON map {error_code: count}
    │      ├── Metrics      →  max/avg CPU, max memory, max error rate
    │      └── Prior Alerts →  count of other alerts in window
    │
    ├── 3. Build incident_context_text (structured plain text)
    │
    └── 4. MERGE into gold_incidents (idempotent upsert)
```

### Step 1: Lookback Join

Every new Silver alert is joined against the full Bronze table:

```sql
alert.application_id = bronze.application_id
AND bronze.timestamp >= alert.first_seen_timestamp - INTERVAL 60 MINUTES
AND bronze.timestamp <= alert.first_seen_timestamp
```

This produces a wide dataset of all Bronze events (metrics, logs, deployments, alerts) that occurred in the 60 minutes leading up to the alert.

### Step 2: Aggregations

Each aggregation filters the lookback join for a specific event type, groups by fingerprint, and produces a summary column.

#### Deployments (`correlated_deployments`)

```
lookback_join
  → filter: event_type = 'deployment'
  → groupBy: fingerprint
  → collect_list(struct(deployment_id, version, change_type, timestamp))
  → to_json(...)
```

Result: a JSON array like `[{"deployment_id":"dep-abc","version":"v2.3.1","change_type":"rolling_update","timestamp":"2026-02-14T10:05:00"}]`

If no deployments exist in the window, this is `NULL` (left join).

#### Error Logs (`error_summary`)

```
lookback_join
  → filter: event_type = 'log' AND log_level = 'ERROR' AND error_code IS NOT NULL
  → groupBy: fingerprint, error_code
  → count(*)
  → collapse into: map_from_arrays(error_codes, counts)
  → to_json(...)
```

Result: a JSON map like `{"TimeoutException":"12","DBConnectionError":"4"}`

#### Metrics (`metric_summary`)

```
lookback_join
  → filter: event_type = 'metric'
  → groupBy: fingerprint
  → agg:
      max(cpu_percent)     → cpu_max
      avg(cpu_percent)     → cpu_avg
      max(memory_percent)  → memory_max
      max(error_rate)      → error_rate_max
  → to_json(struct(...))
```

Result: a JSON object like `{"cpu_max":94.5,"cpu_avg":72.1,"memory_max":68.2,"error_rate_max":0.15}`

#### Prior Alerts (`prior_alert_count`)

```
lookback_join
  → filter: event_type = 'alert' AND alert_id != current alert's alert_id
  → groupBy: fingerprint
  → count(*)
```

Result: an integer count of other alerts for the same app in the window.

### Step 3: Context Bundle Builder (`incident_context_text`)

A Python UDF takes all the aggregated columns and assembles them into a fixed-format plain-text string:

```
Incident Alert:
cpu_high triggered on payments-api host-a
Current value: 92.30 Threshold: 80.00

Recent Deployments:
  - v2.3.1 deployed at 2026-02-14T10:05:00 (rolling_update)

Error Summary (last 60 mins):
  - TimeoutException: 12 occurrences
  - DBConnectionError: 4 occurrences

Metric Summary:
  - CPU max: 94.50%
  - CPU avg: 72.10%
  - Memory max: 68.20%
  - Error rate max: 0.15

Prior alerts in window: 3
```

The UDF parses the JSON columns back into Python objects (lists/dicts) and formats each section. If a section has no data (e.g., no deployments), it outputs `"None"`.

**Why a UDF and not SQL?** The formatting requires conditional logic (null checks, list iteration, number formatting) that would be verbose and fragile in pure SQL `CONCAT()` expressions.

### Step 4: MERGE to Gold

The final DataFrame is written using a MERGE (upsert) on `fingerprint + alert_timestamp`:

```sql
MERGE INTO gold_incidents AS target
USING new_incidents AS source
ON target.fingerprint = source.fingerprint
   AND target.alert_timestamp = source.alert_timestamp
WHEN NOT MATCHED THEN INSERT *
```

This ensures **idempotency** — reprocessing the same micro-batch (e.g., after a restart) won't create duplicate incidents.

### Column Disambiguation Note

After joining alerts with Bronze and then joining multiple aggregation DataFrames back, Spark can encounter ambiguous `fingerprint` columns (the same column name from multiple sources). This is especially strict in Spark Connect.

**Fix**: Each aggregation renames its `groupBy` key to a unique alias (`_fp_dep`, `_fp_err`, `_fp_met`, `_fp_pri`), joins on that alias, then drops it. This leaves only one `fingerprint` column from the original alerts DataFrame — zero ambiguity.

### Why `incident_context_text` Exists

This string is the **handoff boundary between deterministic and stochastic processing**:

- **Gold layer** (deterministic): builds `incident_context_text` using pure Spark operations — joins, aggregations, formatting. Same input always produces the same output.
- **Platinum layer** (stochastic): passes this exact string to `ai_query()` for LLM analysis. Different runs may produce different summaries.

If the LLM gives a bad summary, you can inspect `incident_context_text` in the Gold table to verify whether the input was correct — full auditability without re-running the correlation pipeline.

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

`ai_query()` is used instead of `ai_summarize()` and is deliberately separated from the streaming pipeline, running as a batch SQL UPDATE:

- **Structured output**: `ai_query()` supports `responseFormat` with JSON schema, so the LLM returns typed fields (summary, patterns, root_cause, confidence_score, recommended_action) directly — no regex parsing needed.
- **Model choice**: `ai_query()` lets you pick any Model Serving endpoint (e.g., `databricks-meta-llama-3-3-70b-instruct`).
- **Cost control**: In streaming, every micro-batch would fire LLM calls — potentially hundreds per hour during an incident storm. Batch mode processes only `WHERE summary IS NULL` rows, naturally deduplicating work.
- **Latency isolation**: LLM calls take 2–10 seconds. Inside the streaming DAG, they would block correlation processing, increasing end-to-end latency for dashboards and PagerDuty integrations.
- **Retry safety**: LLM calls can fail (rate limits, model timeouts). In streaming, a failed micro-batch retries ALL records. In batch mode, the idempotent `WHERE summary IS NULL` clause means only unfilled rows are retried. Using `failOnError => false` ensures one bad row doesn't kill the entire batch.
- **Deterministic/stochastic separation**: Bronze → Silver → Gold is deterministic (same input → same output). LLM analysis is stochastic (different runs may produce different text). Keeping them in separate execution contexts means the deterministic pipeline can be independently validated and tested.

### Why Bronze / Silver / Gold Separation Matters

The medallion architecture is not just organizational — it provides critical operational guarantees:

| Layer | Purpose | Key Property |
|-------|---------|-------------|
| **Bronze** | Raw, immutable event log | **Reprocessable**: If Silver or Gold logic has a bug, we can recompute from Bronze without re-ingesting |
| **Silver** | Cleaned, deduplicated alerts | **Noise-free**: Downstream consumers see only meaningful signals, not repeated firings |
| **Gold** | Business-level incidents | **Enriched**: Each incident has full deterministic context, ready for dashboards and Platinum LLM layer |
| **Platinum** | LLM-analyzed incidents | **AI-Enriched**: Gold + summary, patterns, root cause, confidence, recommended action via ai_query() |

Without this separation:
- A bug in dedup logic could corrupt raw data (no Bronze isolation)
- Alert storms would flood LLM summarization (no Silver filtering)
- Re-processing would require re-ingesting from source systems (no immutable Bronze)

### Why `ai_query()` Should Not Be Inside the Streaming DAG

Beyond the cost and latency reasons above, there is an architectural principle: **streaming DAGs should be side-effect-free and deterministic**.

`ai_query()` has external side effects (API call to a model endpoint) and is non-deterministic (same prompt may produce different responses). Placing it inside a streaming DAG means:

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
| `BASE_PATH` | `/Volumes/akash_s_demo/ams/alert_pipeline` | Root storage path (UC Volume) |
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
Task 4: 05_llm_analysis.py      (scheduled every 5 min — CTAS Platinum)
```

Tasks 1–3 run as long-running streaming jobs. Task 4 runs on a schedule and fully rebuilds the Platinum table via `CREATE OR REPLACE TABLE ... AS SELECT`.

---

## Technology Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Storage | Delta Lake | ACID transactions, time travel, schema enforcement |
| Ingestion | Auto Loader (cloudFiles) | Exactly-once file discovery, scalable to millions of files |
| Processing | PySpark Structured Streaming | Unified batch/stream, stateful aggregation, watermarking |
| Deduplication | Stateful window aggregation | Deterministic, auditable, sub-second per micro-batch |
| Correlation | foreachBatch + batch join | Flexible lookback window, full SQL expressiveness |
| LLM Analysis | Databricks SQL ai_query() | Structured JSON output, model choice, failOnError support |
