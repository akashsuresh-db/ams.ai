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

## Deep Dive: Deduplication Logic

### What the code does

The Silver layer (`03_silver_dedup.py`) implements **fingerprint-based tumbling-window deduplication**. Here is the exact logic:

```
Bronze alerts (streaming)
    │
    ├── 1. Filter: event_type = 'alert'
    ├── 2. Watermark on timestamp (1 minute delay)
    ├── 3. GroupBy: fingerprint + application_id + host_id + alert_type
    │              + window(timestamp, "5 minutes")
    ├── 4. Aggregate:
    │      first(alert_id)      → canonical alert ID to keep
    │      first(severity)      → severity of the first occurrence
    │      first(threshold)     → threshold of the first occurrence
    │      first(current_value) → value of the first occurrence
    │      min(timestamp)       → first_seen_timestamp
    │      max(timestamp)       → last_seen_timestamp
    │      count(*) - 1         → suppressed_count
    │
    └── 5. Write ONE row per fingerprint per 5-min window
```

### What is a fingerprint?

The fingerprint is a deterministic hash computed by the **data generator** at alert creation time:

```python
def fingerprint(alert_type, app, host):
    raw = f"{alert_type}|{app}|{host}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
```

This means: every `cpu_high` alert on `payments-api` `host-a` produces the **same** fingerprint, regardless of when it fires or what the current CPU value is. The Silver layer uses this fingerprint as the grouping key.

**Critical implication**: The fingerprint is computed at the **source** (the alerting system or data generator), not by the pipeline. In production, your monitoring system (Prometheus Alertmanager, Datadog, PagerDuty, etc.) should include a fingerprint field in the alert payload. If it doesn't, you would compute one in Bronze or Silver using the same deterministic hash logic.

### Why this is the standard pattern

This approach — deterministic key + time window — is used by virtually every production alert pipeline (Prometheus Alertmanager's `group_by`, PagerDuty's dedup key, Datadog's aggregation key). The reasons:

**1. Reproducibility**

Re-running the pipeline on the same Bronze data produces exactly the same Silver output. The SHA-256 fingerprint and the 5-minute tumbling window are both deterministic functions of the input data. This is essential for:
- Debugging: "Why was alert X suppressed?" → look at the fingerprint and window boundaries
- Compliance: SOX/HIPAA/SOC2 auditors can verify the suppression logic
- Backfill: If Silver has a bug, truncate it, fix the code, reprocess from Bronze, get identical results

**2. No false negatives**

The fingerprint groups alerts by `(alert_type, application_id, host_id)`. Alerts that **should** be different (different type, different app, or different host) will always have different fingerprints. There's no fuzzy matching that could accidentally merge unrelated alerts.

**3. Predictable state management**

Spark's stateful aggregation with watermarking guarantees bounded memory: once a window closes (watermark passes), its state is discarded. Without this, the state store grows unbounded with every new fingerprint, eventually causing OOM.

**4. Performance**

The entire dedup operation is a single `groupBy` + `agg` — a highly optimized operation in Spark. No external calls, no lookups, no shuffles beyond the groupBy. Each micro-batch completes in sub-second time.

### Alternatives we considered and why they were rejected

| Alternative | Why rejected |
|-------------|-------------|
| **`dropDuplicatesWithinWatermark()`** | Simpler API, but cannot track `suppressed_count` or `last_seen_timestamp`. You lose visibility into how noisy a particular alert is. |
| **LLM-based dedup** ("are these two alerts about the same issue?") | Stochastic, expensive, slow (2-10s per comparison), and a pair-wise comparison would be O(n²) for n alerts in a window. |
| **Probabilistic (MinHash / LSH)** | Useful for near-duplicate text (e.g., log messages), but alerts already have a structured identity — no need for approximate matching. |
| **Session windows** (instead of tumbling) | Session windows group events by gaps in activity. This would merge alerts across arbitrary time spans if they keep firing, making the window size unpredictable and state unbounded. Tumbling windows give a fixed, predictable window. |
| **Sliding windows** | Creates overlapping windows, meaning the same alert could appear in multiple windows. This breaks the "one row per alert occurrence" guarantee. |

### How it handles edge cases

| Edge case | Behavior |
|-----------|----------|
| Same fingerprint fires in two different 5-min windows | Two separate Silver rows — one per window. Both are valid incidents. |
| An alert arrives late (after the watermark passes) | Dropped silently — Spark cannot reopen a closed window. The watermark delay (1 minute) is the tolerance. |
| Different hosts fire the same alert_type for the same app | Different fingerprints (host is part of the hash). They are NOT deduplicated against each other. |
| An alert fires only once in a window | One Silver row with `suppressed_count = 0`. |
| Thousands of alerts fire in one window | One Silver row with `suppressed_count = N-1`. Spark handles this in memory; no data written until window closes. |

---

## Deep Dive: Correlation Logic

### What the code does

The Gold layer (`04_gold_correlation.py`) uses a `foreachBatch` pattern to enrich each new Silver alert with surrounding context from Bronze. For each alert, it answers: **"What was happening in this application in the 60 minutes before the alert fired?"**

### Why `foreachBatch` + batch join is the best approach

The core operation is a **stream-to-batch join**: new Silver alerts (streaming) joined against the full Bronze table (batch). Spark Structured Streaming's native join semantics don't support this well for several reasons:

**1. Arbitrary lookback windows**

Spark's stream-stream joins require you to define the time range in the join condition. But our lookback is relative to each alert's timestamp (not a fixed wall-clock range), and we need to scan 60 minutes of history. In a stream-stream join, this would require Spark to hold 60 minutes of Bronze state in memory for every application — potentially millions of rows.

With `foreachBatch`, we do a simple batch read of Bronze per micro-batch. Spark scans the Delta table efficiently (partition pruning, Z-ordering, file skipping) without holding state.

**2. Multiple aggregations from the same join**

The correlation produces four separate aggregations (deployments, errors, metrics, prior alerts) from the same lookback join. In a stream-stream join, you'd need four separate streaming queries or a complex flatMap. With `foreachBatch`, it's just four DataFrame operations on the same joined result — clean and readable.

**3. MERGE-based idempotency**

The Gold write uses `MERGE INTO ... WHEN NOT MATCHED THEN INSERT *` on `(fingerprint, alert_timestamp)`. This makes reprocessing safe: if a micro-batch is retried (e.g., after a node failure), the same alert won't create a duplicate incident. Streaming writes with `append` mode don't have this guarantee.

**4. No state store overhead**

`foreachBatch` processes each micro-batch as a standalone batch job. There's no state store to manage, no watermark to tune, no risk of state corruption. The only state is the streaming checkpoint, which tracks "which Silver rows have I already processed?"

### Alternatives considered

| Alternative | Why rejected |
|-------------|-------------|
| **Stream-stream join** (Silver stream × Bronze stream) | Requires holding 60 minutes of Bronze data in state for every app. Memory-intensive, hard to tune, and doesn't support multiple aggregations from the same join cleanly. |
| **Stream-static join** (native Spark API) | Spark's built-in stream-static join re-reads the static table on every micro-batch, which is what `foreachBatch` does anyway. But `foreachBatch` gives us control over the MERGE write and multi-aggregation logic. |
| **Materialized view / pre-aggregated Bronze** | Would require maintaining a separate materialized view of Bronze aggregations per application per time window. Adds complexity and another table to manage. |
| **LLM-based correlation** ("find related events") | Stochastic, expensive, and the LLM cannot efficiently scan thousands of Bronze rows. Correlation is a data engineering operation, not a reasoning task. |

### How the lookback join works in detail

The join condition scopes Bronze events to the **same application** and **60 minutes before the alert**:

```sql
alert.application_id = bronze.application_id
AND bronze.timestamp >= alert.first_seen_timestamp - INTERVAL 60 MINUTES
AND bronze.timestamp <= alert.first_seen_timestamp
```

**Important design choice**: The join is on `application_id` only, not `host_id`. This is deliberate — a deployment on `host-a` can cause alerts on `host-b` (e.g., load balancer shifts traffic). If you want host-level isolation, you would add `host_id` to the join condition.

**Another design choice**: The lookback is **backward-only** (before the alert). We don't look at events after the alert. In production, you might want a forward window too (e.g., "what happened in the 15 minutes after the alert fired?") to capture recovery events. This would require changing the join condition and potentially the trigger timing.

---

## Adapting for Real-World Scenarios — All Tunables

This pipeline is built for demo/MVP purposes with compressed timing and simplified assumptions. Below is an exhaustive list of every value, assumption, and design choice you'd need to change for a production deployment, organized by file.

### 1. Data Generator (`01_data_generator.py`)

| Parameter | Current Value | What it controls | Real-world change |
|-----------|--------------|------------------|-------------------|
| `EMIT_INTERVAL` | `5` seconds | How often noise events are written | In production, this is your monitoring agent's collection interval (15s–60s for Prometheus, 10s for Datadog). Change to match your actual telemetry cadence. |
| `NOISE_BATCH_SIZE` | `5` events | Events per noise batch | Production systems generate thousands of metrics per second per host. Scale up to simulate realistic volume. |
| `SCENARIO_INTERVAL` | `120` seconds | Time between incident injections | Real incidents are rare: maybe 2-5 per day per service. Change to hours for realistic simulation. |
| `SCENARIO_EVENT_DELAY` | `2` seconds | Delay between event files during a scenario | Controls how quickly a scenario unfolds. Real incidents unfold over minutes to hours, not seconds. |
| `timedelta(seconds=5)` in repeated alerts | `5` seconds between alerts | Spacing between repeated alert firings within a scenario | Real monitoring systems fire alerts every 30–60 seconds (Prometheus evaluation interval). Change to `timedelta(seconds=60)` for realistic spacing. **This directly affects how `suppressed_count` appears in Silver.** |
| `APPLICATIONS` | `["payments-api", "orders-api", "auth-service"]` | Number and names of services | Production may have 50–500 microservices. Add more to test scalability and cross-service correlation. |
| `HOSTS` | `["host-a", "host-b"]` | Number of hosts per service | Production has 10–100+ hosts per service. More hosts = more unique fingerprints = more Silver rows. |
| CPU threshold in `make_alert` | `80.0` | When CPU alerts fire | Match your actual alerting thresholds. Many teams use 70%, 85%, or 90%. |
| Memory threshold | `85.0` | When memory alerts fire | Same — match your runbooks. |
| Error rate threshold | `0.05` | When error rate alerts fire | Varies widely: 1% for user-facing, 5% for batch jobs, 0.1% for payment systems. |
| Deployment frequency | One per scenario cycle (~2 min) | How often deployments happen | Real teams deploy 1–20 times per day per service, not every 2 minutes. Decouple deployments from incidents. |
| Version format | `v{major}.{minor}.{patch}` random | Version strings | Match your actual versioning (semantic, date-based, commit SHA). |

**The stateless alert model** — the most significant simplification:

The current generator fires alerts **without any state**. There is no "resolved" event. In production, monitoring systems track alert lifecycle:

```
FIRING  →  FIRING  →  FIRING  →  RESOLVED
```

Each firing has the same fingerprint, but the final RESOLVED event signals the condition has ended. Our pipeline has no concept of resolution. To add it:

1. Add a `status` field to the alert schema (`FIRING` / `RESOLVED`)
2. In the data generator, emit a `RESOLVED` event when metrics return to normal
3. In Silver dedup, track `last_status` alongside `suppressed_count`
4. In Gold, the context text should indicate whether the alert is still active or resolved
5. In Platinum, the LLM prompt should distinguish between ongoing and resolved incidents

**Files to change**: `01_data_generator.py` (add resolved events), `00_setup.py` (add `status` column to Bronze and Silver schemas), `03_silver_dedup.py` (track resolution state), `04_gold_correlation.py` (include status in context text).

### 2. Bronze Ingestion (`02_bronze_ingestion.py`)

| Parameter | Current Value | What it controls | Real-world change |
|-----------|--------------|------------------|-------------------|
| `processingTime` | `"10 seconds"` | How often Auto Loader polls for new files | Depends on your SLA. For near-real-time: 5–10s. For cost optimization: 30–60s. Lower = lower latency but more frequent cluster wake-ups. |
| `maxFilesPerTrigger` | `10` | Files processed per micro-batch | Prevents overwhelming the cluster during backfill. For production with thousands of files arriving per minute, increase to 100–1000. |
| `cloudFiles.inferColumnTypes` | `"false"` | Whether Auto Loader infers types | Keep `false` in production. Schema inference can silently change types between batches if the source format changes. |
| Explicit schema | Hardcoded in `bronze_schema` | Expected JSON structure | In production, schemas evolve. Consider using `cloudFiles.schemaEvolutionMode = "addNewColumns"` and `mergeSchema = true` to handle new fields gracefully. |
| `trigger(availableNow=True)` vs `processingTime` | Both available, one commented | One-shot vs continuous | Use `processingTime` for production streaming. Use `availableNow=True` for backfill or testing only. |

### 3. Silver Deduplication (`03_silver_dedup.py`)

| Parameter | Current Value | What it controls | Real-world change |
|-----------|--------------|------------------|-------------------|
| `DEDUP_WINDOW_MINUTES` | `5` | Tumbling window size for alert grouping | **This is the most critical tunable.** It defines "how long do we consider repeated alerts as the same incident?" Options: |
| | | | - **5 minutes**: Good for fast-firing alerts (every 30s). Groups ~10 repetitions into one. |
| | | | - **15 minutes**: Better for slower alerting systems (every 1-2 min) or when incidents typically take 10-15 min to escalate. |
| | | | - **30-60 minutes**: For systems where you want ONE incident per alerting episode, regardless of how long it persists. Risk: very long-running alerts become one mega-row. |
| | | | **Rule of thumb**: Set the window to 2-3x your monitoring system's evaluation interval, or match your team's typical "initial triage" time. |
| `WATERMARK_DELAY` | `"1 minutes"` | How long Spark waits for late-arriving data | Must be >= your maximum expected event latency. In production: |
| | | | - Same-region Kafka/Event Hub: 30 seconds–2 minutes |
| | | | - Cross-region replication: 5–10 minutes |
| | | | - Batch file drops (S3/ADLS): 15–30 minutes |
| | | | **Trade-off**: Larger watermark = more memory (state store holds open windows longer) but fewer dropped late events. Smaller watermark = lower latency but late events are silently dropped. |
| `processingTime` | `"30 seconds"` | How often Silver reads from Bronze | Should be >= Bronze trigger interval. No point reading Silver faster than Bronze writes. In production: 30–60 seconds is typical. |
| `first("severity")` | Keeps first occurrence's severity | Which severity to record | In production, you may want `max("severity")` (keep the worst) or `last("severity")` (keep the most recent). If an alert escalates from HIGH to CRITICAL, `first()` would miss the escalation. |
| `first("current_value")` | Keeps first occurrence's value | Which metric value to record | Same — you may want `max("current_value")` for CPU/memory (worst case) or `last("current_value")` for the most current reading. |
| groupBy includes `host_id` | Yes | Whether dedup is per-host | If you want to deduplicate across all hosts (same alert_type + app, any host = one incident), remove `host_id` from groupBy. Trade-off: fewer Silver rows but less granularity. |

### 4. Gold Correlation (`04_gold_correlation.py`)

| Parameter | Current Value | What it controls | Real-world change |
|-----------|--------------|------------------|-------------------|
| `CORRELATION_LOOKBACK_MINUTES` | `60` | How far back to search for related events | Depends on your incident patterns: |
| | | | - **30 minutes**: For fast-moving incidents (deployment → alert in < 30 min) |
| | | | - **60 minutes** (current): Covers most deployment incidents and gradual degradation |
| | | | - **120–240 minutes**: For slow memory leaks or gradual capacity exhaustion |
| | | | **Trade-off**: Larger window = more Bronze rows scanned per alert = slower correlation. Use Delta's Z-ordering on `(application_id, timestamp)` to mitigate. |
| `processingTime` | `"60 seconds"` | How often Gold processes new Silver rows | Gold involves a batch JOIN against Bronze (expensive). Longer intervals reduce redundant reads. In production: 60–300 seconds. |
| Join on `application_id` only | `alert.application_id = bronze.application_id` | Scope of related events | The current join finds ALL events for the same app, across ALL hosts. If your services are host-isolated (no cross-host impact), add `host_id` to the join condition to reduce scan size and improve relevance. |
| Backward-only lookback | `bronze.timestamp <= alert.first_seen_timestamp` | Whether to include post-alert events | Only looks at events BEFORE the alert. To include recovery events (e.g., "deployment rolled back 10 minutes later"), change to: `bronze.timestamp <= alert.first_seen_timestamp + INTERVAL 15 MINUTES`. This requires the Gold stream to wait longer before processing. |
| MERGE key | `fingerprint + alert_timestamp` | Idempotency key | If you change the Silver groupBy (e.g., remove host_id), this key may need adjustment to avoid false matches. |
| Error log filter | `log_level = 'ERROR'` only | Which logs are included in error_summary | You may also want `WARN` level logs for context. Change the filter to `log_level IN ('ERROR', 'WARN')`. |
| Metric aggregation | `max(cpu)`, `avg(cpu)`, `max(memory)`, `max(error_rate)` | Which metrics are summarized | Add `min()` for baseline comparison, `stddev()` for volatility, or `percentile_approx()` for P95/P99. Add more metric names if your telemetry has `request_latency_ms`, `queue_depth`, etc. |

### 5. LLM Analysis (`05_llm_analysis.py`)

| Parameter | Current Value | What it controls | Real-world change |
|-----------|--------------|------------------|-------------------|
| `LLM_ENDPOINT` | `"databricks-gpt-oss-120b"` | Which model analyzes incidents | Change to any Model Serving endpoint in your workspace. Smaller models (7B) are cheaper/faster but less accurate. Larger models (70B, GPT-4) give better analysis but cost more. |
| `temperature` | `0.1` | LLM creativity/randomness | `0.0`–`0.1` for consistent, near-deterministic analysis. `0.3`–`0.5` for more varied recommendations. |
| `max_tokens` | `1024` | Maximum response length | Increase if summaries are being truncated. Decrease for cost savings. |
| `failOnError => false` | Swallows LLM failures | Error handling strategy | In production, you may want `true` if every incident MUST have an analysis. With `false`, failed rows have NULL LLM fields and can be retried. |
| `CREATE OR REPLACE TABLE` | Full rebuild every run | Incremental vs full rebuild | For large Gold tables (100K+ rows), consider incremental: query only `gold_incidents WHERE _created_at > (SELECT MAX(_created_at) FROM platinum_incidents)` and INSERT into Platinum. The CTAS approach rescans and re-analyzes everything, which costs LLM tokens for already-analyzed rows. |
| LLM prompt | Hardcoded in SQL | What the LLM is asked to analyze | Customize for your domain: add runbook references, team escalation paths, known-issue databases. The more specific the prompt, the more actionable the output. |
| `responseFormat` JSON schema | Fixed 5 fields | Structure of LLM output | Add fields like `affected_services`, `estimated_impact`, `similar_incidents`, or `mitigation_steps` if your SRE team needs them. |

### 6. Config (`config.py`)

| Parameter | Current Value | What it controls | Real-world change |
|-----------|--------------|------------------|-------------------|
| `CATALOG` | `"akash_s_demo"` | Unity Catalog name | Change to your organization's catalog (`prod_ops`, `platform`, etc.) |
| `SCHEMA` | `"ams"` | Schema name | Change to your team's schema |
| `BASE_PATH` | `/Volumes/akash_s_demo/ams/alert_pipeline` | Volume path for files | Change catalog/schema/volume names to match your environment |

### 7. Pipeline-Wide Assumptions

These are architectural assumptions baked into the design that you'd revisit for production:

| Assumption | Current behavior | Real-world alternative |
|------------|-----------------|----------------------|
| **Alerts are stateless** | No FIRING → RESOLVED lifecycle | Add `status` field, track transitions, create resolved incidents |
| **One alert type per fingerprint** | fingerprint = hash(type + app + host) | Include additional dimensions: `environment`, `region`, `cluster_id`, `namespace` |
| **Deployments are correlated by app only** | Any deployment on the same app in 60 min is correlated | Filter by `environment` or `change_type` to reduce false correlations |
| **No severity escalation** | First severity is kept | Track max severity across the window, or detect escalation pattern (MEDIUM → HIGH → CRITICAL) |
| **No cross-service correlation** | Each alert correlates with its own app's events only | For microservice architectures, correlate across dependent services (e.g., payments-api alert → check orders-api too) using a service dependency graph |
| **No human feedback loop** | LLM analysis is one-shot | Add a feedback table where SREs rate LLM accuracy, use for fine-tuning or prompt improvement |
| **Fixed lookback window** | Always 60 minutes | Adaptive window based on alert type (deployment alerts → 120 min, memory alerts → 240 min, network alerts → 15 min) |
| **Batch LLM (full rebuild)** | `CREATE OR REPLACE TABLE` every run | Incremental: only analyze new Gold rows, append to Platinum. Much cheaper at scale. |
| **No alert grouping across types** | `cpu_high` and `error_rate_high` on the same app are separate incidents | In production, these may be the same incident. Consider a secondary grouping step that merges related alerts by `(application_id, time_proximity)` into a single incident. |

### Quick Reference: "I want to simulate X"

| Scenario | What to change |
|----------|---------------|
| **Alerts fire every 60 seconds (like Prometheus)** | `01_data_generator.py`: Change `timedelta(seconds=5)` to `timedelta(seconds=60)` in all scenario repeated-alert loops. Increase `SCENARIO_INTERVAL` to 600+ seconds. |
| **Dedup window matches a 15-minute triage cycle** | `03_silver_dedup.py`: Change `DEDUP_WINDOW_MINUTES = 15`. Consider increasing `WATERMARK_DELAY` to `"3 minutes"`. |
| **Deployments happen once a day, not every 2 minutes** | `01_data_generator.py`: Remove deployments from the noise loop. Create a separate deployment injection schedule (e.g., once every `SCENARIO_INTERVAL * 10`). |
| **Alerts have RESOLVED state** | `01_data_generator.py`: Add `make_alert(..., status="RESOLVED")` events after metrics normalize. `00_setup.py`: Add `status STRING` to Bronze and Silver. `03_silver_dedup.py`: Track `last("status")`. |
| **50 microservices, 20 hosts each** | `01_data_generator.py`: Expand `APPLICATIONS` list to 50 entries, `HOSTS` to 20 entries. Increase `NOISE_BATCH_SIZE` proportionally. |
| **Memory leak that takes 4 hours** | `01_data_generator.py`: Change `scenario_memory_leak_rt()` to spread memory increases over `timedelta(minutes=10)` intervals. Requires real `time.sleep()` or much longer `SCENARIO_INTERVAL`. `04_gold_correlation.py`: Increase `CORRELATION_LOOKBACK_MINUTES = 240`. |
| **Cross-region event latency (5–10 min late arrivals)** | `03_silver_dedup.py`: Change `WATERMARK_DELAY = "10 minutes"`. Accept higher Silver write latency. |
| **LLM analyzes only new incidents (incremental)** | `05_llm_analysis.py`: Replace `CREATE OR REPLACE TABLE` with `INSERT INTO platinum_incidents SELECT ... FROM gold_incidents WHERE _created_at > (SELECT COALESCE(MAX(_created_at), '1970-01-01') FROM platinum_incidents)`. |
| **Host-level isolation (no cross-host correlation)** | `04_gold_correlation.py`: Add `AND alert.host_id = bronze.host_id` to the lookback join condition. |
| **Worst-case severity tracking** | `03_silver_dedup.py`: Change `first("severity")` to a custom UDF that orders CRITICAL > HIGH > MEDIUM > LOW and keeps the maximum. |

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
