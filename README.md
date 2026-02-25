# Smart Application Monitoring (AMS) — DLT Pipeline

AI-powered application monitoring pipeline built on Databricks Delta Live Tables. Ingests raw alert events from multiple observability sources, normalises and deduplicates them, correlates related alerts into incidents, and produces LLM-enriched incident analysis — all in a continuous **Bronze → Silver → Gold** medallion architecture.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        RAW SOURCES                                  │
│   ┌─────────────────┐          ┌──────────────────────┐             │
│   │  DataDog JSON   │          │  Azure Monitor JSON  │             │
│   │  (nested tags,  │          │  (MS naming conv.,   │             │
│   │   user.email)   │          │   properties.*)      │             │
│   └────────┬────────┘          └──────────┬───────────┘             │
│            │  Auto Loader (cloudFiles)     │                         │
└────────────┼──────────────────────────────┼─────────────────────────┘
             │                              │
             ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (bronze_demo.py)                          [Streaming] │
│                                                                     │
│  bronze_raw_datadog_demo      bronze_raw_azure_demo                 │
│  (source-specific schema)     (source-specific schema)              │
│          │                            │                             │
│          ▼                            ▼                             │
│  bronze_normalized_datadog_demo  bronze_normalized_azure_demo       │
│  (mapped to common schema)        (mapped to common schema)         │
│          │                            │                             │
│          └──────────────┬─────────────┘                             │
│                         ▼                                           │
│                bronze_events_demo                                   │
│          (unified common data model, 652 rows, PII preserved)       │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (silver_demo.py)                          [Streaming] │
│                                                                     │
│  silver_alerts_demo                                                 │
│  (5-min window deduplication, 486 rows)                             │
│          │                                                          │
│          ▼                                                          │
│  silver_incidents_demo                                              │
│  (60-min lookback correlation + LLM context text, 486 rows)         │
│  [stream-static join: new alerts × full Bronze snapshot]            │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (gold_demo.py)                              [Streaming] │
│                                                                     │
│  gold_incidents_demo                                                │
│  (ai_query() fires once per new incident as it arrives:             │
│   summary, root cause, patterns, confidence score,                  │
│   recommended action, 486 rows)                                     │
└─────────────────────────────────────────────────────────────────────┘
```

The pipeline runs in **continuous mode** — no polling interval, no scheduled triggers. Each new alert flows from Bronze through to a Gold LLM analysis in real time.

---

## Pipeline Notebooks & Tables

| Notebook | Layer | Tables Produced | Rows |
|----------|-------|----------------|-----:|
| `bronze_demo.py` | Bronze | `bronze_raw_datadog_demo`, `bronze_raw_azure_demo`, `bronze_normalized_datadog_demo`, `bronze_normalized_azure_demo`, `bronze_events_demo` | 652 |
| `silver_demo.py` | Silver | `silver_alerts_demo`, `silver_incidents_demo` | 486 each |
| `gold_demo.py` | Gold | `gold_incidents_demo` | 486 |

**Catalog:** `akash_s_demo` | **Schema:** `ams` | **Compute:** Serverless + Photon

---

## End-to-End Data Walkthrough

The following traces a single real incident — a **Memory High alert on `payments-api`** — through every layer, using actual records from the pipeline.

---

### Step 1 — Raw File Landing → `bronze_raw_datadog_demo`

A JSONL file is dropped into the DataDog volume by the data generator:

```
/Volumes/akash_s_demo/ams/alert_pipeline/raw_events_demo/datadog/events_203c2a19.jsonl
```

Auto Loader picks it up and stores it in `bronze_raw_datadog_demo` exactly as received — no transformations, full audit trail preserved. The raw event is in DataDog's native nested format:

```json
{
  "@timestamp": "2026-02-20T10:55:13.000Z",
  "alert": {
    "id":       "203c2a19-af4",
    "name":     "Memory High",
    "priority": "high",
    "status":   "triggered"
  },
  "service": { "name": "payments-api" },
  "host":    { "name": "prod-host-05" },
  "metric": {
    "name":      "memory_percent",
    "current":   116.11,
    "threshold": 85.0
  },
  "fingerprint": "3b48cc6d11c408b5",
  "user": {
    "email": "john.doe@email.com",
    "name":  "John Doe",
    "phone": "555-123-4567"
  },
  "client_ip":     "192.168.1.10",
  "error_message": "memory_high threshold breached on payments-api: 116.1 > 85.0",
  "tags": ["application:payments-api", "host:prod-host-05", "environment:production"],
  "_source":       "datadog",
  "_ingested_at":  "2026-02-20T06:20:07.323"
}
```

Similarly, Azure Monitor events land in `bronze_raw_azure_demo` with a completely different structure (`properties.userEmail`, `properties.clientIP`, numeric severity codes instead of strings, etc.).

---

### Step 2 — Normalisation → `bronze_events_demo`

Both sources are mapped to a **single common data model**, flattening nested structs and standardising every field name. The `_source` tag tracks origin throughout.

**DataDog field mapping:**

| Raw Field | Common Field | Notes |
|-----------|-------------|-------|
| `alert.id` | `alert_id` | Unnested |
| `service.name` | `application_id` | Renamed |
| `host.name` | `host_id` | Renamed |
| `alert.name` | `alert_type` | Renamed |
| `alert.priority` (uppercased) | `severity` | Normalised to CRITICAL/HIGH/MEDIUM/LOW |
| `metric.threshold` | `threshold` | Unnested |
| `metric.current` | `current_value` | Unnested |
| `user.email` | `user_email` | PII — preserved |
| `user.name` | `user_name` | PII — preserved |
| `user.phone` | `user_phone` | PII — preserved |
| `client_ip` | `client_ip` | PII — preserved |

**Azure** follows the same target schema from `properties.*` fields, additionally carrying `user_ssn`, `payment_card`, and `session_id` where present.

**Unified record in `bronze_events_demo`:**

```
event_type    : alert
timestamp     : 2026-02-20T10:55:13
application_id: payments-api
host_id       : prod-host-05
alert_id      : 203c2a19-af4
fingerprint   : 3b48cc6d11c408b5
alert_type    : Memory High
severity      : HIGH
threshold     : 85.0
current_value : 116.11
user_email    : john.doe@email.com        ← PII
user_name     : John Doe                  ← PII
user_phone    : 555-123-4567              ← PII
client_ip     : 192.168.1.10              ← PII
error_message : memory_high threshold breached on payments-api: 116.1 > 85.0
_source       : datadog
_ingested_at  : 2026-02-20T06:20:07
```

Both DataDog and Azure events now coexist with identical schemas — **652 rows total**.

---

### Step 3 — Deduplication → `silver_alerts_demo`

Alert systems re-fire continuously while a condition persists. Without deduplication, the same incident generates hundreds of rows. This step collapses them.

**How it works:**
- Events are grouped by `(fingerprint, application_id, host_id, alert_type)` in **5-minute tumbling windows** with a 10-minute watermark for late arrivals
- Within each window, one representative record is retained (`first()` aggregation)
- `suppressed_count` tracks how many duplicates were collapsed
- All PII fields are preserved from the first occurrence

**Deduplicated record in `silver_alerts_demo`:**

```
window_start     : 2026-02-20T10:50:00
window_end       : 2026-02-20T10:55:00
fingerprint      : 3b48cc6d11c408b5
application_id   : payments-api
host_id          : prod-host-05
alert_type       : Memory High
alert_id         : 203c2a19-af4
first_seen       : 2026-02-20T10:55:13
last_seen        : 2026-02-20T10:55:13
suppressed_count : 1
severity         : HIGH
threshold        : 85.0
current_value    : 116.11
_source          : datadog
user_email       : john.doe@email.com
user_name        : John Doe
user_phone       : 555-123-4567
client_ip        : 192.168.1.10
error_message    : memory_high threshold breached on payments-api: 116.1 > 85.0
```

**652 raw events → 486 deduplicated alerts.**

---

### Step 4 — Correlation → `silver_incidents_demo`

A single alert in isolation gives limited context. This step enriches each deduplicated alert by **looking back 60 minutes** across `bronze_events_demo` and joining on every event associated with the same `application_id` — regardless of event type.

#### What "all event types" means

`bronze_events_demo` carries three distinct `event_type` values:

| Event Type | What it is | What it tells you in correlation |
|------------|-----------|----------------------------------|
| `alert` | A threshold breach fired by DataDog or Azure Monitor | How many times has this service already fired in the last hour? A count of 11 prior alerts means this is not a one-off — the service is in sustained distress |
| `log` | Application-emitted log lines — errors, warnings, stack traces | Raw diagnostic evidence: `error_message` fields here can show `OutOfMemoryError`, connection pool exhaustion, GC overhead exceeded, etc. — the textual clues the LLM needs to name a root cause |
| `metric` | Time-series metric samples — CPU %, memory %, request latency, error rate | Trajectory: is memory climbing steadily (slow leak) or did it spike suddenly (burst traffic, OOM kill)? Metric samples from the last 60 minutes tell that story |

The join in `silver_incidents_demo` is a **stream-static join**: new deduplicated alerts stream in from `silver_alerts_demo`, while `bronze_events_demo` is read as a static snapshot for the lookback:

```python
lookback_join = (
    alerts_df.alias("alert")           # ← streaming: new alerts only
    .join(
        bronze_df.alias("bronze"),     # ← static: full Bronze snapshot for lookback
        (col("alert.application_id") == col("bronze.application_id"))
        & (col("bronze.timestamp") >= expr("alert.first_seen - INTERVAL 60 MINUTES"))
        & (col("bronze.timestamp") <= col("alert.first_seen")),
        "left"
    )
)
```

Right now, only `prior_alert_count` is extracted (filtered to `event_type == "alert"`). The full joined dataset — including logs and metrics — is the foundation for the next iteration of the pipeline, where `incident_context_text` will include recent error log snippets and metric trajectories alongside the alert count.

#### How `incident_context_text` is built

After correlation, a Python UDF assembles a plain-text prompt for the LLM:

```
Incident Alert:
{alert_type} triggered on {application_id} @ {host_id}
Current value: {current_value:.2f} | Threshold: {threshold:.2f}

Prior alerts in 60-min window: {prior_alert_count}

Analysis needed: Determine root cause and recommended action for this incident.
```

**Why these specific fields?**

- **`alert_type`** — tells the LLM *what kind* of problem it is (memory high, CPU high, latency high) so it can apply the right SRE heuristic
- **`application_id` + `host_id`** — scope the analysis to one service on one machine; the LLM can reason about whether the problem is service-specific or host-wide
- **`current_value` + `threshold`** — the raw numbers: how far over threshold are we? 116 vs 85 (37% over) tells the LLM this is not a borderline breach — it's a significant overrun
- **`prior_alert_count`** — the single most important correlation signal: 11 prior alerts in 60 minutes means the service is in sustained distress, not a transient spike. This shifts the LLM's recommended action from "wait and see" to "investigate immediately"

**Why plain text (not JSON or SQL)?**

LLMs are pre-trained on human-written text like runbooks, incident tickets, and Slack messages — not relational schemas. A plain-text paragraph that reads like an incident ticket ("Memory High triggered on payments-api, value 116 vs threshold 85, 11 prior alerts in 60 minutes") activates the model's SRE knowledge far more reliably than `{"alert_type": "memory_high", "current_value": 116.11}`. The text format mirrors how a human engineer would write up an incident before calling a colleague for advice.

**Prior alerts found for `payments-api` in the 60-min window: 11**

**Enriched record in `silver_incidents_demo`:**

```
incident_id           : a1b6805a-ed66-4838-8a17-9114ff5c1548
alert_id              : 203c2a19-af4
fingerprint           : 3b48cc6d11c408b5
application_id        : payments-api
host_id               : prod-host-05
alert_type            : Memory High
severity              : HIGH
alert_timestamp       : 2026-02-20T10:55:13
prior_alert_count     : 11

incident_context_text :
  "Incident Alert:
   Memory High triggered on payments-api @ prod-host-05
   Current value: 116.11 | Threshold: 85.00

   Prior alerts in 60-min window: 11

   Analysis needed: Determine root cause and recommended action for this incident."

_source               : datadog
user_email            : john.doe@email.com
user_name             : John Doe
user_phone            : 555-123-4567
client_ip             : 192.168.1.10
error_message         : memory_high threshold breached on payments-api: 116.1 > 85.0
_created_at           : 2026-02-20T06:21:34
```

The `incident_context_text` is the **handoff boundary** between deterministic processing (Bronze + Silver) and the LLM call (Gold). Everything before this line is SQL-deterministic. Everything after uses it as input.

---

### Step 5 — LLM Analysis → `gold_incidents_demo`

The `incident_context_text` is sent to **`databricks-meta-llama-3-3-70b-instruct`** via `ai_query()` with a structured JSON schema response format. The LLM returns exactly five typed fields every time.

**Final enriched record in `gold_incidents_demo`:**

```
incident_id        : a1b6805a-ed66-4838-8a17-9114ff5c1548
alert_id           : 203c2a19-af4
fingerprint        : 3b48cc6d11c408b5
application_id     : payments-api
host_id            : prod-host-05
alert_type         : Memory High
severity           : HIGH
alert_timestamp    : 2026-02-20T10:55:13
prior_alert_count  : 11

──── LLM OUTPUT (structured JSON parsed into typed columns) ────

summary            : "The payments-api on prod-host-05 has triggered a Memory High
                      alert with a current value of 116.11, exceeding the threshold
                      of 85.00. This incident has been preceded by 11 prior alerts
                      within the last 60 minutes, indicating a potential memory leak
                      issue. Immediate investigation and action are required to
                      prevent further performance degradation or potential outage."

patterns           : ["High memory usage",
                      "Repeated alerts within a short time frame",
                      "Specific host (prod-host-05) affected"]

root_cause         : "Memory leak in the payments-api application"

confidence_score   : 0.8

recommended_action : "Investigate the payments-api application logs and metrics on
                      prod-host-05 to identify the source of the memory leak and
                      consider restarting the service or scaling up resources
                      if necessary"

──── PII (admin access only, Unity Catalog column masking for others) ────

user_email         : john.doe@email.com
user_name          : John Doe
user_phone         : 555-123-4567
client_ip          : 192.168.1.10
error_message      : memory_high threshold breached on payments-api: 116.1 > 85.0

_created_at        : 2026-02-20T06:21:34
_analyzed_at       : 2026-02-20T06:21:51
```

---

## Record Count Summary

| Layer | Table | Rows | What Happened |
|-------|-------|-----:|---------------|
| Bronze | `bronze_raw_datadog_demo` | — | DataDog JSONL as-is from Auto Loader |
| Bronze | `bronze_raw_azure_demo` | — | Azure Monitor JSONL as-is from Auto Loader |
| Bronze | `bronze_normalized_datadog_demo` | — | Nested structs flattened to common schema |
| Bronze | `bronze_normalized_azure_demo` | — | `properties.*` fields flattened to common schema |
| Bronze | `bronze_events_demo` | **652** | Both sources unioned into single common model |
| Silver | `silver_alerts_demo` | **486** | 5-min tumbling window deduplication |
| Silver | `silver_incidents_demo` | **486** | 60-min lookback correlation + context text built |
| Gold | `gold_incidents_demo` | **486** | LLM-enriched with root cause analysis |

---

## Key Design Decisions

### Why 3-Tier Bronze (Raw → Normalised → Unified)?

Adding a new source (e.g. New Relic) requires:
1. One `bronze_raw_newrelic` table — 10 lines
2. One `bronze_normalized_newrelic` table — schema mapping, ~50 lines
3. One union line in `bronze_events_demo`
4. **Zero changes** to Silver or Gold

Without this pattern every new source would require changes across all three layers. Estimated saving: 2 days vs 2 weeks per new source.

The raw tables also preserve the original audit trail — if normalization logic changes, you can reprocess from raw without touching production monitoring systems.

### Why Deterministic Fingerprint Deduplication (Not LLM)?

- **Reproducible**: SHA-256 of `(alert_type, app, host)` always yields the same result. Same input = same output. Critical for compliance (SOX, HIPAA)
- **Free**: One Spark `groupBy` in sub-second time vs thousands of LLM calls per window
- **Auditable**: "Why was this alert suppressed?" — same fingerprint, same 5-min window. Transparent and testable
- **Industry standard**: PagerDuty, Prometheus Alertmanager, and Datadog all use deterministic dedup keys for the same reasons

### Why SQL Correlation (Not LLM)?

- The 60-min lookback join runs in milliseconds with proper indexing vs seconds per incident for LLM
- SQL joins are exact and deterministic — same alert always correlates with the same Bronze events
- Cost: Spark scan is DBU compute only. LLM correlation at 209 incidents would add significant token cost per run

### Why LLM at Gold as a Streaming Table?

`gold_incidents_demo` is a DLT streaming table — it reads from `silver_incidents_demo` using `dlt.read_stream()`. This means `ai_query()` fires **exactly once per new incident** the moment Silver emits it. The pipeline runs in **continuous mode**, so there is no polling lag between a new alert arriving and its LLM analysis landing in Gold.

This works because by the time a row reaches Gold's stream, all the expensive deterministic work is already done:
1. **Silver dedup** has collapsed repeated alert firings into one record — `ai_query()` is never called on a duplicate
2. **Silver correlation** has run the 60-min lookback and assembled `incident_context_text` — the LLM receives full context, not a fragment
3. Gold's `ai_query()` call has everything it needs in a single row — one call, one complete answer

Calling the LLM earlier in the pipeline would mean calling it on raw, incomplete data — before deduplication has run, before the 60-minute correlation window has closed. That would produce more calls (one per raw event instead of one per deduplicated incident) and worse answers (no `prior_alert_count`, no full context). Gold streaming gives you the best of both worlds: real-time analysis the moment context is complete.

An added practical benefit: if the LLM endpoint is slow or temporarily unavailable, only Gold gets NULL rows. Bronze and Silver keep running without interruption. Failed rows are retried on the next continuous processing cycle (`failOnError => false`).

### Why Structured JSON Output (Not Free Text)?

Imagine the LLM just returned a paragraph: *"This looks like a memory leak in the payments API, probably caused by a connection pool not being released. I'd recommend restarting the service and checking the GC logs."*

That paragraph is useful if a human reads it. It's useless for everything else:
- You cannot sort 486 incidents by "how confident is the LLM?" when confidence is buried in prose
- You cannot ask "how many incidents this week were caused by memory leaks?" without running another AI call to parse the paragraph
- A Lakeview dashboard cannot put it in a bar chart
- A ticketing system like ServiceNow cannot automatically populate the "root cause" and "recommended action" fields

Structured JSON output forces the LLM to return **typed, named fields every time**:

```
summary           → STRING  (2–3 sentence overview — human-readable)
patterns          → ARRAY   (list of observable signals — machine-queryable)
root_cause        → STRING  (single cause — can GROUP BY this in SQL)
confidence_score  → DOUBLE  (0.0–1.0 — can ORDER BY, filter WHERE >= 0.8)
recommended_action→ STRING  (next step — can be pushed directly to a ticket)
```

Because these are real SQL columns, you can immediately do things like:

```sql
-- Which root cause is most common this week?
SELECT root_cause, COUNT(*) AS incidents
FROM akash_s_demo.ams.gold_incidents_demo
GROUP BY root_cause ORDER BY incidents DESC;

-- Only page on-call for high-confidence critical incidents
SELECT * FROM akash_s_demo.ams.gold_incidents_demo
WHERE severity = 'CRITICAL' AND confidence_score >= 0.8;
```

No post-processing, no regex, no second AI call to parse the first AI call's output. The LLM's reasoning is directly queryable the moment it lands in Delta.

---

## PII Handling

All PII fields are preserved end-to-end and available at Gold for admin users. Apply Unity Catalog column masking for role-based access:

| Field | PII Type | Source | Masked Example |
|-------|----------|--------|---------------|
| `user_email` | EMAIL | Both | `j***@e***.com` |
| `user_name` | PERSON_NAME | Both | `J*** D***` |
| `user_phone` | PHONE_NUMBER | Both | `***-***-4567` |
| `client_ip` | IP_ADDRESS | Both | `***.***.***.10` |
| `user_ssn` | US_SSN | Azure only | `***-**-6789` |
| `payment_card` | CREDIT_CARD | Azure only | `****-****-****-9012` |
| `session_id` | SESSION_ID | Azure only | — |

Unity Catalog auto-classifies these via `ANALYZE TABLE` — no custom regex required.

---

## LLM Configuration

| Parameter | Value | Notes |
|-----------|-------|-------|
| Model | `databricks-meta-llama-3-3-70b-instruct` | Best quality/cost balance, open-source, no vendor lock-in |
| Temperature | `0.1` | Near-deterministic — same incident produces consistent analysis |
| Max tokens | `1024` | Sufficient for all 5 output fields (~300 tokens avg) |
| Response format | Structured JSON schema | 5 typed fields, no parsing edge cases |
| failOnError | `false` | Pipeline continues on LLM failure; NULL rows retried on next cycle |

---

## Running the Pipeline

**1. Generate synthetic data (local machine):**
```bash
cd /Users/akash.s/code/ams.ai
python3 data_generator_demo.py
# Uses Databricks SDK (profile=mobility-attrition)
# Writes to /Volumes/akash_s_demo/ams/alert_pipeline/raw_events_demo/
```

**2. Start the pipeline in continuous mode:**
```bash
databricks pipelines start-update e2b0e890-2c9a-4131-8da2-876f9a440543 --profile=field-east
```

The pipeline runs continuously — Auto Loader picks up new files as they land, Silver deduplicates and correlates in real time, and Gold calls the LLM the moment each fully-formed incident is ready. No manual re-triggering required.

**3. Full refresh (reprocesses all historical files):**
```bash
databricks pipelines start-update e2b0e890-2c9a-4131-8da2-876f9a440543 \
  --profile=field-east --full-refresh
```

**4. Query the Gold table:**
```sql
-- LLM-enriched incidents, most recent first
SELECT application_id, host_id, severity, prior_alert_count,
       summary, root_cause, confidence_score, recommended_action
FROM akash_s_demo.ams.gold_incidents_demo
ORDER BY alert_timestamp DESC;

-- CRITICAL incidents with high LLM confidence
SELECT * FROM akash_s_demo.ams.gold_incidents_demo
WHERE severity = 'CRITICAL'
  AND confidence_score >= 0.8
ORDER BY prior_alert_count DESC;

-- Top root causes
SELECT root_cause, COUNT(*) AS incidents
FROM akash_s_demo.ams.gold_incidents_demo
GROUP BY root_cause
ORDER BY incidents DESC;
```

---

## Troubleshooting

| Symptom | Probable Cause | Fix |
|---------|---------------|-----|
| Silver shows 0 rows | Watermark hasn't closed first 5-min window | Wait 10 min after first Bronze events, or run full-refresh |
| Gold has NULL LLM fields | LLM endpoint unavailable | Check Model Serving status; `failOnError=false` keeps pipeline alive — rows with NULL will be retried on the next continuous cycle |
| Deduplication not working | `fingerprint` missing in Bronze | Verify Bronze events have a `fingerprint` column; check the Silver `groupBy` includes it |
| Correlation is slow | No Z-ordering on Bronze | `OPTIMIZE akash_s_demo.ams.bronze_events_demo ZORDER BY (application_id, timestamp)` |

---

## Contact

**Author**: akash.s@databricks.com
**Pipeline ID**: `e2b0e890-2c9a-4131-8da2-876f9a440543`
**Workspace**: `https://adb-984752964297111.11.azuredatabricks.net`
