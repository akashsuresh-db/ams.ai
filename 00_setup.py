# Databricks notebook source
# =========================================================
# 00_setup.py — Schema & Table Setup
# =========================================================
#
# PURPOSE:
#   Creates the catalog, schema, and Delta tables needed
#   by the pipeline.  Run this ONCE before starting the
#   streaming jobs.
#
# DESIGN DECISIONS:
#   - Uses Unity Catalog (catalog.schema.table) for
#     governance and lineage tracking.
#   - Delta Lake is the storage format for ACID guarantees,
#     time travel, and schema enforcement.
#   - Tables are created as MANAGED tables so that DROP
#     TABLE also removes the underlying files.
# =========================================================

# COMMAND ----------

# %run ./config

# COMMAND ----------

# ---------------------------------------------------------
# 1. CREATE CATALOG AND SCHEMA
# ---------------------------------------------------------
# Uncomment if you have CREATE CATALOG privileges.
# Otherwise ask your workspace admin to create these.

spark.sql("CREATE CATALOG IF NOT EXISTS akash_s_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS akash_s_demo.ams")
spark.sql("USE CATALOG akash_s_demo")
spark.sql("USE SCHEMA ams")

# COMMAND ----------

# ---------------------------------------------------------
# 2. CREATE VOLUME FOR FILE STORAGE
# ---------------------------------------------------------
# Unity Catalog Volumes replace legacy mount points (/mnt/).
# Volumes provide governed, cataloged access to files and
# support direct /Volumes/... paths in Spark, Python, and SQL.
# The data generator writes JSONL files here; Auto Loader
# reads from the same path.

spark.sql("""
CREATE VOLUME IF NOT EXISTS alert_pipeline
COMMENT 'Landing zone for raw JSONL events and streaming checkpoints'
""")

print("✓ alert_pipeline volume created")

# Create subdirectories for organization
import os
volume_base = "/Volumes/akash_s_demo/ams/alert_pipeline"
for subdir in ["raw_events", "checkpoints/bronze", "checkpoints/silver", "checkpoints/gold"]:
    path = f"{volume_base}/{subdir}"
    os.makedirs(path, exist_ok=True)

print("✓ Volume subdirectories created")

# COMMAND ----------

# ---------------------------------------------------------
# 3. BRONZE TABLE — Raw event ingestion
# ---------------------------------------------------------
# Stores every raw event exactly as received (append-only).
# The unified schema covers all four event types:
#   metric, alert, log, deployment
#
# Nullable columns accommodate fields that only appear in
# certain event types (e.g., alert_id is NULL for metrics).

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_events (
    -- Common fields
    event_type       STRING    NOT NULL  COMMENT 'metric | alert | log | deployment',
    timestamp        TIMESTAMP NOT NULL  COMMENT 'Event origination time (ISO-8601)',
    application_id   STRING    NOT NULL  COMMENT 'Source application identifier',
    host_id          STRING    NOT NULL  COMMENT 'Source host identifier',

    -- Metric fields
    metric_name      STRING              COMMENT 'cpu_percent, memory_percent, error_rate, etc.',
    metric_value     DOUBLE              COMMENT 'Numeric metric value',

    -- Alert fields
    alert_id         STRING              COMMENT 'Unique alert instance ID',
    fingerprint      STRING              COMMENT 'Deterministic dedup key for repeated alerts',
    alert_type       STRING              COMMENT 'cpu_high, memory_high, error_rate_high, etc.',
    severity         STRING              COMMENT 'CRITICAL | HIGH | MEDIUM | LOW',
    threshold        DOUBLE              COMMENT 'Configured alerting threshold',
    current_value    DOUBLE              COMMENT 'Value that triggered the alert',

    -- Log fields
    log_level        STRING              COMMENT 'ERROR | WARN | INFO | DEBUG',
    message          STRING              COMMENT 'Free-text log message',
    error_code       STRING              COMMENT 'Structured error code if applicable',

    -- Deployment fields
    deployment_id    STRING              COMMENT 'Unique deployment ID',
    version          STRING              COMMENT 'Deployed version string',
    change_type      STRING              COMMENT 'rolling_update | blue_green | canary',

    -- Ingestion metadata
    _ingested_at     TIMESTAMP           COMMENT 'Time the record was written to Bronze',
    _source_file     STRING              COMMENT 'Auto Loader source file path'
)
USING DELTA
COMMENT 'Raw event landing zone — append-only, no transformations'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
""")

print("✓ bronze_events table created")

# COMMAND ----------

# ---------------------------------------------------------
# 3. SILVER TABLE — Deduplicated alerts
# ---------------------------------------------------------
# Contains one row per unique alert fingerprint per
# suppression window.  Downstream consumers see only
# deduplicated, meaningful alerts.

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_alerts (
    fingerprint          STRING    NOT NULL  COMMENT 'Deterministic alert dedup key',
    alert_id             STRING    NOT NULL  COMMENT 'Alert ID of the FIRST occurrence',
    application_id       STRING    NOT NULL,
    host_id              STRING    NOT NULL,
    alert_type           STRING    NOT NULL,
    severity             STRING    NOT NULL,
    threshold            DOUBLE,
    current_value        DOUBLE,
    first_seen_timestamp TIMESTAMP NOT NULL  COMMENT 'Earliest firing in the window',
    last_seen_timestamp  TIMESTAMP NOT NULL  COMMENT 'Latest firing in the window',
    suppressed_count     INT       NOT NULL  COMMENT 'Number of duplicates suppressed',
    _processed_at        TIMESTAMP           COMMENT 'Silver processing timestamp'
)
USING DELTA
COMMENT 'Deduplicated alerts — one row per fingerprint per suppression window'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
""")

print("✓ silver_alerts table created")

# COMMAND ----------

# ---------------------------------------------------------
# 4. GOLD TABLE — Correlated incidents
# ---------------------------------------------------------
# Each row represents one incident: a deduplicated alert
# enriched with contextual data from the 60-minute lookback
# window plus an LLM-generated summary.

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_incidents (
    incident_id            STRING    NOT NULL  COMMENT 'Unique incident identifier',
    alert_id               STRING    NOT NULL  COMMENT 'Source alert from Silver',
    fingerprint            STRING    NOT NULL,
    application_id         STRING    NOT NULL,
    host_id                STRING    NOT NULL,
    alert_type             STRING    NOT NULL,
    severity               STRING,
    alert_timestamp        TIMESTAMP NOT NULL  COMMENT 'When the alert first fired',

    -- Correlation outputs
    correlated_deployments STRING              COMMENT 'JSON array of deployment events in window',
    error_summary          STRING              COMMENT 'JSON: {error_code: count}',
    metric_summary         STRING              COMMENT 'JSON: {cpu_max, cpu_avg, mem_max, err_rate_max}',
    prior_alert_count      INT                 COMMENT 'Count of other alerts in 60-min window',

    -- Context & LLM (populated by ai_query batch job)
    incident_context_text  STRING              COMMENT 'Structured text bundle for LLM prompt',
    summary                STRING              COMMENT 'LLM-generated incident summary',
    patterns               STRING              COMMENT 'JSON array of detected patterns',
    root_cause             STRING              COMMENT 'Most likely root cause from LLM',
    confidence_score       DOUBLE              COMMENT 'LLM confidence in root cause 0–1',
    recommended_action     STRING              COMMENT 'Suggested next step for on-call engineer',

    _created_at            TIMESTAMP           COMMENT 'Gold row creation time'
)
USING DELTA
COMMENT 'Enriched incidents with correlation context and LLM summary'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
""")

print("✓ gold_incidents table created")

# COMMAND ----------

# ---------------------------------------------------------
# 5. VERIFY
# ---------------------------------------------------------
for tbl in ["bronze_events", "silver_alerts", "gold_incidents"]:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {tbl}").collect()[0]["cnt"]
    print(f"  {tbl}: {count} rows")

print("\n✓ Setup complete — all tables ready")
