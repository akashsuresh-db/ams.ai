# Databricks notebook source
# =========================================================
# config.py — Shared Configuration for Alert Pipeline
# =========================================================
# Centralizes all path, schema, and parameter configurations
# so that every notebook references a single source of truth.
# =========================================================

# ---------------------------------------------------------
# 1. STORAGE PATHS
# ---------------------------------------------------------
# Root path for the pipeline's file storage.
# Uses a Unity Catalog Volume for governed, cataloged access
# to unstructured/semi-structured files.
# Format: /Volumes/<catalog>/<schema>/<volume_name>
BASE_PATH = "/Volumes/akash_s_demo/ams/alert_pipeline"

# Where the synthetic data generator writes JSONL files
RAW_EVENTS_PATH = f"{BASE_PATH}/raw_events"

# Delta table locations (managed or external)
BRONZE_TABLE_PATH = f"{BASE_PATH}/bronze/bronze_events"
SILVER_TABLE_PATH = f"{BASE_PATH}/silver/silver_alerts"
GOLD_TABLE_PATH   = f"{BASE_PATH}/gold/gold_incidents"

# Streaming checkpoint locations (one per stream)
BRONZE_CHECKPOINT = f"{BASE_PATH}/checkpoints/bronze"
SILVER_CHECKPOINT = f"{BASE_PATH}/checkpoints/silver"

# ---------------------------------------------------------
# 2. CATALOG / SCHEMA NAMES (Unity Catalog compatible)
# ---------------------------------------------------------
CATALOG  = "akash_s_demo"
SCHEMA   = "ams"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_events"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_alerts"
GOLD_TABLE   = f"{CATALOG}.{SCHEMA}.gold_incidents"

# ---------------------------------------------------------
# 3. PIPELINE PARAMETERS
# ---------------------------------------------------------

# Deduplication: suppress duplicate alerts within this window
DEDUP_WINDOW_MINUTES = 5

# Correlation: lookback window for gathering context
CORRELATION_LOOKBACK_MINUTES = 60

# Watermark delay for late-arriving events
WATERMARK_DELAY = "10 minutes"

# Auto Loader: max files per micro-batch
AUTOLOADER_MAX_FILES_PER_TRIGGER = 10

# ---------------------------------------------------------
# 4. DATA GENERATOR PARAMETERS
# ---------------------------------------------------------
APPLICATIONS = ["payments-api", "orders-api", "auth-service"]
HOSTS = ["host-a", "host-b"]
EVENT_TYPES = ["metric", "alert", "log", "deployment"]

# Output file for the generator
GENERATOR_OUTPUT_FILE = f"{RAW_EVENTS_PATH}/events.jsonl"

# ---------------------------------------------------------
# 5. LLM CONFIGURATION
# ---------------------------------------------------------
# ai_query() is used instead of ai_summarize() for structured
# JSON output.  The model endpoint must be available in your
# workspace's Model Serving.  Check the Serving UI for endpoints.
#
# Keep deterministic correlation separate from stochastic LLM.

LLM_MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
LLM_TEMPERATURE = 0.1       # Near-deterministic for consistency
LLM_MAX_TOKENS = 1024       # Enough for structured JSON response
