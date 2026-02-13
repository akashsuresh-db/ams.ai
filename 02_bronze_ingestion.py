# Databricks notebook source
# =========================================================
# 02_bronze_ingestion.py — Bronze Layer (Raw Streaming Ingestion)
# =========================================================
#
# PURPOSE:
#   Continuously ingests raw JSONL events from the landing
#   zone into the bronze_events Delta table using Auto Loader
#   (cloudFiles) and Structured Streaming.
#
# DESIGN DECISIONS:
#   - Auto Loader (cloudFiles) provides exactly-once file
#     discovery with automatic schema inference and file
#     tracking via RocksDB state store.
#   - The Bronze layer is APPEND-ONLY: no transformations,
#     no filtering.  Every event is preserved for auditability
#     and reprocessing.
#   - Ingestion metadata (_ingested_at, _source_file) is added
#     to support lineage and debugging.
#   - Schema is explicitly defined (not inferred) to prevent
#     schema drift from corrupting downstream layers.
# =========================================================

# COMMAND ----------

# %run ./config

# Inline config for standalone reference
RAW_EVENTS_PATH  = "/mnt/alert_pipeline/raw_events"
BRONZE_TABLE      = "akash_s_demo.ams.bronze_events"
BRONZE_CHECKPOINT = "/mnt/alert_pipeline/checkpoints/bronze"

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql.functions import current_timestamp, input_file_name, col

# ---------------------------------------------------------
# 1. DEFINE SCHEMA
# ---------------------------------------------------------
# Explicit schema prevents Auto Loader from inferring types
# incorrectly (e.g., treating numeric strings as strings).
# This is critical for production: schema drift in the source
# should be caught here, not silently propagated.

bronze_schema = StructType([
    StructField("event_type",    StringType(), False),
    StructField("timestamp",     StringType(), False),   # Parsed below
    StructField("application_id", StringType(), False),
    StructField("host_id",       StringType(), False),

    # Metric fields
    StructField("metric_name",   StringType(), True),
    StructField("metric_value",  DoubleType(), True),

    # Alert fields
    StructField("alert_id",      StringType(), True),
    StructField("fingerprint",   StringType(), True),
    StructField("alert_type",    StringType(), True),
    StructField("severity",      StringType(), True),
    StructField("threshold",     DoubleType(), True),
    StructField("current_value", DoubleType(), True),

    # Log fields
    StructField("log_level",     StringType(), True),
    StructField("message",       StringType(), True),
    StructField("error_code",    StringType(), True),

    # Deployment fields
    StructField("deployment_id", StringType(), True),
    StructField("version",       StringType(), True),
    StructField("change_type",   StringType(), True),
])

# COMMAND ----------

# ---------------------------------------------------------
# 2. AUTO LOADER STREAMING READ
# ---------------------------------------------------------
# cloudFiles source options:
#   - format: json (our JSONL files)
#   - schemaLocation: persists inferred schema for evolution
#     (not needed here since we provide explicit schema, but
#     good practice for production)
#   - maxFilesPerTrigger: controls micro-batch size to prevent
#     overwhelming the cluster on initial backfill
#
# WHY AUTO LOADER OVER fileStream:
#   - Automatic file tracking (no manual bookkeeping)
#   - Scales to millions of files via notification mode
#   - Built-in schema evolution support
#   - Exactly-once file processing guarantees

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation",
            f"{BRONZE_CHECKPOINT}/_schema")
    .option("cloudFiles.maxFilesPerTrigger", 10)
    .option("cloudFiles.inferColumnTypes", "false")
    .schema(bronze_schema)
    .load(RAW_EVENTS_PATH)
)

# COMMAND ----------

# ---------------------------------------------------------
# 3. ADD INGESTION METADATA
# ---------------------------------------------------------
# _ingested_at: wallclock time when the row enters Bronze.
#   Useful for SLA monitoring (ingestion lag = _ingested_at - timestamp).
# _source_file: the JSONL file that contained this event.
#   Essential for debugging bad data back to its source file.

bronze_stream = (
    raw_stream
    .withColumn("timestamp",
                col("timestamp").cast(TimestampType()))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# COMMAND ----------

# ---------------------------------------------------------
# 4. STREAMING WRITE TO DELTA
# ---------------------------------------------------------
# outputMode("append"): Bronze is append-only by design.
#   We never update or delete raw events.
#
# checkpointLocation: enables exactly-once delivery.
#   If the stream restarts, it resumes from the last
#   committed micro-batch offset.
#
# trigger(availableNow=True): processes all available files
#   then stops.  In production, replace with:
#     .trigger(processingTime="30 seconds")
#   for continuous streaming.

query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .option("mergeSchema", "true")     # Handles minor schema evolution
    .queryName("bronze_ingestion")

    # --- Choose ONE trigger mode ---
    # Production (continuous):
    # .trigger(processingTime="30 seconds")

    # Development / backfill (process all, then stop):
    .trigger(availableNow=True)

    .toTable(BRONZE_TABLE)
)

print(f"Bronze ingestion stream started → {BRONZE_TABLE}")
print(f"Checkpoint: {BRONZE_CHECKPOINT}")

# COMMAND ----------

# ---------------------------------------------------------
# 5. MONITOR (optional)
# ---------------------------------------------------------
# In a Databricks notebook, you can view the streaming
# query's progress in the cell output.  For programmatic
# monitoring:

# query.awaitTermination()   # Block until stream stops
# query.status               # Current status dict
# query.recentProgress       # Last few micro-batch stats

# To verify data landed:
# display(spark.sql(f"SELECT event_type, COUNT(*) FROM {BRONZE_TABLE} GROUP BY 1"))
