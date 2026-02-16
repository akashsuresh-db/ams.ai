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

# MAGIC %run ./config

# COMMAND ----------

# %run ./config

# Inline config for standalone reference
RAW_EVENTS_PATH  = "/Volumes/akash_s_demo/ams/alert_pipeline/raw_events"
BRONZE_TABLE      = "akash_s_demo.ams.bronze_events"
BRONZE_CHECKPOINT = "/Volumes/akash_s_demo/ams/alert_pipeline/checkpoints/bronze"

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

# DBTITLE 1,Cell 6
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
    .withColumn("_source_file", col("_metadata.file_path"))
)

# COMMAND ----------

# DBTITLE 1,Cell 7
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
# TRIGGER MODES:
#   processingTime="10 seconds"  — Continuous: polls for new
#     files every 10 seconds.  Use this with the data generator's
#     stream_events_to_volume() which drip-feeds JSONL files.
#     Auto Loader discovers each new file and processes it in
#     the next micro-batch.
#
#   availableNow=True — One-shot: processes all available files
#     then stops.  Use for backfill or testing ONLY.

query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .option("mergeSchema", "true")     # Handles minor schema evolution
    .queryName("bronze_ingestion")

    # Continuous streaming: checks for new files every 10 seconds.
    # The data generator writes a new JSONL file every 5 seconds,
    # so each micro-batch picks up ~1-2 new files.
    .trigger(processingTime="10 seconds")

    # For one-shot backfill, comment the above and uncomment:
    # .trigger(availableNow=True)

    .toTable(BRONZE_TABLE)
)

print(f"Bronze ingestion stream started → {BRONZE_TABLE}")
print(f"Trigger: processingTime=10s | Checkpoint: {BRONZE_CHECKPOINT}")

# COMMAND ----------

# ---------------------------------------------------------
# 5. MONITOR
# ---------------------------------------------------------
# The stream runs continuously. To watch it:
#
#   query.status               # Current status dict
#   query.recentProgress       # Last few micro-batch stats
#   query.awaitTermination()   # Block until manually stopped
#
# To check data landing in real-time:
display(spark.sql(f"SELECT event_type, COUNT(*) FROM {BRONZE_TABLE} GROUP BY 1"))
#
# To stop the stream:
# query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  akash_s_demo.ams.bronze_events

# COMMAND ----------


