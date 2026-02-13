# Databricks notebook source
# =========================================================
# 03_silver_dedup.py — Silver Layer (Deduplication Engine)
# =========================================================
#
# PURPOSE:
#   Reads alert events from the Bronze table, deduplicates
#   them by fingerprint within a 5-minute suppression window,
#   and writes one consolidated row per unique alert to the
#   silver_alerts Delta table.
#
# DESIGN DECISIONS:
#
#   WHY DETERMINISTIC DEDUPLICATION:
#     Alert systems often fire the same alert repeatedly
#     (e.g., CPU > 80% every minute while the condition holds).
#     Without dedup, downstream systems (correlation, paging,
#     LLM summarization) get flooded with identical signals.
#     Dedup MUST be deterministic (based on fingerprint + time
#     window) — NOT probabilistic or LLM-based — because:
#       a) Reproducibility: re-running the pipeline on the same
#          data must produce identical results.
#       b) Auditability: an engineer must be able to explain
#          exactly why an alert was suppressed.
#       c) Latency: dedup runs in the streaming hot path;
#          LLM calls would add seconds of latency per event.
#
#   WHY WATERMARKING:
#     Structured Streaming's watermark tells Spark how long to
#     wait for late-arriving data before finalizing a window.
#     Without it, the state store would grow unbounded.
#     We use a 10-minute watermark (2× the 5-minute dedup window)
#     to tolerate moderate late arrivals.
#
#   WHY STATEFUL AGGREGATION:
#     We use groupBy + window aggregation rather than
#     dropDuplicatesWithinWatermark because we need to track
#     suppressed_count and last_seen_timestamp — metadata that
#     dropDuplicates does not provide.
# =========================================================

# COMMAND ----------

# %run ./config

# Inline config for standalone reference
BRONZE_TABLE      = "alert_ops.pipeline.bronze_events"
SILVER_TABLE      = "alert_ops.pipeline.silver_alerts"
SILVER_CHECKPOINT = "/mnt/alert_pipeline/checkpoints/silver"
DEDUP_WINDOW_MINUTES = 5
WATERMARK_DELAY      = "10 minutes"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, window, first, last, count, current_timestamp, lit
)

# ---------------------------------------------------------
# 1. READ ALERTS FROM BRONZE (Streaming)
# ---------------------------------------------------------
# Filter to event_type = 'alert' at the source to minimize
# data shuffled through the dedup aggregation.  Bronze
# contains all event types; Silver only cares about alerts.

bronze_alerts = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
    .filter(col("event_type") == "alert")
    .select(
        col("alert_id"),
        col("fingerprint"),
        col("timestamp"),
        col("application_id"),
        col("host_id"),
        col("alert_type"),
        col("severity"),
        col("threshold"),
        col("current_value"),
    )
    # Watermark on event timestamp for state cleanup
    .withWatermark("timestamp", WATERMARK_DELAY)
)

# COMMAND ----------

# ---------------------------------------------------------
# 2. DEDUPLICATE BY FINGERPRINT + 5-MINUTE WINDOW
# ---------------------------------------------------------
# The tumbling window groups alerts by fingerprint into
# 5-minute buckets.  Within each bucket:
#   - first(alert_id)   → the canonical alert ID we keep
#   - min(timestamp)    → first_seen_timestamp
#   - max(timestamp)    → last_seen_timestamp
#   - count(*) - 1      → suppressed_count (subtract the one we keep)
#
# This approach ensures that if the same alert fires 6 times
# in 5 minutes, we emit ONE row with suppressed_count = 5.

deduped_alerts = (
    bronze_alerts
    .groupBy(
        col("fingerprint"),
        col("application_id"),
        col("host_id"),
        col("alert_type"),
        window(col("timestamp"), f"{DEDUP_WINDOW_MINUTES} minutes")
    )
    .agg(
        first("alert_id", ignorenulls=True).alias("alert_id"),
        first("severity", ignorenulls=True).alias("severity"),
        first("threshold", ignorenulls=True).alias("threshold"),
        first("current_value", ignorenulls=True).alias("current_value"),
        F.min("timestamp").alias("first_seen_timestamp"),
        F.max("timestamp").alias("last_seen_timestamp"),
        (count("*") - lit(1)).cast("int").alias("suppressed_count"),
    )
    # Drop the window struct column (we've extracted what we need)
    .drop("window")
    # Add processing metadata
    .withColumn("_processed_at", current_timestamp())
)

# COMMAND ----------

# ---------------------------------------------------------
# 3. WRITE DEDUPLICATED ALERTS TO SILVER
# ---------------------------------------------------------
# outputMode("update"):
#   In stateful aggregation with watermark, Spark emits
#   updated aggregates as new events arrive in the window.
#   Once the watermark passes, the window is finalized.
#   "update" mode writes only changed rows, minimizing I/O.
#
# NOTE: "complete" mode would rewrite the entire result
#   table on every micro-batch — prohibitively expensive.
#   "append" mode only emits after watermark, adding latency.
#   "update" is the right balance for near-real-time dedup.

silver_query = (
    deduped_alerts.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .queryName("silver_dedup")

    # --- Choose ONE trigger mode ---
    # Production:
    # .trigger(processingTime="1 minute")

    # Development:
    .trigger(availableNow=True)

    .toTable(SILVER_TABLE)
)

print(f"Silver dedup stream started → {SILVER_TABLE}")
print(f"Dedup window: {DEDUP_WINDOW_MINUTES} min | "
      f"Watermark: {WATERMARK_DELAY}")

# COMMAND ----------

# ---------------------------------------------------------
# 4. VERIFICATION QUERY (run after stream completes)
# ---------------------------------------------------------
# Uncomment to inspect results:
#
# display(
#     spark.sql(f"""
#         SELECT fingerprint, alert_type, application_id,
#                first_seen_timestamp, last_seen_timestamp,
#                suppressed_count
#         FROM {SILVER_TABLE}
#         ORDER BY first_seen_timestamp
#     """)
# )
