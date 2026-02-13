# Databricks notebook source
# =========================================================
# 04_gold_correlation.py — Gold Layer (Correlation Engine
#                          + Context Bundle Builder)
# =========================================================
#
# PURPOSE:
#   For each deduplicated alert in Silver, this notebook:
#     1. Opens a 60-minute lookback window into Bronze
#     2. Aggregates deployments, errors, metrics, prior alerts
#     3. Builds a structured incident_context_text string
#     4. Writes enriched incidents to the gold_incidents table
#
# DESIGN DECISIONS:
#
#   WHY CORRELATION MUST NOT RELY ON LLM:
#     Correlation is a deterministic data-engineering operation:
#     "find all related events in the last 60 minutes for the
#     same application."  This MUST be done with SQL/Spark
#     because:
#       a) Determinism: the same inputs always produce the same
#          correlated context.  LLMs are stochastic.
#       b) Cost: LLM calls cost money per token.  Correlation
#          can involve scanning thousands of Bronze rows —
#          Spark does this efficiently; LLMs cannot.
#       c) Latency: correlation runs in the streaming hot path;
#          LLM round-trips would bottleneck throughput.
#       d) Auditability: an engineer can inspect the SQL logic
#          and verify the correlation window.  LLM reasoning
#          is a black box.
#
#   WHY THIS RUNS AS foreachBatch:
#     We need to JOIN each micro-batch of new Silver alerts
#     against the Bronze table (a static/batch read).
#     Structured Streaming does not support stream-stream
#     joins with arbitrary lookback windows efficiently.
#     foreachBatch gives us the flexibility to:
#       - Read each micro-batch as a DataFrame
#       - Join against Bronze (batch read)
#       - Write enriched results to Gold
#
#   WHY BRONZE / SILVER / GOLD SEPARATION MATTERS:
#     - Bronze: raw, immutable audit log.  If any downstream
#       logic is wrong, we can always reprocess from Bronze.
#     - Silver: cleaned, deduplicated data.  Reduces noise
#       for downstream consumers.
#     - Gold: business-level aggregates (incidents).  Optimized
#       for dashboards, alerting, and LLM summarization.
#     Without this separation, a bug in correlation logic
#     could corrupt raw data, making recovery impossible.
# =========================================================

# COMMAND ----------

# %run ./config

# Inline config for standalone reference
BRONZE_TABLE = "alert_ops.pipeline.bronze_events"
SILVER_TABLE = "alert_ops.pipeline.silver_alerts"
GOLD_TABLE   = "alert_ops.pipeline.gold_incidents"
GOLD_CHECKPOINT = "/mnt/alert_pipeline/checkpoints/gold"
CORRELATION_LOOKBACK_MINUTES = 60

# COMMAND ----------

import json
import uuid
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, current_timestamp, collect_list, struct,
    when, max as spark_max, avg as spark_avg,
    count as spark_count, to_json, concat_ws
)
from pyspark.sql.types import StringType

# ---------------------------------------------------------
# 1. CONTEXT BUNDLE BUILDER (UDF)
# ---------------------------------------------------------
# Builds the structured text that will be sent to the LLM.
# This is a pure Python function — no LLM calls, no I/O.
# Deterministic: same inputs → same output string.

def build_context_text(
    alert_type, application_id, host_id,
    current_value, threshold,
    deployments_json, error_summary_json,
    cpu_max, cpu_avg, memory_max, error_rate_max,
    prior_alert_count
):
    """
    Assemble the incident_context_text string in a fixed,
    predictable format.  This is what the LLM will summarize.
    """
    if alert_type is None:
        return ""

    # Parse deployment list
    try:
        deps = json.loads(deployments_json) if deployments_json else []
    except (json.JSONDecodeError, TypeError):
        deps = []

    dep_lines = "\n".join(
        [f"  - {d.get('version', '?')} deployed at "
         f"{d.get('timestamp', '?')} ({d.get('change_type', '?')})"
         for d in deps]
    ) if deps else "  None"

    # Parse error summary
    try:
        errors = json.loads(error_summary_json) if error_summary_json else {}
    except (json.JSONDecodeError, TypeError):
        errors = {}

    err_lines = "\n".join(
        [f"  - {code}: {cnt} occurrences" for code, cnt in errors.items()]
    ) if errors else "  None"

    # Format values safely
    def fmt(val, suffix=""):
        if val is None:
            return "N/A"
        return f"{val:.2f}{suffix}"

    context = f"""Incident Alert:
{alert_type} triggered on {application_id} {host_id}
Current value: {fmt(current_value)} Threshold: {fmt(threshold)}

Recent Deployments:
{dep_lines}

Error Summary (last 60 mins):
{err_lines}

Metric Summary:
  - CPU max: {fmt(cpu_max, '%')}
  - CPU avg: {fmt(cpu_avg, '%')}
  - Memory max: {fmt(memory_max, '%')}
  - Error rate max: {fmt(error_rate_max)}

Prior alerts in window: {prior_alert_count if prior_alert_count is not None else 0}"""

    return context


# Register as UDF for use in Spark SQL
build_context_udf = F.udf(build_context_text, StringType())

# COMMAND ----------

# ---------------------------------------------------------
# 2. CORRELATION LOGIC (foreachBatch function)
# ---------------------------------------------------------

def correlate_and_enrich(micro_batch_df, batch_id):
    """
    For each micro-batch of new deduplicated alerts from Silver:
      1. Read Bronze events in the 60-min lookback window
      2. Aggregate deployments, errors, metrics, prior alerts
      3. Build context text
      4. Write to Gold
    """
    if micro_batch_df.count() == 0:
        print(f"Batch {batch_id}: empty — skipping")
        return

    print(f"Batch {batch_id}: processing "
          f"{micro_batch_df.count()} new alerts")

    # Cache the micro-batch for multiple joins
    alerts_df = micro_batch_df.alias("alert")

    # --------------------------------------------------
    # 2a. GATHER BRONZE EVENTS IN LOOKBACK WINDOW
    # --------------------------------------------------
    # For each alert, find all Bronze events where:
    #   - same application_id
    #   - timestamp in [alert_time - 60 min, alert_time]

    bronze_df = spark.read.table(BRONZE_TABLE).alias("bronze")

    # Join condition: same app + within lookback window
    lookback_join = (
        alerts_df.join(
            bronze_df,
            (col("alert.application_id") == col("bronze.application_id"))
            & (col("bronze.timestamp") >= F.expr(
                f"alert.first_seen_timestamp - INTERVAL "
                f"{CORRELATION_LOOKBACK_MINUTES} MINUTES"))
            & (col("bronze.timestamp") <= col("alert.first_seen_timestamp")),
            "left"
        )
    )

    # --------------------------------------------------
    # 2b. AGGREGATE DEPLOYMENTS
    # --------------------------------------------------
    deployments_agg = (
        lookback_join
        .filter(col("bronze.event_type") == "deployment")
        .groupBy(col("alert.fingerprint"))
        .agg(
            to_json(
                collect_list(
                    struct(
                        col("bronze.deployment_id"),
                        col("bronze.version"),
                        col("bronze.change_type"),
                        col("bronze.timestamp").cast("string").alias("timestamp")
                    )
                )
            ).alias("correlated_deployments")
        )
    )

    # --------------------------------------------------
    # 2c. AGGREGATE ERROR LOGS
    # --------------------------------------------------
    # Group by error_code within each alert's window,
    # then pivot into a JSON summary: {"ERROR_CODE": count}
    error_logs = (
        lookback_join
        .filter(
            (col("bronze.event_type") == "log")
            & (col("bronze.log_level") == "ERROR")
            & (col("bronze.error_code").isNotNull())
        )
        .groupBy(col("alert.fingerprint"), col("bronze.error_code"))
        .agg(spark_count("*").alias("err_count"))
    )

    # Collapse error counts into a JSON map per fingerprint
    error_summary_agg = (
        error_logs
        .groupBy("fingerprint")
        .agg(
            to_json(
                F.map_from_arrays(
                    collect_list("error_code"),
                    collect_list(col("err_count").cast("string"))
                )
            ).alias("error_summary")
        )
    )

    # --------------------------------------------------
    # 2d. AGGREGATE METRICS
    # --------------------------------------------------
    metrics_agg = (
        lookback_join
        .filter(col("bronze.event_type") == "metric")
        .groupBy(col("alert.fingerprint"))
        .agg(
            spark_max(
                when(col("bronze.metric_name") == "cpu_percent",
                     col("bronze.metric_value"))
            ).alias("cpu_max"),
            spark_avg(
                when(col("bronze.metric_name") == "cpu_percent",
                     col("bronze.metric_value"))
            ).alias("cpu_avg"),
            spark_max(
                when(col("bronze.metric_name") == "memory_percent",
                     col("bronze.metric_value"))
            ).alias("memory_max"),
            spark_max(
                when(col("bronze.metric_name") == "error_rate",
                     col("bronze.metric_value"))
            ).alias("error_rate_max"),
        )
    )

    # --------------------------------------------------
    # 2e. COUNT PRIOR ALERTS IN WINDOW
    # --------------------------------------------------
    prior_alerts_agg = (
        lookback_join
        .filter(
            (col("bronze.event_type") == "alert")
            & (col("bronze.alert_id") != col("alert.alert_id"))
        )
        .groupBy(col("alert.fingerprint"))
        .agg(
            spark_count("*").alias("prior_alert_count")
        )
    )

    # --------------------------------------------------
    # 2f. JOIN ALL AGGREGATIONS BACK TO ALERTS
    # --------------------------------------------------
    enriched = (
        alerts_df
        .join(deployments_agg,
              alerts_df.fingerprint == deployments_agg.fingerprint,
              "left")
        .drop(deployments_agg.fingerprint)

        .join(error_summary_agg,
              alerts_df.fingerprint == error_summary_agg.fingerprint,
              "left")
        .drop(error_summary_agg.fingerprint)

        .join(metrics_agg,
              alerts_df.fingerprint == metrics_agg.fingerprint,
              "left")
        .drop(metrics_agg.fingerprint)

        .join(prior_alerts_agg,
              alerts_df.fingerprint == prior_alerts_agg.fingerprint,
              "left")
        .drop(prior_alerts_agg.fingerprint)
    )

    # --------------------------------------------------
    # 2g. BUILD METRIC SUMMARY JSON
    # --------------------------------------------------
    enriched = enriched.withColumn(
        "metric_summary",
        to_json(
            struct(
                col("cpu_max"), col("cpu_avg"),
                col("memory_max"), col("error_rate_max")
            )
        )
    )

    # --------------------------------------------------
    # 2h. BUILD CONTEXT TEXT
    # --------------------------------------------------
    enriched = enriched.withColumn(
        "incident_context_text",
        build_context_udf(
            col("alert_type"),
            col("application_id"),
            col("host_id"),
            col("current_value"),
            col("threshold"),
            col("correlated_deployments"),
            col("error_summary"),
            col("cpu_max"),
            col("cpu_avg"),
            col("memory_max"),
            col("error_rate_max"),
            col("prior_alert_count"),
        )
    )

    # --------------------------------------------------
    # 2i. BUILD FINAL GOLD SCHEMA
    # --------------------------------------------------
    gold_df = enriched.select(
        F.expr("uuid()").alias("incident_id"),
        col("alert_id"),
        col("fingerprint"),
        col("application_id"),
        col("host_id"),
        col("alert_type"),
        col("severity"),
        col("first_seen_timestamp").alias("alert_timestamp"),
        col("correlated_deployments"),
        col("error_summary"),
        col("metric_summary"),
        F.coalesce(col("prior_alert_count"), lit(0))
         .alias("prior_alert_count"),
        col("incident_context_text"),
        lit(None).cast("string").alias("summary"),
        lit(None).cast("string").alias("root_cause"),
        lit(None).cast("double").alias("confidence_score"),
        current_timestamp().alias("_created_at"),
    )

    # --------------------------------------------------
    # 2j. WRITE TO GOLD (MERGE to handle reprocessing)
    # --------------------------------------------------
    # We use a MERGE (upsert) on fingerprint + alert_timestamp
    # to make the pipeline idempotent: rerunning the same
    # micro-batch won't create duplicate incidents.

    gold_df.createOrReplaceTempView("new_incidents")

    spark.sql(f"""
        MERGE INTO {GOLD_TABLE} AS target
        USING new_incidents AS source
        ON target.fingerprint = source.fingerprint
           AND target.alert_timestamp = source.alert_timestamp
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Batch {batch_id}: wrote {gold_df.count()} incidents to Gold")


# COMMAND ----------

# ---------------------------------------------------------
# 3. START STREAMING PIPELINE
# ---------------------------------------------------------
# Read from Silver (streaming) and apply foreachBatch
# correlation logic.

silver_stream = (
    spark.readStream
    .format("delta")
    .table(SILVER_TABLE)
)

gold_query = (
    silver_stream.writeStream
    .foreachBatch(correlate_and_enrich)
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .queryName("gold_correlation")

    # --- Choose ONE trigger mode ---
    # Production:
    # .trigger(processingTime="2 minutes")

    # Development:
    .trigger(availableNow=True)

    .start()
)

print(f"Gold correlation stream started → {GOLD_TABLE}")

# COMMAND ----------

# ---------------------------------------------------------
# 4. VERIFICATION (run after stream completes)
# ---------------------------------------------------------
# Uncomment to inspect:
#
# display(
#     spark.sql(f"""
#         SELECT incident_id, alert_type, application_id,
#                alert_timestamp, prior_alert_count,
#                LEFT(incident_context_text, 200) AS context_preview,
#                summary
#         FROM {GOLD_TABLE}
#         ORDER BY alert_timestamp
#     """)
# )
