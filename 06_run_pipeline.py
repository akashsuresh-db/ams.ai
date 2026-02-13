# Databricks notebook source
# =========================================================
# 06_run_pipeline.py — Orchestrator / Quick-Start Runner
# =========================================================
#
# PURPOSE:
#   Single notebook that runs the entire pipeline end-to-end
#   in the correct order.  Useful for demos, testing, and
#   development.
#
# HOW IT WORKS:
#   1. Setup: create catalog, schema, volume, tables
#   2. Start Bronze/Silver/Gold streaming queries (continuous)
#   3. Start data generator (drip-feeds files into Volume)
#   4. Streams pick up files as they arrive
#   5. After generator finishes, run LLM summarization batch
#
#   In production, each step would be a separate Task in a
#   Databricks Workflow (DAG), with dependencies configured
#   in the Workflow UI or via Terraform/Pulumi.
# =========================================================

# COMMAND ----------

# ---------------------------------------------------------
# STEP 0: SETUP — Create catalog, schema, volume, tables
# ---------------------------------------------------------
# %run ./00_setup

# COMMAND ----------

# ---------------------------------------------------------
# STEP 1: IMPORT DATA GENERATOR (don't generate yet)
# ---------------------------------------------------------
# %run ./01_data_generator

# COMMAND ----------

# ---------------------------------------------------------
# STEP 2: START BRONZE STREAM (continuous)
# ---------------------------------------------------------
# Start the Bronze Auto Loader stream FIRST.
# It polls the Volume landing zone every 10 seconds for
# new JSONL files.

# %run ./02_bronze_ingestion

# COMMAND ----------

# ---------------------------------------------------------
# STEP 3: START SILVER STREAM (continuous)
# ---------------------------------------------------------
# Reads new alert events from Bronze every 30 seconds,
# deduplicates by fingerprint within 5-minute windows.

# %run ./03_silver_dedup

# COMMAND ----------

# ---------------------------------------------------------
# STEP 4: START GOLD STREAM (continuous)
# ---------------------------------------------------------
# Reads new deduplicated alerts from Silver every 60 seconds,
# correlates with Bronze context, builds incident bundles.

# %run ./04_gold_correlation

# COMMAND ----------

# ---------------------------------------------------------
# STEP 5: START DATA GENERATOR (drip-feed into Volume)
# ---------------------------------------------------------
# NOW start writing events.  The three streams above are
# already running and will pick up files as they land.
#
# stream_events_to_volume() writes ~20 events per file
# every 5 seconds.  ~500 events / 20 per file = ~25 files
# over ~2 minutes.

# events = generate_all_events()
# stream_events_to_volume(events)

# COMMAND ----------

# ---------------------------------------------------------
# STEP 6: WAIT FOR PIPELINE TO DRAIN
# ---------------------------------------------------------
# After the generator finishes, give the streams time to
# process the last few batches before checking results.

import time
# print("Waiting 90 seconds for streams to finish processing...")
# time.sleep(90)

# COMMAND ----------

# ---------------------------------------------------------
# STEP 7: VERIFY STREAMING RESULTS
# ---------------------------------------------------------

print("=" * 60)
print("PIPELINE VERIFICATION")
print("=" * 60)

for table_name, label in [
    ("akash_s_demo.ams.bronze_events", "Bronze (raw events)"),
    ("akash_s_demo.ams.silver_alerts", "Silver (deduped alerts)"),
    ("akash_s_demo.ams.gold_incidents", "Gold (incidents)"),
]:
    try:
        count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_name}"
        ).collect()[0]["cnt"]
        print(f"  {label}: {count:,} rows")
    except Exception as e:
        print(f"  {label}: ERROR — {e}")

# COMMAND ----------

# ---------------------------------------------------------
# STEP 8: PLATINUM — LLM ANALYSIS (Batch using ai_query)
# ---------------------------------------------------------
# Run AFTER streaming data has landed in Gold.
# Creates platinum_incidents via CREATE OR REPLACE TABLE AS SELECT.
# This is a batch operation — not part of the streaming DAG.

# %run ./05_llm_analysis

# COMMAND ----------

# ---------------------------------------------------------
# STEP 9: INSPECT PLATINUM RESULTS
# ---------------------------------------------------------

# display(
#     spark.sql("""
#         SELECT incident_id, alert_type, application_id,
#                alert_timestamp, prior_alert_count,
#                summary, patterns,
#                root_cause, confidence_score,
#                recommended_action
#         FROM akash_s_demo.ams.platinum_incidents
#         ORDER BY alert_timestamp
#     """)
# )

# COMMAND ----------

# ---------------------------------------------------------
# STEP 10: STOP STREAMS (when done)
# ---------------------------------------------------------
# for stream in spark.streams.active:
#     print(f"Stopping {stream.name}...")
#     stream.stop()
# print("All streams stopped.")
