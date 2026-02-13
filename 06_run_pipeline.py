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
#   In production, each step would be a separate Task in a
#   Databricks Workflow (DAG), with dependencies configured
#   in the Workflow UI or via Terraform/Pulumi.
# =========================================================

# COMMAND ----------

# ---------------------------------------------------------
# STEP 0: SETUP — Create catalog, schema, tables
# ---------------------------------------------------------
# %run ./00_setup

# COMMAND ----------

# ---------------------------------------------------------
# STEP 1: GENERATE SYNTHETIC DATA
# ---------------------------------------------------------
# %run ./01_data_generator

# Generate events and upload to the landing zone
# events, local_path = generate_all_events()
# dbutils.fs.cp(f"file:{local_path}",
#               f"{RAW_EVENTS_PATH}/events_batch1.jsonl")

# COMMAND ----------

# ---------------------------------------------------------
# STEP 2: BRONZE INGESTION (Auto Loader)
# ---------------------------------------------------------
# %run ./02_bronze_ingestion

# Wait for Bronze stream to finish (availableNow mode)
# query.awaitTermination()
# print(f"Bronze: {spark.sql('SELECT COUNT(*) FROM {BRONZE_TABLE}').collect()[0][0]} rows")

# COMMAND ----------

# ---------------------------------------------------------
# STEP 3: SILVER DEDUPLICATION
# ---------------------------------------------------------
# %run ./03_silver_dedup

# Wait for Silver stream to finish
# silver_query.awaitTermination()
# print(f"Silver: {spark.sql('SELECT COUNT(*) FROM {SILVER_TABLE}').collect()[0][0]} rows")

# COMMAND ----------

# ---------------------------------------------------------
# STEP 4: GOLD CORRELATION + CONTEXT BUILD
# ---------------------------------------------------------
# %run ./04_gold_correlation

# Wait for Gold stream to finish
# gold_query.awaitTermination()
# print(f"Gold: {spark.sql('SELECT COUNT(*) FROM {GOLD_TABLE}').collect()[0][0]} rows")

# COMMAND ----------

# ---------------------------------------------------------
# STEP 5: LLM SUMMARIZATION (Batch SQL)
# ---------------------------------------------------------
# Run the SQL file — this can also be a separate SQL task
# in a Databricks Workflow.

# spark.sql(open("05_llm_summarization.sql").read())
# OR use:
# %run ./05_llm_summarization

# COMMAND ----------

# ---------------------------------------------------------
# STEP 6: VERIFY END-TO-END
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

# Show Gold incidents with summaries
print("\n--- Gold Incidents ---")
# display(
#     spark.sql("""
#         SELECT incident_id, alert_type, application_id,
#                alert_timestamp, prior_alert_count,
#                LEFT(incident_context_text, 300) AS context_preview,
#                LEFT(summary, 200) AS summary_preview,
#                root_cause, confidence_score
#         FROM akash_s_demo.ams.gold_incidents
#         ORDER BY alert_timestamp
#     """)
# )
