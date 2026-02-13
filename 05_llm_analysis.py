# Databricks notebook source
# =========================================================
# 05_llm_analysis.py — Platinum Layer (LLM-Enriched Incidents)
# =========================================================
#
# PURPOSE:
#   Reads un-analyzed incidents from Gold, calls ai_query()
#   with structured JSON output to produce: summary, patterns,
#   root cause, confidence score, and recommended action.
#   Writes the result as a NEW Platinum table using
#   CREATE OR REPLACE TABLE ... AS SELECT.
#
# WHY A SEPARATE PLATINUM TABLE (not UPDATE on Gold):
#   - Gold remains a clean, deterministic output of the
#     correlation engine — no LLM artifacts mixed in.
#   - Platinum is the LLM-enriched view: stochastic by nature,
#     can be regenerated at any time without touching Gold.
#   - CREATE OR REPLACE is idempotent: re-running always
#     produces a fresh, consistent Platinum table.
#   - Avoids mutable UPDATE patterns that are hard to audit
#     and debug in Delta Lake.
#
# FUNCTION USED:
#   ai_query(endpoint, prompt, responseFormat => ...)
#   Reference: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/ai_query
#
# DESIGN DECISIONS:
#
#   WHY ai_query() INSTEAD OF ai_summarize():
#     - ai_query() supports responseFormat with json_schema,
#       returning typed JSON fields directly.
#     - Lets us choose the model endpoint explicitly.
#     - Supports modelParameters (temperature, max_tokens).
#     - We get structured fields (patterns, root_cause, etc.)
#       that ai_summarize() cannot produce cleanly.
#
#   WHY THIS RUNS AS BATCH, NOT IN STREAMING:
#     1. COST CONTROL: Each ai_query() call costs tokens.
#        Batch processes only what exists in Gold right now.
#     2. LATENCY ISOLATION: LLM calls take 2–10 seconds.
#        Must not block the streaming correlation pipeline.
#     3. RETRY SAFETY: failOnError => false ensures one bad
#        row doesn't kill the entire batch.
#     4. DETERMINISTIC/STOCHASTIC SEPARATION: Bronze → Silver
#        → Gold is deterministic. Platinum (LLM) is stochastic.
#        Re-running Platinum never affects upstream tables.
#
# SCHEDULING:
#   Run as a Databricks Job Task, triggered:
#     - Every 5 minutes (near-real-time)
#     - Or after the Gold streaming query completes a batch
# =========================================================

# COMMAND ----------

# %run ./config

# Inline config for standalone reference
GOLD_TABLE     = "akash_s_demo.ams.gold_incidents"
PLATINUM_TABLE = "akash_s_demo.ams.platinum_incidents"
LLM_ENDPOINT   = "databricks-meta-llama-3-3-70b-instruct"

# COMMAND ----------

# ---------------------------------------------------------
# 1. CREATE PLATINUM TABLE VIA CTAS
# ---------------------------------------------------------
# CREATE OR REPLACE TABLE ... AS SELECT:
#   - Reads all Gold incidents
#   - Calls ai_query() to generate structured LLM analysis
#   - Parses the JSON response into typed columns
#   - Writes everything as a new Platinum Delta table
#
# This is fully idempotent: re-running replaces the entire
# table with fresh LLM analysis of the current Gold data.
#
# The JSON response schema enforced by responseFormat:
#   {
#     "summary": "...",
#     "patterns": ["pattern1", "pattern2"],
#     "root_cause": "...",
#     "confidence_score": 0.85,
#     "recommended_action": "..."
#   }
#
# NOTE: Replace the endpoint name if your workspace uses
# a different model. Check Model Serving UI for available
# endpoints.

spark.sql(f"""
CREATE OR REPLACE TABLE {PLATINUM_TABLE}
USING DELTA
COMMENT 'LLM-enriched incidents — summary, patterns, root cause, recommended action'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
AS
WITH llm_raw AS (
    SELECT
        -- Pass through all Gold columns
        incident_id,
        alert_id,
        fingerprint,
        application_id,
        host_id,
        alert_type,
        severity,
        alert_timestamp,
        correlated_deployments,
        error_summary,
        metric_summary,
        prior_alert_count,
        incident_context_text,

        -- Call ai_query() with structured JSON output
        ai_query(
            '{LLM_ENDPOINT}',
            CONCAT(
                'You are an expert SRE analyzing production incidents. ',
                'Analyze the following incident context and provide a structured assessment.\\n\\n',
                'Instructions:\\n',
                '- summary: A concise 3-5 line summary of what happened.\\n',
                '- patterns: List the observable patterns (e.g., "CPU spike after deployment", ',
                '  "memory leak without deployment", "cascading timeouts"). Be specific.\\n',
                '- root_cause: The single most likely root cause based on the evidence.\\n',
                '- confidence_score: Your confidence in the root cause (0.0 to 1.0).\\n',
                '- recommended_action: One concrete next step for the on-call engineer.\\n\\n',
                'Incident Context:\\n',
                incident_context_text
            ),
            modelParameters => named_struct(
                'temperature', CAST(0.1 AS DOUBLE),
                'max_tokens', 1024
            ),
            responseFormat => '{{
                "type": "json_schema",
                "json_schema": {{
                    "name": "incident_analysis",
                    "schema": {{
                        "type": "object",
                        "properties": {{
                            "summary": {{
                                "type": "string",
                                "description": "3-5 line incident summary"
                            }},
                            "patterns": {{
                                "type": "array",
                                "items": {{"type": "string"}},
                                "description": "List of detected patterns"
                            }},
                            "root_cause": {{
                                "type": "string",
                                "description": "Most likely root cause"
                            }},
                            "confidence_score": {{
                                "type": "number",
                                "description": "Confidence in root cause 0.0 to 1.0"
                            }},
                            "recommended_action": {{
                                "type": "string",
                                "description": "Next step for on-call engineer"
                            }}
                        }},
                        "required": ["summary", "patterns", "root_cause", "confidence_score", "recommended_action"]
                    }},
                    "strict": true
                }}
            }}',
            failOnError => false
        ) AS llm_response,

        _created_at

    FROM {GOLD_TABLE}
)

-- ---------------------------------------------------------
-- 2. PARSE STRUCTURED JSON INTO TYPED COLUMNS
-- ---------------------------------------------------------
-- get_json_object() extracts fields from the ai_query() JSON.
-- If the LLM call failed (failOnError => false), llm_response
-- is NULL and all parsed fields will be NULL — no crash.

SELECT
    incident_id,
    alert_id,
    fingerprint,
    application_id,
    host_id,
    alert_type,
    severity,
    alert_timestamp,
    correlated_deployments,
    error_summary,
    metric_summary,
    prior_alert_count,
    incident_context_text,

    -- Parsed LLM fields
    get_json_object(llm_response, '$.summary')            AS summary,
    get_json_object(llm_response, '$.patterns')            AS patterns,
    get_json_object(llm_response, '$.root_cause')          AS root_cause,
    CAST(
        get_json_object(llm_response, '$.confidence_score')
        AS DOUBLE
    )                                                       AS confidence_score,
    get_json_object(llm_response, '$.recommended_action')  AS recommended_action,

    -- Raw LLM response preserved for debugging
    llm_response                                            AS llm_raw_response,

    _created_at,
    current_timestamp()                                     AS _analyzed_at

FROM llm_raw
""")

print(f"✓ Platinum table created: {PLATINUM_TABLE}")

# COMMAND ----------

# ---------------------------------------------------------
# 3. VERIFY RESULTS
# ---------------------------------------------------------

result = spark.sql(f"""
    SELECT
        incident_id,
        alert_type,
        application_id,
        alert_timestamp,
        summary,
        patterns,
        root_cause,
        confidence_score,
        recommended_action,
        prior_alert_count
    FROM {PLATINUM_TABLE}
    ORDER BY alert_timestamp DESC
""")

print(f"Platinum incidents: {result.count()} rows")
# display(result)

# COMMAND ----------

# ---------------------------------------------------------
# 4. QUALITY CHECK — Flag failed LLM calls
# ---------------------------------------------------------
# Rows where llm_response is NULL indicate ai_query() failures
# (rate limits, timeouts, etc.).  These can be retried by
# simply re-running this notebook.

failed = spark.sql(f"""
    SELECT COUNT(*) AS failed_count
    FROM {PLATINUM_TABLE}
    WHERE summary IS NULL
""").collect()[0]["failed_count"]

total = spark.sql(f"""
    SELECT COUNT(*) AS total
    FROM {PLATINUM_TABLE}
""").collect()[0]["total"]

print(f"\nLLM Analysis Quality:")
print(f"  Total incidents:  {total}")
print(f"  Analyzed:         {total - failed}")
print(f"  Failed (retry):   {failed}")
if failed > 0:
    print(f"  → Re-run this notebook to retry failed rows")
